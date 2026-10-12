//! Supervised `JetStream` intake and durable channel-recap execution.

use std::sync::Arc;
use std::time::Duration;

use async_nats::connection::State;
use async_nats::jetstream;
use futures_util::StreamExt as _;
use ratatoskr_channel_digest_contracts::{
    ChannelDigestRecapFailureCode, KnowledgeChannelDigestRecapFailed,
    KnowledgeChannelDigestRecapRequested, OutputLanguage,
};
use ratatoskr_event_envelope::CommandEnvelope;
use ratatoskr_identifiers::WireTimestamp;
use ratatoskr_knowledge::{
    BlobStore, ChannelRecapConfig, ChannelRecapContextError, ChannelRecapContextPolicy,
    ChannelRecapInbox, ChannelRecapOutputLanguage, ChannelRecapPipeline, ChannelRecapProviderMode,
    ChannelRecapRunState, ChannelRecapRunStore, Config, Database, DigestManifestAttemptOutcome,
    DigestSourceClient, DigestSourceClientSettings, LlmProvider, PreparedChannelRecapContext,
    ProviderResponse, ProviderUsage, RateLimiter, ScriptedProvider, VerifiedDigestManifest,
    attempt_digest_manifest, build_channel_recap_provider_request, prepare_channel_recap_context,
};
use tokio::sync::watch;

use crate::bus::{self, DurableSpec};
use crate::channel_recap_delivery::run_with_progress;
use crate::provider_runtime::{ControlledOpenRouter, openrouter_provider};
use crate::redelivery::RedeliverySchedule;
use crate::{Lifecycle, Metrics};

const PRODUCER: &str = "ratatoskr-channel-digests";

/// Safe worker supervision failures without endpoint, credential, or content values.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChannelRecapWorkerError {
    /// Bus connection, topology, stream, acknowledgement, or publication failed.
    #[error("the channel recap bus dependency is unavailable")]
    Bus,
    /// The pre-provisioned durable does not match the fixed contract.
    #[error("the channel recap durable is incompatible")]
    Durable,
    /// Digest source policy or authenticated readiness failed.
    #[error("the channel recap source dependency is unavailable")]
    Source,
    /// Durable work could not be admitted, resumed, or settled.
    #[error("the channel recap durable work failed")]
    Work,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    Ack,
    Term,
    Nak,
}

/// Spawns one owned recap supervisor. The caller must signal and join the returned handle.
///
/// `limiter` is the process-wide inference limiter, shared with the ingest worker.
#[must_use]
pub fn spawn_channel_recap_worker(
    config: Config,
    database: Database,
    blobs: BlobStore,
    lifecycle: Lifecycle,
    drain: watch::Receiver<bool>,
    limiter: Arc<RateLimiter>,
    metrics: Arc<Metrics>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        Box::pin(supervise(
            config, database, blobs, lifecycle, drain, limiter, metrics,
        ))
        .await;
    })
}

/// What one recap supervisor shares with every reconnect of its consumer.
struct RecapWorker<'a> {
    config: &'a Config,
    database: &'a Database,
    blobs: &'a BlobStore,
    provider: &'a RuntimeProvider,
    lifecycle: &'a Lifecycle,
    metrics: &'a Metrics,
}

/// The first and the longest wait between two probes of an unreachable digest source.
const SOURCE_RETRY_MIN: Duration = Duration::from_secs(1);
const SOURCE_RETRY_MAX: Duration = Duration::from_secs(30);

/// Whether the digest source answered the last time it was asked.
///
/// It lives in the supervisor, not in one connection, so a bus reconnect while the source stays
/// down does not log the same transition again (XR-021 CONTRACTS.md R3-09).
#[derive(Debug)]
struct SourceWatch {
    reachable: bool,
}

impl SourceWatch {
    /// A source is assumed reachable until a probe says otherwise, so a healthy start logs nothing.
    const fn new() -> Self {
        Self { reachable: true }
    }

    /// Records one probe: the gauge follows every probe, the log only a change of state.
    fn observe(&mut self, reachable: bool, metrics: &Metrics) {
        metrics.set_channel_recap_source_up(reachable);
        if reachable == self.reachable {
            return;
        }
        self.reachable = reachable;
        if reachable {
            tracing::info!(
                class = "source_reachable",
                "channel recap digest source answers again"
            );
        } else {
            tracing::warn!(
                class = "source_unavailable",
                "channel recap digest source is unreachable; commands wait on the durable"
            );
        }
    }
}

/// How one pass over the message stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Served {
    /// Drain began.
    Drained,
    /// The digest source stopped answering between two messages.
    SourceDown,
}

async fn supervise(
    config: Config,
    database: Database,
    blobs: BlobStore,
    lifecycle: Lifecycle,
    mut drain: watch::Receiver<bool>,
    limiter: Arc<RateLimiter>,
    metrics: Arc<Metrics>,
) {
    let provider = match runtime_provider(&config, &database, limiter) {
        Ok(provider) => provider,
        Err(error) => {
            tracing::error!(class = "provider_configuration", %error);
            return;
        }
    };
    let worker = RecapWorker {
        config: &config,
        database: &database,
        blobs: &blobs,
        provider: &provider,
        lifecycle: &lifecycle,
        metrics: &metrics,
    };
    let mut source_watch = SourceWatch::new();
    while !*drain.borrow() {
        let result = consume_once(&worker, &mut drain, &mut source_watch).await;
        lifecycle.set_channel_recap_ready(false);
        if *drain.borrow() {
            break;
        }
        tracing::warn!(
            class = match result {
                Ok(()) => "consumer_stopped",
                Err(ChannelRecapWorkerError::Bus) => "bus_unavailable",
                Err(ChannelRecapWorkerError::Durable) => "durable_mismatch",
                Err(ChannelRecapWorkerError::Source) => "source_unavailable",
                Err(ChannelRecapWorkerError::Work) => "work_failed",
            },
            "channel recap worker is not ready"
        );
        // cancel-safe: watch::Receiver::changed and sleep retain no partial work.
        tokio::select! {
            biased;
            _ = drain.changed() => {}
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

/// Connects, verifies the durable, reports ready, then serves the durable while the digest source
/// answers and waits for it while it does not.
///
/// Readiness follows the bus connection and the durable only: the digest source is Knowledge's
/// own dependency for recaps, not a condition for answering library traffic (R3-09).
async fn consume_once(
    worker: &RecapWorker<'_>,
    drain: &mut watch::Receiver<bool>,
    source_watch: &mut SourceWatch,
) -> Result<(), ChannelRecapWorkerError> {
    let recap = &worker.config.channel_recap;
    let client = connect_bus(recap).await?;
    let context = jetstream::new(client.clone());
    let consumer: jetstream::consumer::PullConsumer = context
        .get_consumer_from_stream(&recap.bus_durable, &recap.bus_stream)
        .await
        .map_err(|_| ChannelRecapWorkerError::Bus)?;
    verify_consumer(&consumer, recap)?;
    let source = source_client(
        recap,
        Duration::from_millis(worker.config.limits.provider_timeout_ms),
    )?;
    worker.lifecycle.set_channel_recap_ready(true);
    loop {
        if !wait_for_source(&source, &client, drain, source_watch, worker.metrics).await? {
            return Ok(());
        }
        if serve(worker, &consumer, &context, &source, drain).await? == Served::Drained {
            return Ok(());
        }
    }
}

/// Probes the digest source until it answers, backing off from 1 s to 30 s between probes.
///
/// Returns `Ok(false)` when drain began first and an error when the bus connection is lost while
/// waiting. No pull stream exists meanwhile, so no command is prefetched from the durable.
async fn wait_for_source(
    source: &DigestSourceClient,
    client: &async_nats::Client,
    drain: &mut watch::Receiver<bool>,
    source_watch: &mut SourceWatch,
    metrics: &Metrics,
) -> Result<bool, ChannelRecapWorkerError> {
    let mut delay = SOURCE_RETRY_MIN;
    loop {
        // cancel-safe: watch::changed and the probe request commit nothing.
        tokio::select! {
            biased;
            _ = drain.changed() => return Ok(false),
            probed = source.probe() => {
                source_watch.observe(probed.is_ok(), metrics);
                if probed.is_ok() {
                    return Ok(true);
                }
            }
        }
        // cancel-safe: watch::changed and sleep retain no partial work.
        tokio::select! {
            biased;
            _ = drain.changed() => return Ok(false),
            () = tokio::time::sleep(delay) => {}
        }
        if client.connection_state() != State::Connected {
            return Err(ChannelRecapWorkerError::Bus);
        }
        delay = delay.saturating_mul(2).min(SOURCE_RETRY_MAX);
    }
}

/// Pulls and handles commands until drain, a lost bus connection or a silent digest source.
async fn serve(
    worker: &RecapWorker<'_>,
    consumer: &jetstream::consumer::PullConsumer,
    context: &jetstream::Context,
    source: &DigestSourceClient,
    drain: &mut watch::Receiver<bool>,
) -> Result<Served, ChannelRecapWorkerError> {
    let config = worker.config;
    let recap = &config.channel_recap;
    let mut messages = consumer
        .stream()
        .max_messages_per_batch(usize::try_from(recap.fetch_batch).unwrap_or(1))
        .messages()
        .await
        .map_err(|_| ChannelRecapWorkerError::Bus)?;
    let mut probe = tokio::time::interval(Duration::from_secs(5));
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    probe.tick().await;
    loop {
        // cancel-safe: watch::changed, Interval::tick, and StreamExt::next do not commit work.
        tokio::select! {
            biased;
            _ = drain.changed() => return Ok(Served::Drained),
            _ = probe.tick() => {
                if source.probe().await.is_err() {
                    return Ok(Served::SourceDown);
                }
            }
            next = messages.next() => {
                let Some(next) = next else {
                    return Err(ChannelRecapWorkerError::Bus);
                };
                let message = next.map_err(|_| ChannelRecapWorkerError::Bus)?;
                // The handler runs to completion beside a progress ticker (never dropped), so a
                // message that outlasts the ack wait is not redelivered under its own handler.
                let disposition = run_with_progress(
                    &message,
                    Duration::from_secs(recap.ack_wait_seconds),
                    async {
                        let disposition = process_message(
                            worker.database,
                            worker.blobs,
                            source,
                            worker.provider,
                            config.limits.context_characters,
                            Duration::from_millis(config.limits.provider_timeout_ms),
                            config.limits.provider_max_output_tokens,
                            &recap.bus_subject,
                            message.payload.as_ref(),
                        ).await;
                        publish_outbox(worker.database, context).await?;
                        Ok::<_, ChannelRecapWorkerError>(disposition)
                    },
                ).await?;
                let ack = match disposition {
                    Disposition::Ack => jetstream::AckKind::Ack,
                    Disposition::Term => jetstream::AckKind::Term,
                    Disposition::Nak => {
                        let delivered = message.info().map_or(1, |info| info.delivered);
                        worker.metrics.record_channel_recap_redelivery();
                        jetstream::AckKind::Nak(Some(RedeliverySchedule::STANDARD.delay(delivered)))
                    }
                };
                message.ack_with(ack).await.map_err(|_| ChannelRecapWorkerError::Bus)?;
            }
        }
    }
}

async fn connect_bus(
    config: &ChannelRecapConfig,
) -> Result<async_nats::Client, ChannelRecapWorkerError> {
    bus::connect(&config.bus_endpoint, config.bus_credentials_file.as_deref())
        .await
        .map_err(|_| ChannelRecapWorkerError::Bus)
}

fn verify_consumer(
    consumer: &jetstream::consumer::PullConsumer,
    config: &ChannelRecapConfig,
) -> Result<(), ChannelRecapWorkerError> {
    bus::verify_consumer(
        consumer,
        &DurableSpec {
            name: &config.bus_durable,
            filter: &config.bus_subject,
            ack_wait: Duration::from_secs(config.ack_wait_seconds),
            max_deliver: None,
        },
    )
    .map_err(|_| ChannelRecapWorkerError::Durable)
}

fn source_client(
    config: &ChannelRecapConfig,
    provider_timeout: Duration,
) -> Result<DigestSourceClient, ChannelRecapWorkerError> {
    let secret = config
        .digest_source_service_secret
        .clone()
        .ok_or(ChannelRecapWorkerError::Source)?;
    DigestSourceClient::new(DigestSourceClientSettings {
        base_url: config.digest_source_base_url.clone(),
        service_secret: secret,
        connect_timeout: provider_timeout.min(Duration::from_secs(5)),
        request_deadline: provider_timeout,
        response_byte_cap: 1_048_576,
        retry_delay: Duration::from_secs(2),
    })
    .map_err(|_| ChannelRecapWorkerError::Source)
}

#[allow(
    clippy::too_many_arguments,
    reason = "bounded message execution inputs"
)]
async fn process_message(
    database: &Database,
    blobs: &BlobStore,
    source: &DigestSourceClient,
    provider: &RuntimeProvider,
    context_characters: usize,
    provider_timeout: Duration,
    max_output_tokens: u32,
    expected_subject: &str,
    bytes: &[u8],
) -> Disposition {
    let Ok(envelope) = CommandEnvelope::from_json(bytes) else {
        return Disposition::Term;
    };
    let expected_type = expected_subject.strip_prefix("cmd.").unwrap_or_default();
    if envelope.command_type.to_wire() != expected_type || envelope.producer.as_str() != PRODUCER {
        return Disposition::Term;
    }
    let Ok(request) = envelope.payload_as::<KnowledgeChannelDigestRecapRequested>() else {
        return Disposition::Term;
    };
    if ChannelRecapInbox::new(database)
        .accept(&envelope)
        .await
        .is_err()
    {
        return Disposition::Nak;
    }
    let Ok((run_id, state)) = load_run(database, &request).await else {
        return Disposition::Nak;
    };
    if matches!(state.as_str(), "completed" | "failed") {
        return Disposition::Ack;
    }
    let runs = ChannelRecapRunStore::new(database);
    match attempt_digest_manifest(source, &runs, run_id, &request).await {
        Ok(DigestManifestAttemptOutcome::RetryScheduled) | Err(_) => return Disposition::Nak,
        Ok(DigestManifestAttemptOutcome::Failed) => return Disposition::Ack,
        Ok(DigestManifestAttemptOutcome::Accepted) => {}
    }
    let Ok(verified) = load_manifest(database, run_id).await else {
        return Disposition::Nak;
    };
    let state = load_run(database, &request).await.map(|value| value.1);
    if matches!(state.as_deref(), Ok("manifest_verified"))
        && runs
            .transition(
                run_id,
                ChannelRecapRunState::ManifestVerified,
                ChannelRecapRunState::ContextPrepared,
            )
            .await
            .is_err()
    {
        return Disposition::Nak;
    }
    let policy = ChannelRecapContextPolicy {
        max_sources: 100,
        max_channels: 20,
        max_characters: context_characters,
    };
    let context = match prepare_channel_recap_context(&verified, policy) {
        Ok(context) => context,
        Err(ChannelRecapContextError::ContextBudget) => {
            return settle_context_failure(&runs, run_id, &request).await;
        }
        Err(_) => return Disposition::Nak,
    };
    let language = output_language(request.output_language);
    let Ok(provider_request) =
        build_channel_recap_provider_request(&context, language, max_output_tokens)
    else {
        return Disposition::Nak;
    };
    let outcome = match provider {
        RuntimeProvider::Scripted => {
            let provider = scripted_provider(&context);
            execute_pipeline(
                database,
                blobs,
                &provider,
                provider_timeout,
                run_id,
                provider_request,
                &context,
                &verified.digest_hex,
                language,
            )
            .await
        }
        RuntimeProvider::OpenRouter(provider) => {
            execute_pipeline(
                database,
                blobs,
                provider.as_ref(),
                provider_timeout,
                run_id,
                provider_request,
                &context,
                &verified.digest_hex,
                language,
            )
            .await
        }
    };
    match outcome {
        Ok(_) => Disposition::Ack,
        Err(_) => Disposition::Nak,
    }
}

#[derive(Debug)]
enum RuntimeProvider {
    Scripted,
    OpenRouter(Box<ControlledOpenRouter>),
}

fn runtime_provider(
    config: &Config,
    database: &Database,
    limiter: Arc<RateLimiter>,
) -> Result<RuntimeProvider, ChannelRecapWorkerError> {
    if config.channel_recap.provider_mode == ChannelRecapProviderMode::Scripted {
        tracing::warn!(
            provider_mode = "scripted",
            "channel recap answers every request with fixed synthetic text"
        );
        return Ok(RuntimeProvider::Scripted);
    }
    openrouter_provider(config, database, limiter)
        .map(RuntimeProvider::OpenRouter)
        .map_err(|_| ChannelRecapWorkerError::Work)
}

#[allow(clippy::too_many_arguments, reason = "typed pipeline boundary")]
async fn execute_pipeline<P: LlmProvider>(
    database: &Database,
    blobs: &BlobStore,
    provider: &P,
    timeout: Duration,
    run_id: uuid::Uuid,
    request: ratatoskr_knowledge::ChannelRecapProviderRequest,
    context: &PreparedChannelRecapContext,
    manifest_digest: &str,
    language: ChannelRecapOutputLanguage,
) -> Result<ratatoskr_knowledge::ChannelDigestRecap, ratatoskr_knowledge::ChannelRecapPipelineError>
{
    ChannelRecapPipeline::new(database, provider, blobs, timeout)
        .execute(run_id, request, context, manifest_digest, language)
        .await
}

async fn load_run(
    database: &Database,
    request: &KnowledgeChannelDigestRecapRequested,
) -> Result<(uuid::Uuid, String), ChannelRecapWorkerError> {
    sqlx::query_as(
        "select recap_run_id, state from knowledge.channel_recap_runs
         where owner_ref = $1 and digest_run_id = $2 and manifest_digest_hex = $3
           and output_language = $4",
    )
    .bind(request.owner.to_string())
    .bind(request.digest_run_id.as_uuid())
    .bind(request.manifest_digest.hex.as_str())
    .bind(match request.output_language {
        OutputLanguage::Ru => "ru",
        OutputLanguage::En => "en",
    })
    .fetch_one(database.pool())
    .await
    .map_err(|_| ChannelRecapWorkerError::Work)
}

async fn load_manifest(
    database: &Database,
    recap_run_id: uuid::Uuid,
) -> Result<VerifiedDigestManifest, ChannelRecapWorkerError> {
    let (digest_hex, manifest): (String, serde_json::Value) = sqlx::query_as(
        "select manifest_digest_hex, manifest from knowledge.channel_recap_manifests
         where recap_run_id = $1",
    )
    .bind(recap_run_id)
    .fetch_one(database.pool())
    .await
    .map_err(|_| ChannelRecapWorkerError::Work)?;
    Ok(VerifiedDigestManifest {
        digest_hex,
        manifest: serde_json::from_value(manifest).map_err(|_| ChannelRecapWorkerError::Work)?,
    })
}

async fn settle_context_failure(
    runs: &ChannelRecapRunStore<'_>,
    recap_run_id: uuid::Uuid,
    request: &KnowledgeChannelDigestRecapRequested,
) -> Disposition {
    let fact: Result<KnowledgeChannelDigestRecapFailed, _> =
        serde_json::from_value(serde_json::json!({
            "owner": request.owner,
            "operation_id": request.operation_id,
            "digest_run_id": request.digest_run_id,
            "manifest_digest": request.manifest_digest,
            "failure_code": ChannelDigestRecapFailureCode::ContextBudget,
            "failed_at": WireTimestamp::now(),
        }));
    let Ok(fact) = fact else {
        return Disposition::Nak;
    };
    match runs
        .settle_failed(recap_run_id, ChannelRecapRunState::ContextPrepared, &fact)
        .await
    {
        Ok(()) => Disposition::Ack,
        Err(_) => Disposition::Nak,
    }
}

fn output_language(language: OutputLanguage) -> ChannelRecapOutputLanguage {
    match language {
        OutputLanguage::Ru => ChannelRecapOutputLanguage::Ru,
        OutputLanguage::En => ChannelRecapOutputLanguage::En,
    }
}

fn scripted_provider(context: &PreparedChannelRecapContext) -> ScriptedProvider {
    let citation = context
        .sources
        .first()
        .map(|source| source.revision_ref.clone())
        .unwrap_or_default();
    // What a model returns: the text and warnings only. Language, digest and coverage are
    // composed by the pipeline from the run.
    let result = serde_json::json!({
        "headline": "Synthetic channel recap",
        "overview": "The selected synthetic channel evidence contains a bounded update.",
        "topics": [{
            "label": "Channel update",
            "summary": "A grounded update is available in the cited source revision.",
            "citations": [citation]
        }],
        "notable_items": [],
        "warnings": []
    });
    let response = serde_json::to_vec(&result).map(|bytes| ProviderResponse {
        bytes,
        request_id: Some("scripted-channel-recap".to_owned()),
        usage: ProviderUsage {
            input_tokens: u64::try_from(context.estimated_tokens).unwrap_or(u64::MAX),
            output_tokens: 64,
        },
    });
    ScriptedProvider::new([response.map_err(|_| ratatoskr_knowledge::ProviderError::Internal)])
}

async fn publish_outbox(
    database: &Database,
    context: &jetstream::Context,
) -> Result<(), ChannelRecapWorkerError> {
    let rows: Vec<(
        uuid::Uuid,
        String,
        serde_json::Value,
        uuid::Uuid,
        String,
        uuid::Uuid,
        uuid::Uuid,
    )> = sqlx::query_as(
        "select outbox.outbox_id, outbox.subject, outbox.payload, runs.digest_run_id,
                runs.owner_ref, inbox.operation_id, inbox.command_id
         from knowledge.channel_recap_outbox outbox
         join knowledge.channel_recap_runs runs using (recap_run_id)
         join knowledge.channel_recap_inbox inbox on inbox.command_id = runs.inbox_command_id
         where outbox.published_at is null order by outbox.created_at limit 32",
    )
    .fetch_all(database.pool())
    .await
    .map_err(|_| ChannelRecapWorkerError::Work)?;
    for (outbox_id, subject, payload, digest_run_id, owner, operation_id, command_id) in rows {
        let envelope = serde_json::json!({
            "event_id": outbox_id,
            "event_type": subject,
            "occurred_at": WireTimestamp::now(),
            "producer": "ratatoskr-knowledge",
            "aggregate_id": format!("channel-digest-run:{digest_run_id}"),
            "correlation_id": format!("operation:{operation_id}"),
            "causation_id": format!("command:{command_id}"),
            "tenant_id": owner,
            "schema_version": 1,
            "payload": payload
        });
        let bytes = serde_json::to_vec(&envelope).map_err(|_| ChannelRecapWorkerError::Work)?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", outbox_id.to_string());
        let transport_subject = format!("evt.{subject}");
        context
            .publish_with_headers(transport_subject, headers, bytes.into())
            .await
            .map_err(|_| ChannelRecapWorkerError::Bus)?
            .await
            .map_err(|_| ChannelRecapWorkerError::Bus)?;
        sqlx::query(
            "update knowledge.channel_recap_outbox set published_at = now()
             where outbox_id = $1 and published_at is null",
        )
        .bind(outbox_id)
        .execute(database.pool())
        .await
        .map_err(|_| ChannelRecapWorkerError::Work)?;
    }
    Ok(())
}
