//! Supervised `JetStream` intake and durable channel-recap execution.

use std::time::Duration;

use async_nats::jetstream;
use futures_util::StreamExt as _;
use ratatoskr_channel_digest_contracts::{
    ChannelDigestRecapFailureCode, KnowledgeChannelDigestRecapFailed,
    KnowledgeChannelDigestRecapRequested, OutputLanguage,
};
use ratatoskr_event_envelope::CommandEnvelope;
use ratatoskr_identifiers::WireTimestamp;
use ratatoskr_knowledge::{
    BlobStore, BudgetLedger, BudgetLimits, ChannelRecapConfig, ChannelRecapContextError,
    ChannelRecapContextPolicy, ChannelRecapInbox, ChannelRecapOutputLanguage, ChannelRecapPipeline,
    ChannelRecapProviderMode, ChannelRecapRunState, ChannelRecapRunStore, Config,
    ControlledProvider, Database, DigestManifestAttemptOutcome, DigestSourceClient,
    DigestSourceClientSettings, LlmProvider, OpenRouterProvider, OpenRouterSettings,
    PreparedChannelRecapContext, ProviderResponse, ProviderUsage, RateLimiter, RetryPolicy,
    ScriptedProvider, SpendControls, TokenPrices, VerifiedDigestManifest, attempt_digest_manifest,
    build_channel_recap_provider_request, prepare_channel_recap_context,
};
use tokio::sync::watch;

use crate::Lifecycle;

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
#[must_use]
pub fn spawn_channel_recap_worker(
    config: Config,
    database: Database,
    blobs: BlobStore,
    lifecycle: Lifecycle,
    drain: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        supervise(config, database, blobs, lifecycle, drain).await;
    })
}

async fn supervise(
    config: Config,
    database: Database,
    blobs: BlobStore,
    lifecycle: Lifecycle,
    mut drain: watch::Receiver<bool>,
) {
    let provider = match runtime_provider(&config, &database) {
        Ok(provider) => provider,
        Err(error) => {
            tracing::error!(class = "provider_configuration", %error);
            return;
        }
    };
    while !*drain.borrow() {
        let result = consume_once(
            &config, &database, &blobs, &provider, &lifecycle, &mut drain,
        )
        .await;
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

async fn consume_once(
    config: &Config,
    database: &Database,
    blobs: &BlobStore,
    provider: &RuntimeProvider,
    lifecycle: &Lifecycle,
    drain: &mut watch::Receiver<bool>,
) -> Result<(), ChannelRecapWorkerError> {
    let recap = &config.channel_recap;
    let client = connect_bus(recap).await?;
    let context = jetstream::new(client.clone());
    let consumer: jetstream::consumer::PullConsumer = context
        .get_consumer_from_stream(&recap.bus_durable, &recap.bus_stream)
        .await
        .map_err(|_| ChannelRecapWorkerError::Bus)?;
    verify_consumer(&consumer, recap)?;
    let source = source_client(
        recap,
        Duration::from_millis(config.limits.provider_timeout_ms),
    )?;
    source
        .probe()
        .await
        .map_err(|_| ChannelRecapWorkerError::Source)?;
    let mut messages = consumer
        .stream()
        .max_messages_per_batch(usize::try_from(recap.fetch_batch).unwrap_or(32))
        .messages()
        .await
        .map_err(|_| ChannelRecapWorkerError::Bus)?;
    lifecycle.set_channel_recap_ready(true);
    let mut probe = tokio::time::interval(Duration::from_secs(5));
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    probe.tick().await;
    loop {
        // cancel-safe: watch::changed, Interval::tick, and StreamExt::next do not commit work.
        tokio::select! {
            biased;
            _ = drain.changed() => return Ok(()),
            _ = probe.tick() => {
                source.probe().await.map_err(|_| ChannelRecapWorkerError::Source)?;
            }
            next = messages.next() => {
                let Some(next) = next else {
                    return Err(ChannelRecapWorkerError::Bus);
                };
                let message = next.map_err(|_| ChannelRecapWorkerError::Bus)?;
                let disposition = process_message(
                    database,
                    blobs,
                    &source,
                    provider,
                    config.limits.context_characters,
                    Duration::from_millis(config.limits.provider_timeout_ms),
                    config.limits.provider_max_output_tokens,
                    &recap.bus_subject,
                    message.payload.as_ref(),
                ).await;
                publish_outbox(database, &context).await?;
                let ack = match disposition {
                    Disposition::Ack => jetstream::AckKind::Ack,
                    Disposition::Term => jetstream::AckKind::Term,
                    Disposition::Nak => jetstream::AckKind::Nak(Some(Duration::from_secs(2))),
                };
                message.ack_with(ack).await.map_err(|_| ChannelRecapWorkerError::Bus)?;
            }
        }
    }
}

async fn connect_bus(
    config: &ChannelRecapConfig,
) -> Result<async_nats::Client, ChannelRecapWorkerError> {
    if let Some(path) = &config.bus_credentials_file {
        let seed = tokio::fs::read_to_string(path)
            .await
            .map_err(|_| ChannelRecapWorkerError::Bus)?;
        async_nats::ConnectOptions::with_nkey(seed.trim().to_owned())
            .connect(&config.bus_endpoint)
            .await
            .map_err(|_| ChannelRecapWorkerError::Bus)
    } else {
        async_nats::connect(&config.bus_endpoint)
            .await
            .map_err(|_| ChannelRecapWorkerError::Bus)
    }
}

fn verify_consumer(
    consumer: &jetstream::consumer::PullConsumer,
    config: &ChannelRecapConfig,
) -> Result<(), ChannelRecapWorkerError> {
    let actual = &consumer.cached_info().config;
    if actual.durable_name.as_deref() != Some(config.bus_durable.as_str())
        || actual.filter_subject != config.bus_subject
        || actual.ack_policy != jetstream::consumer::AckPolicy::Explicit
        || actual.ack_wait != Duration::from_secs(config.ack_wait_seconds)
        || actual.deliver_subject.is_some()
        || actual.deliver_policy != jetstream::consumer::DeliverPolicy::All
    {
        return Err(ChannelRecapWorkerError::Durable);
    }
    Ok(())
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
    let Ok(provider_request) = build_channel_recap_provider_request(&context, max_output_tokens)
    else {
        return Disposition::Nak;
    };
    let outcome = match provider {
        RuntimeProvider::Scripted => {
            let provider = scripted_provider(&request, &context, &verified.digest_hex);
            execute_pipeline(
                database,
                blobs,
                &provider,
                provider_timeout,
                run_id,
                provider_request,
                &context,
                &verified.digest_hex,
                output_language(request.output_language),
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
                output_language(request.output_language),
            )
            .await
        }
    };
    match outcome {
        Ok(_) => Disposition::Ack,
        Err(_) => Disposition::Nak,
    }
}

type ControlledOpenRouter = ControlledProvider<OpenRouterProvider>;

#[derive(Debug)]
enum RuntimeProvider {
    Scripted,
    OpenRouter(Box<ControlledOpenRouter>),
}

fn runtime_provider(
    config: &Config,
    database: &Database,
) -> Result<RuntimeProvider, ChannelRecapWorkerError> {
    if config.channel_recap.provider_mode == ChannelRecapProviderMode::Scripted {
        return Ok(RuntimeProvider::Scripted);
    }
    let configured = config
        .provider
        .openrouter
        .as_ref()
        .ok_or(ChannelRecapWorkerError::Work)?;
    let inner = OpenRouterProvider::new(OpenRouterSettings {
        base_url: configured.base_url.clone(),
        model: configured.model.clone(),
        credential: configured.api_key.clone(),
        max_output_tokens: config.limits.provider_max_output_tokens,
        response_byte_cap: config.limits.raw_response_bytes,
        call_deadline: Duration::from_millis(config.limits.provider_timeout_ms),
        connect_timeout: Duration::from_millis(config.limits.provider_timeout_ms)
            .min(Duration::from_secs(5)),
        retry: RetryPolicy::new(3, 200, 2_000),
    })
    .map_err(|_| ChannelRecapWorkerError::Work)?;
    let spacing =
        Duration::try_from_secs_f64(60.0 / f64::from(config.limits.provider_requests_per_minute))
            .map_err(|_| ChannelRecapWorkerError::Work)?;
    Ok(RuntimeProvider::OpenRouter(Box::new(
        ControlledProvider::new(
            inner,
            std::sync::Arc::new(RateLimiter::new(spacing)),
            BudgetLedger::new(database.pool().clone()),
            SpendControls {
                limits: BudgetLimits {
                    daily_tokens: config.limits.provider_daily_token_budget,
                    monthly_tokens: config.limits.provider_monthly_token_budget,
                    daily_cost_micro_usd: config.limits.provider_daily_cost_micro_usd,
                    monthly_cost_micro_usd: config.limits.provider_monthly_cost_micro_usd,
                },
                prices: TokenPrices {
                    input_micro_usd_per_mtoken: configured.input_micro_usd_per_mtoken,
                    output_micro_usd_per_mtoken: configured.output_micro_usd_per_mtoken,
                },
                max_output_tokens: config.limits.provider_max_output_tokens,
            },
        ),
    )))
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

fn scripted_provider(
    request: &KnowledgeChannelDigestRecapRequested,
    context: &PreparedChannelRecapContext,
    manifest_digest: &str,
) -> ScriptedProvider {
    let citation = context
        .sources
        .first()
        .map(|source| source.revision_ref.clone())
        .unwrap_or_default();
    let warnings = if context.omitted_count > 0 {
        vec!["context_omitted_sources"]
    } else {
        Vec::new()
    };
    let result = serde_json::json!({
        "contract_version": "channel_digest_recap.v1",
        "prompt_version": "channel_digest_recap_prompt.v1",
        "context_version": "channel_digest_recap_context.v1",
        "output_language": match request.output_language { OutputLanguage::Ru => "ru", OutputLanguage::En => "en" },
        "manifest_digest": {"algorithm": "sha256", "hex": manifest_digest},
        "headline": "Synthetic channel recap",
        "overview": "The selected synthetic channel evidence contains a bounded update.",
        "topics": [{
            "label": "Channel update",
            "summary": "A grounded update is available in the cited source revision.",
            "citations": [citation]
        }],
        "notable_items": [],
        "coverage": {
            "selected_count": context.selected_count,
            "included_count": context.included_count,
            "omitted_count": context.omitted_count,
            "channel_count": context.channel_count
        },
        "warnings": warnings
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
