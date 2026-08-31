use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ratatoskr_ai_archive_contracts::{
    AiArchiveAnalysisCompleted, AiArchiveSubject, AiConversationAdded, AiConversationUpdated,
    AiProjectAdded, AiProjectUpdated,
};
use ratatoskr_document_contracts::Document;
use ratatoskr_event_envelope::{EventEnvelope, EventPayload};
use ratatoskr_github_contracts::{
    AnalysisFailureCode, RepositoryAnalysisFailed, RepositoryAnalysisRequested,
};
use ratatoskr_identifiers::{Extensions, WireTimestamp};
use ratatoskr_knowledge::{
    AnalysisIdentity, AnalysisWork, AnalysisWorkState, ArchiveEventAdmission, ArchiveEventConsumer,
    ArticlePipeline, BlobStore, Config, ControlledProvider, Database, FamilyPipeline,
    FamilyPipelineError, GithubRepositoryReadmeResolver, OpenRouterProvider, PipelineError,
    RepositoryReadmeError, SourceInbox, SourceInboxAdmission, SourceReference, TerminalOutbox,
    TerminalOutboxError, WorkQueue, build_generation_request, prepare_context,
};
use ratatoskr_social_contracts::{
    SocialSourceAnalysisCompleted, SocialSourceCaptured, SocialSourceUpdated,
};
use tokio::sync::watch;
use uuid::Uuid;

use crate::{Lifecycle, Metrics};

type PrimaryProvider = ControlledProvider<OpenRouterProvider>;

#[must_use]
#[allow(
    clippy::too_many_arguments,
    reason = "one worker owns its complete bounded dependency set"
)]
pub(super) fn spawn_worker(
    ordinal: u32,
    config: Config,
    database: Database,
    blobs: BlobStore,
    provider: Arc<PrimaryProvider>,
    resolver: Arc<GithubRepositoryReadmeResolver>,
    lifecycle: Lifecycle,
    metrics: Arc<Metrics>,
    workers_failed: Arc<AtomicBool>,
    drain: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let worker = tokio::spawn(async move {
            run_worker(
                ordinal, &config, &database, &blobs, &provider, &resolver, &metrics, drain,
            )
            .await;
        });
        let _result = worker.await;
        workers_failed.store(true, Ordering::Release);
        lifecycle.set_primary_workers_ready(false);
    })
}

#[must_use]
pub(super) fn spawn_dependency_supervisor(
    config: Config,
    database: Database,
    resolver: Arc<GithubRepositoryReadmeResolver>,
    workers_failed: Arc<AtomicBool>,
    lifecycle: Lifecycle,
    mut drain: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let provider = config.provider.openrouter.clone();
        let probe_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .ok();
        while !*drain.borrow() && !workers_failed.load(Ordering::Acquire) {
            let github_ready = resolver.probe().await.is_ok();
            let provider_ready = match (provider.as_ref(), probe_client.as_ref()) {
                (Some(provider), Some(client)) => probe_provider(client, provider).await,
                _ => false,
            };
            let storage_ready = tokio::time::timeout(
                Duration::from_secs(2),
                sqlx::query_scalar::<_, i32>("select 1").fetch_one(database.pool()),
            )
            .await
            .is_ok_and(|result| matches!(result, Ok(1)));
            lifecycle.set_primary_workers_ready(github_ready && provider_ready && storage_ready);
            tokio::select! {
                biased;
                _ = drain.changed() => {}
                () = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
        lifecycle.set_primary_workers_ready(false);
    })
}

async fn probe_provider(
    client: &reqwest::Client,
    provider: &ratatoskr_knowledge::OpenRouterProviderConfig,
) -> bool {
    let Ok(endpoint) =
        reqwest::Url::parse(&format!("{}/key", provider.base_url.trim_end_matches('/')))
    else {
        return false;
    };
    client
        .get(endpoint)
        .bearer_auth(provider.api_key.expose_secret())
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

#[allow(
    clippy::too_many_arguments,
    reason = "one worker owns its complete bounded dependency set"
)]
async fn run_worker(
    ordinal: u32,
    config: &Config,
    database: &Database,
    blobs: &BlobStore,
    provider: &PrimaryProvider,
    resolver: &GithubRepositoryReadmeResolver,
    metrics: &Metrics,
    mut drain: watch::Receiver<bool>,
) {
    let worker = format!("knowledge-primary-{ordinal}");
    let queue = WorkQueue::new(database);
    while !*drain.borrow() {
        let claimed = queue
            .claim(&worker, Duration::from_secs(config.primary.lease_seconds))
            .await;
        let Ok(Some(work)) = claimed else {
            tokio::select! {
                biased;
                _ = drain.changed() => {}
                () = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
            continue;
        };
        if *drain.borrow() {
            let _result = queue.release(work.work_id, &worker).await;
            break;
        }
        match work.state {
            AnalysisWorkState::Admitted => {
                let _result = queue
                    .transition(
                        work.work_id,
                        &worker,
                        AnalysisWorkState::Admitted,
                        AnalysisWorkState::Preparing,
                    )
                    .await;
            }
            AnalysisWorkState::Preparing | AnalysisWorkState::RetryWait => {
                let _result = queue
                    .transition(
                        work.work_id,
                        &worker,
                        work.state,
                        AnalysisWorkState::ProviderPending,
                    )
                    .await;
            }
            AnalysisWorkState::ProviderPending
            | AnalysisWorkState::ResponseReceived
            | AnalysisWorkState::Persisting => {
                tokio::select! {
                    biased;
                    _ = drain.changed() => {
                        let _result = queue.release(work.work_id, &worker).await;
                        break;
                    }
                    () = settle_execution(
                        config, database, blobs, provider, resolver, metrics,
                        &queue, &worker, &work,
                    ) => {}
                }
            }
            AnalysisWorkState::ProviderOutcomeUnknown
            | AnalysisWorkState::Completed
            | AnalysisWorkState::Failed
            | AnalysisWorkState::Suppressed => {}
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one settlement binds work, dependencies, and metrics"
)]
async fn settle_execution(
    config: &Config,
    database: &Database,
    blobs: &BlobStore,
    provider: &PrimaryProvider,
    resolver: &GithubRepositoryReadmeResolver,
    metrics: &Metrics,
    queue: &WorkQueue<'_>,
    worker: &str,
    work: &AnalysisWork,
) {
    let outcome = execute(config, database, blobs, provider, resolver, work).await;
    let outbox = TerminalOutbox::new(database);
    match outcome {
        ExecutionOutcome::Fact {
            event_type,
            subject,
            envelope,
        } => {
            let result = outbox
                .settle(work.work_id, worker, true, event_type, subject, &envelope)
                .await;
            discard_if_suppressed(queue, work.work_id, &result).await;
        }
        ExecutionOutcome::FailedFact {
            event_type,
            subject,
            envelope,
        } => {
            let result = outbox
                .settle(work.work_id, worker, false, event_type, subject, &envelope)
                .await;
            discard_if_suppressed(queue, work.work_id, &result).await;
        }
        ExecutionOutcome::CompletedWithoutFact => {
            let result = outbox
                .finish_without_fact(work.work_id, worker, true, None)
                .await;
            discard_if_suppressed(queue, work.work_id, &result).await;
        }
        ExecutionOutcome::FinalFailure => {
            let result = outbox
                .finish_without_fact(work.work_id, worker, false, Some("analysis_failed"))
                .await;
            discard_if_suppressed(queue, work.work_id, &result).await;
        }
        ExecutionOutcome::Retry => {
            metrics.record_primary_retry();
            let shift = u32::try_from(work.attempt_count).unwrap_or(8).min(8);
            let delay = Duration::from_secs(1_u64 << shift);
            if queue
                .retry_after(work.work_id, worker, delay)
                .await
                .is_err()
            {
                let result = outbox
                    .finish_without_fact(work.work_id, worker, false, Some("retry_exhausted"))
                    .await;
                discard_if_suppressed(queue, work.work_id, &result).await;
            }
        }
        ExecutionOutcome::Unknown => {
            metrics.record_primary_uncertain();
            let _result = queue.mark_provider_unknown(work.work_id, worker).await;
        }
    }
}

async fn discard_if_suppressed<T>(
    queue: &WorkQueue<'_>,
    work_id: Uuid,
    result: &Result<T, TerminalOutboxError>,
) {
    if matches!(result, Err(TerminalOutboxError::Transition)) {
        let _result = queue.discard_suppressed_derivatives(work_id).await;
    }
}

enum ExecutionOutcome {
    Fact {
        event_type: &'static str,
        subject: &'static str,
        envelope: serde_json::Value,
    },
    FailedFact {
        event_type: &'static str,
        subject: &'static str,
        envelope: serde_json::Value,
    },
    CompletedWithoutFact,
    FinalFailure,
    Retry,
    Unknown,
}

async fn execute(
    config: &Config,
    database: &Database,
    blobs: &BlobStore,
    provider: &PrimaryProvider,
    resolver: &GithubRepositoryReadmeResolver,
    work: &AnalysisWork,
) -> ExecutionOutcome {
    let Ok(envelope) = serde_json::from_value::<EventEnvelope>(work.input_envelope.clone()) else {
        return ExecutionOutcome::FinalFailure;
    };
    let event_type = envelope.event_type.to_wire();
    if event_type == Document::EVENT_TYPE {
        return execute_document(config, database, blobs, provider, &envelope).await;
    }
    if matches!(
        event_type.as_str(),
        SocialSourceCaptured::EVENT_TYPE | SocialSourceUpdated::EVENT_TYPE
    ) {
        return execute_social(config, database, blobs, provider, &envelope).await;
    }
    if matches!(
        event_type.as_str(),
        AiConversationAdded::EVENT_TYPE
            | AiConversationUpdated::EVENT_TYPE
            | AiProjectAdded::EVENT_TYPE
            | AiProjectUpdated::EVENT_TYPE
    ) {
        return execute_archive(config, database, blobs, provider, &envelope).await;
    }
    if event_type == RepositoryAnalysisRequested::EVENT_TYPE {
        return execute_repository(config, database, blobs, provider, resolver, &envelope).await;
    }
    ExecutionOutcome::CompletedWithoutFact
}

async fn execute_document(
    config: &Config,
    database: &Database,
    blobs: &BlobStore,
    provider: &PrimaryProvider,
    envelope: &EventEnvelope,
) -> ExecutionOutcome {
    let Ok(document) = envelope.payload_as::<Document>() else {
        return ExecutionOutcome::FinalFailure;
    };
    let Some(tenant) = envelope.tenant_id else {
        return ExecutionOutcome::FinalFailure;
    };
    let Ok(bytes) = envelope.to_canonical_json() else {
        return ExecutionOutcome::FinalFailure;
    };
    let Ok(source_blob) = blobs.store_raw(bytes.as_bytes()).await else {
        return ExecutionOutcome::Retry;
    };
    let source = SourceReference {
        tenant,
        owner_context: "ratatoskr-knowledge".to_owned(),
        ai_archive_id: String::new(),
        document_id: document.document_id,
        content_digest: document.content_digest.clone(),
        source_blob,
    };
    let Ok(source) = database.register_source(&source).await else {
        return ExecutionOutcome::Retry;
    };
    let identity = AnalysisIdentity {
        source_revision_id: source.id,
        contract_version: "article_v1".to_owned(),
        prompt_version: "article_prompt_v1".to_owned(),
        context_builder_version: "document_context_v1".to_owned(),
        model_policy: "primary_default_v1".to_owned(),
    };
    let Ok(run) = database.create_run(&identity).await else {
        return ExecutionOutcome::Retry;
    };
    let Ok(context) = prepare_context(&document, config.limits.context_characters) else {
        return ExecutionOutcome::FinalFailure;
    };
    let Ok(request) = build_generation_request(&context) else {
        return ExecutionOutcome::FinalFailure;
    };
    match ArticlePipeline::new(
        database,
        provider,
        blobs,
        Duration::from_millis(config.limits.provider_timeout_ms),
    )
    .execute(run.id, request, &context, &document)
    .await
    {
        Ok(_) => ExecutionOutcome::CompletedWithoutFact,
        Err(PipelineError::ProviderOutcomeUnknown) => ExecutionOutcome::Unknown,
        Err(PipelineError::Persistence(_) | PipelineError::Blob(_)) => ExecutionOutcome::Retry,
        Err(_) => ExecutionOutcome::FinalFailure,
    }
}

async fn execute_social(
    config: &Config,
    database: &Database,
    blobs: &BlobStore,
    provider: &PrimaryProvider,
    envelope: &EventEnvelope,
) -> ExecutionOutcome {
    let snapshot = if envelope.event_type.to_wire() == SocialSourceCaptured::EVENT_TYPE {
        envelope
            .payload_as::<SocialSourceCaptured>()
            .map(|payload| payload.source)
    } else {
        envelope
            .payload_as::<SocialSourceUpdated>()
            .map(|payload| payload.source)
    };
    let Ok(snapshot) = snapshot else {
        return ExecutionOutcome::FinalFailure;
    };
    let inbox = SourceInbox::new(database);
    let admission = inbox
        .accept_social(
            envelope.event_id.0,
            &envelope.event_type.to_wire(),
            &snapshot,
        )
        .await;
    match admission {
        Ok(SourceInboxAdmission::AcceptedCurrent | SourceInboxAdmission::Duplicate) => {}
        Ok(SourceInboxAdmission::AcceptedHistorical | SourceInboxAdmission::Tombstoned) => {
            return ExecutionOutcome::CompletedWithoutFact;
        }
        Err(_) => return ExecutionOutcome::Retry,
    }
    let pipeline = FamilyPipeline::new(
        database,
        provider,
        blobs,
        Duration::from_millis(config.limits.provider_timeout_ms),
    );
    match pipeline.execute_social(&snapshot).await {
        Ok(_) => {
            let payload = SocialSourceAnalysisCompleted {
                owner: snapshot.owner,
                social_source_id: snapshot.social_source_id,
                content_digest: snapshot.content_digest,
                completed_at: WireTimestamp::now(),
                extensions: Extensions::new(),
            };
            fact(envelope, &payload, "evt.knowledge.analysis.completed.v1")
        }
        Err(FamilyPipelineError::ProviderOutcomeUnknown) => ExecutionOutcome::Unknown,
        Err(
            FamilyPipelineError::Persistence(_)
            | FamilyPipelineError::Blob(_)
            | FamilyPipelineError::RepositorySource(RepositoryReadmeError::Unavailable),
        ) => ExecutionOutcome::Retry,
        Err(_) => ExecutionOutcome::FinalFailure,
    }
}

async fn execute_archive(
    config: &Config,
    database: &Database,
    blobs: &BlobStore,
    provider: &PrimaryProvider,
    envelope: &EventEnvelope,
) -> ExecutionOutcome {
    let admission = ArchiveEventConsumer::new(database).accept(envelope).await;
    match admission {
        Ok(
            ArchiveEventAdmission::Conversation(
                SourceInboxAdmission::AcceptedCurrent | SourceInboxAdmission::Duplicate,
            )
            | ArchiveEventAdmission::Project(
                SourceInboxAdmission::AcceptedCurrent | SourceInboxAdmission::Duplicate,
            ),
        ) => {}
        Ok(
            ArchiveEventAdmission::Conversation(
                SourceInboxAdmission::AcceptedHistorical | SourceInboxAdmission::Tombstoned,
            )
            | ArchiveEventAdmission::Project(
                SourceInboxAdmission::AcceptedHistorical | SourceInboxAdmission::Tombstoned,
            )
            | ArchiveEventAdmission::ObjectRecorded
            | ArchiveEventAdmission::ObjectDuplicate
            | ArchiveEventAdmission::Tombstone
            | ArchiveEventAdmission::TombstoneDuplicate,
        ) => return ExecutionOutcome::CompletedWithoutFact,
        Err(_) => return ExecutionOutcome::Retry,
    }
    let pipeline = FamilyPipeline::new(
        database,
        provider,
        blobs,
        Duration::from_millis(config.limits.provider_timeout_ms),
    );
    let event_type = envelope.event_type.to_wire();
    let result = if matches!(
        event_type.as_str(),
        AiConversationAdded::EVENT_TYPE | AiConversationUpdated::EVENT_TYPE
    ) {
        pipeline
            .execute_archive_event(&SourceInbox::new(database), envelope.event_id.0)
            .await
            .map(|execution| execution.completion)
    } else if event_type == AiProjectAdded::EVENT_TYPE {
        let Ok(payload) = envelope.payload_as::<AiProjectAdded>() else {
            return ExecutionOutcome::FinalFailure;
        };
        pipeline
            .execute_archive_project_event(&SourceInbox::new(database), envelope.event_id.0)
            .await
            .map(|_| AiArchiveAnalysisCompleted {
                ai_archive_id: payload.import_provenance.ai_archive_id,
                owner: payload.import_provenance.owner,
                subject: AiArchiveSubject::Project {
                    ai_project_id: payload.project.ai_project_id,
                },
                content_digest: payload.content_digest,
                completed_at: WireTimestamp::now(),
                extensions: Extensions::new(),
            })
    } else {
        let Ok(payload) = envelope.payload_as::<AiProjectUpdated>() else {
            return ExecutionOutcome::FinalFailure;
        };
        pipeline
            .execute_archive_project_event(&SourceInbox::new(database), envelope.event_id.0)
            .await
            .map(|_| AiArchiveAnalysisCompleted {
                ai_archive_id: payload.import_provenance.ai_archive_id,
                owner: payload.import_provenance.owner,
                subject: AiArchiveSubject::Project {
                    ai_project_id: payload.project.ai_project_id,
                },
                content_digest: payload.content_digest,
                completed_at: WireTimestamp::now(),
                extensions: Extensions::new(),
            })
    };
    match result {
        Ok(payload) => fact(
            envelope,
            &payload,
            "evt.knowledge.ai_archive_analysis.completed.v1",
        ),
        Err(FamilyPipelineError::ProviderOutcomeUnknown) => ExecutionOutcome::Unknown,
        Err(FamilyPipelineError::Persistence(_) | FamilyPipelineError::Blob(_)) => {
            ExecutionOutcome::Retry
        }
        Err(_) => ExecutionOutcome::FinalFailure,
    }
}

async fn execute_repository(
    config: &Config,
    database: &Database,
    blobs: &BlobStore,
    provider: &PrimaryProvider,
    resolver: &GithubRepositoryReadmeResolver,
    envelope: &EventEnvelope,
) -> ExecutionOutcome {
    let Ok(request) = envelope.payload_as::<RepositoryAnalysisRequested>() else {
        return ExecutionOutcome::FinalFailure;
    };
    let pipeline = FamilyPipeline::new(
        database,
        provider,
        blobs,
        Duration::from_millis(config.limits.provider_timeout_ms),
    );
    match pipeline.execute_repository(&request, resolver).await {
        Ok(execution) => {
            execution
                .completion
                .map_or(ExecutionOutcome::CompletedWithoutFact, |payload| {
                    fact(
                        envelope,
                        &payload,
                        "evt.knowledge.repository_analysis.completed.v1",
                    )
                })
        }
        Err(FamilyPipelineError::ProviderOutcomeUnknown) => ExecutionOutcome::Unknown,
        Err(
            FamilyPipelineError::RepositorySource(RepositoryReadmeError::Unavailable)
            | FamilyPipelineError::Persistence(_)
            | FamilyPipelineError::Blob(_),
        ) => ExecutionOutcome::Retry,
        Err(error) => {
            let (code, retryable) = match error {
                FamilyPipelineError::Timeout => (AnalysisFailureCode::DependencyUnavailable, true),
                FamilyPipelineError::RepositorySource(RepositoryReadmeError::Unauthorized) => {
                    (AnalysisFailureCode::NotAuthorized, false)
                }
                FamilyPipelineError::RepositorySource(
                    RepositoryReadmeError::Missing
                    | RepositoryReadmeError::Oversized
                    | RepositoryReadmeError::Integrity,
                ) => (AnalysisFailureCode::SourceUnavailable, false),
                _ => (AnalysisFailureCode::ContractInvalid, false),
            };
            let payload = RepositoryAnalysisFailed {
                owner: request.owner,
                repository_id: request.repository_id,
                github_repository_numeric_id: request.github_repository_numeric_id,
                request_id: request.request_id,
                source_revision: request.source_revision,
                failure_code: code,
                retryable,
                failed_at: WireTimestamp::now(),
                extensions: Extensions::new(),
            };
            failed_fact(
                envelope,
                &payload,
                "evt.knowledge.repository_analysis.failed.v1",
            )
        }
    }
}

fn fact<P: EventPayload>(
    input: &EventEnvelope,
    payload: &P,
    subject: &'static str,
) -> ExecutionOutcome {
    terminal_envelope(input, payload).map_or(ExecutionOutcome::FinalFailure, |envelope| {
        ExecutionOutcome::Fact {
            event_type: P::EVENT_TYPE,
            subject,
            envelope,
        }
    })
}

fn failed_fact<P: EventPayload>(
    input: &EventEnvelope,
    payload: &P,
    subject: &'static str,
) -> ExecutionOutcome {
    terminal_envelope(input, payload).map_or(ExecutionOutcome::FinalFailure, |envelope| {
        ExecutionOutcome::FailedFact {
            event_type: P::EVENT_TYPE,
            subject,
            envelope,
        }
    })
}

fn terminal_envelope<P: EventPayload>(
    input: &EventEnvelope,
    payload: &P,
) -> Option<serde_json::Value> {
    let event_id = Uuid::now_v7();
    Some(serde_json::json!({
        "event_id": event_id,
        "event_type": P::EVENT_TYPE,
        "occurred_at": WireTimestamp::now(),
        "producer": "ratatoskr-knowledge",
        "aggregate_id": input.aggregate_id,
        "correlation_id": input.correlation_id,
        "causation_id": format!("event:{}", input.event_id),
        "tenant_id": input.tenant_id,
        "schema_version": 1,
        "payload": serde_json::to_value(payload).ok()?
    }))
}
