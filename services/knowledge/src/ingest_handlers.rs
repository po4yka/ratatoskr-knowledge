//! Per-family handlers of the ingest worker (XR-021 CONTRACTS.md S09).
//!
//! A handler turns one delivery into an [`Outcome`]; the consumer loop maps the outcome to the
//! broker disposition. Handlers are idempotent by identity: a redelivery after a crash between
//! the inbox commit and the analysis resumes the analysis instead of skipping it.

use std::time::Duration;

use ratatoskr_ai_archive_contracts::{
    AiArchiveAnalysisCompleted, AiArchiveImport, AiArchiveTombstone, AiArtifactAdded,
    AiArtifactUpdated, AiConversationAdded, AiConversationUpdated, AiProjectAdded,
    AiProjectUpdated,
};
use ratatoskr_event_envelope::{EventEnvelope, EventPayload};
use ratatoskr_github_contracts::{AnalysisFailureCode, RepositoryAnalysisRequested};
use ratatoskr_identifiers::{EntityRef, Extensions, TenantRef, WireTimestamp};
use ratatoskr_knowledge::{
    ArchiveEventAdmission, ArchiveEventConsumer, DocumentIntake, DocumentIntakeError, EventCause,
    EventOutbox, FamilyPipeline, FamilyPipelineError, LlmProvider, OutboxFact, OutboxSubject,
    RepositoryAnalysisConsumer, RepositoryReadmeError, SourceInbox, SourceInboxAdmission,
    collect_pending_blobs,
};
use ratatoskr_social_contracts::{
    SocialSourceAnalysisCompleted, SocialSourceCaptured, SocialSourceRemoved, SocialSourceSnapshot,
    SocialSourceUpdated,
};
use uuid::Uuid;

use crate::ingest::IngestContext;

const DOCUMENT_PRODUCERS: &[&str] = &["ratatoskr-extractor"];
const SOCIAL_PRODUCERS: &[&str] = &["ratatoskr-x", "ratatoskr-instagram", "ratatoskr-threads"];
const ARCHIVE_PRODUCERS: &[&str] = &["ratatoskr-chatgpt", "ratatoskr-claude"];
const REPOSITORY_PRODUCERS: &[&str] = &["ratatoskr-github"];

/// What a handler decided about one delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The durable outcome is committed; acknowledge.
    Done,
    /// The delivery can never succeed (unknown type, wrong producer, missing tenant, invalid
    /// payload); terminate it. The class is logged, never content.
    Reject(&'static str),
    /// The delivery failed in a way a later delivery may not; redeliver after a delay, and give
    /// up after the delivery budget.
    Retry { class: &'static str, event_id: Uuid },
}

fn decode(bytes: &[u8], producers: &[&str]) -> Result<(EventEnvelope, TenantRef), Outcome> {
    let envelope = EventEnvelope::from_json(bytes).map_err(|_| Outcome::Reject("undecodable"))?;
    if !producers.contains(&envelope.producer.as_str()) {
        return Err(Outcome::Reject("producer"));
    }
    let tenant = envelope.tenant_id.ok_or(Outcome::Reject("tenant"))?;
    Ok((envelope, tenant))
}

/// A completion fact could not be queued.
#[derive(Debug, Clone, Copy)]
struct QueueFailure;

impl<P: LlmProvider> IngestContext<P> {
    fn provider_timeout(&self) -> Duration {
        Duration::from_millis(self.config.limits.provider_timeout_ms)
    }

    fn pipeline(&self) -> FamilyPipeline<'_, P> {
        FamilyPipeline::new(
            &self.database,
            &*self.provider,
            &self.blobs,
            self.provider_timeout(),
        )
    }

    /// `content.document.extracted.v1`: register the document and analyse it once.
    pub(crate) async fn documents(&self, bytes: &[u8]) -> Outcome {
        let (envelope, _tenant) = match decode(bytes, DOCUMENT_PRODUCERS) {
            Ok(decoded) => decoded,
            Err(outcome) => return outcome,
        };
        let intake = DocumentIntake::new(
            &self.database,
            self.config.limits.context_characters,
            self.provider_timeout(),
        );
        match intake.accept(&envelope, &*self.provider, &self.blobs).await {
            Ok(_) => Outcome::Done,
            Err(
                DocumentIntakeError::UnsupportedEvent
                | DocumentIntakeError::WrongProducer
                | DocumentIntakeError::MissingTenant
                | DocumentIntakeError::InvalidPayload
                | DocumentIntakeError::Context(_)
                | DocumentIntakeError::Request(_),
            ) => Outcome::Reject("document"),
            Err(_) => Outcome::Retry {
                class: "document_analysis",
                event_id: envelope.event_id.0,
            },
        }
    }

    /// `social.source.*`: claim the delivery, analyse the current head, queue the completion.
    pub(crate) async fn social(&self, bytes: &[u8]) -> Outcome {
        let (envelope, tenant) = match decode(bytes, SOCIAL_PRODUCERS) {
            Ok(decoded) => decoded,
            Err(outcome) => return outcome,
        };
        let subject = envelope.event_type.to_wire();
        let retry = |class| Outcome::Retry {
            class,
            event_id: envelope.event_id.0,
        };
        let snapshot = match subject.as_str() {
            SocialSourceCaptured::EVENT_TYPE => envelope
                .payload_as::<SocialSourceCaptured>()
                .map(|payload| payload.source),
            SocialSourceUpdated::EVENT_TYPE => envelope
                .payload_as::<SocialSourceUpdated>()
                .map(|payload| payload.source),
            SocialSourceRemoved::EVENT_TYPE => return self.social_removed(&envelope, tenant).await,
            _ => return Outcome::Reject("type"),
        };
        let Ok(snapshot) = snapshot else {
            return Outcome::Reject("payload");
        };
        if snapshot.owner != tenant {
            return Outcome::Reject("owner");
        }
        let inbox = SourceInbox::new(&self.database);
        let analyse = match inbox
            .accept_social(envelope.event_id.0, &subject, &snapshot)
            .await
        {
            Ok(SourceInboxAdmission::AcceptedCurrent) => true,
            Ok(SourceInboxAdmission::Duplicate) => {
                match inbox.is_current_head(envelope.event_id.0).await {
                    Ok(head) => head,
                    Err(_) => return retry("social_inbox"),
                }
            }
            Ok(SourceInboxAdmission::AcceptedHistorical | SourceInboxAdmission::Tombstoned) => {
                false
            }
            Err(_) => return retry("social_inbox"),
        };
        if !analyse {
            return Outcome::Done;
        }
        if self
            .pipeline()
            .execute_social_event(&inbox, envelope.event_id.0)
            .await
            .is_err()
        {
            return retry("social_analysis");
        }
        match self.queue_social_completion(&envelope, &snapshot).await {
            Ok(()) => Outcome::Done,
            Err(QueueFailure) => retry("social_outbox"),
        }
    }

    async fn social_removed(&self, envelope: &EventEnvelope, tenant: TenantRef) -> Outcome {
        let Ok(removed) = envelope.payload_as::<SocialSourceRemoved>() else {
            return Outcome::Reject("payload");
        };
        if removed.owner != tenant {
            return Outcome::Reject("owner");
        }
        let admitted = SourceInbox::new(&self.database)
            .accept_social_removed(envelope.event_id.0, &removed)
            .await;
        if admitted.is_err()
            || collect_pending_blobs(&self.database, &self.blobs)
                .await
                .is_err()
        {
            return Outcome::Retry {
                class: "social_removal",
                event_id: envelope.event_id.0,
            };
        }
        Outcome::Done
    }

    async fn queue_social_completion(
        &self,
        envelope: &EventEnvelope,
        snapshot: &SocialSourceSnapshot,
    ) -> Result<(), QueueFailure> {
        let cause = EventCause::of(envelope);
        let completed = SocialSourceAnalysisCompleted {
            owner: snapshot.owner,
            social_source_id: snapshot.social_source_id,
            content_digest: snapshot.content_digest.clone(),
            completed_at: WireTimestamp::now(),
            extensions: Extensions::new(),
        };
        let fact = OutboxFact {
            subject: OutboxSubject::AnalysisCompleted,
            tenant: snapshot.owner,
            aggregate_id: EntityRef::parse(&format!("social_source:{}", snapshot.social_source_id))
                .map_err(|_| QueueFailure)?,
            correlation_id: cause.correlation_id.clone(),
            causation_id: Some(cause.causation_ref().map_err(|_| QueueFailure)?),
            payload: serde_json::to_value(&completed).map_err(|_| QueueFailure)?,
            dedupe_key: format!(
                "social:{}:{}:{}",
                snapshot.owner, snapshot.social_source_id, snapshot.content_digest.hex
            ),
        };
        EventOutbox::new(&self.database)
            .enqueue(&fact)
            .await
            .map(|_| ())
            .map_err(|_| QueueFailure)
    }

    /// `ai_archive.*`: claim the delivery, analyse conversations and projects, apply tombstones.
    pub(crate) async fn ai_archive(&self, bytes: &[u8]) -> Outcome {
        let (envelope, tenant) = match decode(bytes, ARCHIVE_PRODUCERS) {
            Ok(decoded) => decoded,
            Err(outcome) => return outcome,
        };
        let retry = |class| Outcome::Retry {
            class,
            event_id: envelope.event_id.0,
        };
        match archive_owner(&envelope) {
            Ok(owner) if owner == tenant => {}
            Ok(_) => return Outcome::Reject("owner"),
            Err(outcome) => return outcome,
        }
        let Ok(admission) = ArchiveEventConsumer::new(&self.database)
            .accept(&envelope)
            .await
        else {
            return retry("archive_inbox");
        };
        let inbox = SourceInbox::new(&self.database);
        let (conversation, admitted) = match admission {
            ArchiveEventAdmission::Conversation(admitted) => (true, admitted),
            ArchiveEventAdmission::Project(admitted) => (false, admitted),
            ArchiveEventAdmission::Tombstone => {
                return match collect_pending_blobs(&self.database, &self.blobs).await {
                    Ok(_) => Outcome::Done,
                    Err(_) => retry("archive_tombstone"),
                };
            }
            ArchiveEventAdmission::ObjectRecorded
            | ArchiveEventAdmission::ObjectDuplicate
            | ArchiveEventAdmission::TombstoneDuplicate => return Outcome::Done,
        };
        let analyse = match admitted {
            SourceInboxAdmission::AcceptedCurrent => true,
            SourceInboxAdmission::Duplicate => {
                match inbox.is_current_head(envelope.event_id.0).await {
                    Ok(head) => head,
                    Err(_) => return retry("archive_inbox"),
                }
            }
            SourceInboxAdmission::AcceptedHistorical | SourceInboxAdmission::Tombstoned => false,
        };
        if !analyse {
            return Outcome::Done;
        }
        let pipeline = self.pipeline();
        if conversation {
            match pipeline
                .execute_archive_event(&inbox, envelope.event_id.0)
                .await
            {
                Ok(execution) => match self
                    .queue_archive_completion(&envelope, tenant, &execution.completion)
                    .await
                {
                    Ok(()) => Outcome::Done,
                    Err(QueueFailure) => retry("archive_outbox"),
                },
                Err(_) => retry("archive_analysis"),
            }
        } else {
            match pipeline
                .execute_archive_project_event(&inbox, envelope.event_id.0)
                .await
            {
                Ok(_) => Outcome::Done,
                Err(_) => retry("archive_analysis"),
            }
        }
    }

    async fn queue_archive_completion(
        &self,
        envelope: &EventEnvelope,
        tenant: TenantRef,
        completion: &AiArchiveAnalysisCompleted,
    ) -> Result<(), QueueFailure> {
        let cause = EventCause::of(envelope);
        let ratatoskr_ai_archive_contracts::AiArchiveSubject::Conversation { ai_conversation_id } =
            &completion.subject
        else {
            return Err(QueueFailure);
        };
        let fact = OutboxFact {
            subject: OutboxSubject::AiArchiveAnalysisCompleted,
            tenant,
            aggregate_id: EntityRef::parse(&format!("ai_conversation:{ai_conversation_id}"))
                .map_err(|_| QueueFailure)?,
            correlation_id: cause.correlation_id.clone(),
            causation_id: Some(cause.causation_ref().map_err(|_| QueueFailure)?),
            payload: serde_json::to_value(completion).map_err(|_| QueueFailure)?,
            dedupe_key: format!(
                "ai_archive:{tenant}:conversation:{ai_conversation_id}:{}",
                completion.content_digest.hex
            ),
        };
        EventOutbox::new(&self.database)
            .enqueue(&fact)
            .await
            .map(|_| ())
            .map_err(|_| QueueFailure)
    }

    /// `knowledge.repository_analysis.requested.v1`: analyse with the GitHub README, settle.
    pub(crate) async fn repository(&self, bytes: &[u8], delivered: i64) -> Outcome {
        let (envelope, tenant) = match decode(bytes, REPOSITORY_PRODUCERS) {
            Ok(decoded) => decoded,
            Err(outcome) => return outcome,
        };
        let Ok(request) = envelope.payload_as::<RepositoryAnalysisRequested>() else {
            return Outcome::Reject("payload");
        };
        if request.owner != tenant {
            return Outcome::Reject("owner");
        }
        let cause = EventCause::of(&envelope);
        let Err(failure) = self
            .pipeline()
            .execute_repository(&request, &cause, &self.readme)
            .await
        else {
            return Outcome::Done;
        };
        let (code, retryable, transient) = classify_repository_failure(&failure);
        if transient && delivered < crate::ingest::TERMINAL_DELIVERY {
            return Outcome::Retry {
                class: "repository_analysis",
                event_id: envelope.event_id.0,
            };
        }
        match RepositoryAnalysisConsumer::new(&self.database)
            .fail(&request, code, retryable)
            .await
        {
            Ok(_) => Outcome::Done,
            Err(_) => Outcome::Retry {
                class: "repository_settlement",
                event_id: envelope.event_id.0,
            },
        }
    }
}

/// Maps an analysis failure to the safe vocabulary of the terminal fact.
///
/// Returns the failure code, whether a later fresh request may succeed, and whether another
/// delivery of the same request might still succeed.
fn classify_repository_failure(error: &FamilyPipelineError) -> (AnalysisFailureCode, bool, bool) {
    match error {
        FamilyPipelineError::RepositorySource(RepositoryReadmeError::Integrity)
        | FamilyPipelineError::Source => (AnalysisFailureCode::ContractInvalid, false, false),
        FamilyPipelineError::RepositorySource(_) => {
            (AnalysisFailureCode::SourceUnavailable, true, true)
        }
        _ => (AnalysisFailureCode::DependencyUnavailable, true, true),
    }
}

/// The owner an archive event names, from its typed payload.
fn archive_owner(envelope: &EventEnvelope) -> Result<TenantRef, Outcome> {
    let payload = Outcome::Reject("payload");
    let subject = envelope.event_type.to_wire();
    match subject.as_str() {
        AiArchiveImport::EVENT_TYPE => envelope
            .payload_as::<AiArchiveImport>()
            .map(|import| import.owner),
        AiConversationAdded::EVENT_TYPE => envelope
            .payload_as::<AiConversationAdded>()
            .map(|added| added.import_provenance.owner),
        AiConversationUpdated::EVENT_TYPE => envelope
            .payload_as::<AiConversationUpdated>()
            .map(|updated| updated.import_provenance.owner),
        AiProjectAdded::EVENT_TYPE => envelope
            .payload_as::<AiProjectAdded>()
            .map(|added| added.import_provenance.owner),
        AiProjectUpdated::EVENT_TYPE => envelope
            .payload_as::<AiProjectUpdated>()
            .map(|updated| updated.import_provenance.owner),
        AiArtifactAdded::EVENT_TYPE => envelope
            .payload_as::<AiArtifactAdded>()
            .map(|added| added.artifact.owner),
        AiArtifactUpdated::EVENT_TYPE => envelope
            .payload_as::<AiArtifactUpdated>()
            .map(|updated| updated.artifact.owner),
        AiArchiveTombstone::EVENT_TYPE => envelope
            .payload_as::<AiArchiveTombstone>()
            .map(|tombstone| tombstone.owner),
        _ => return Err(Outcome::Reject("type")),
    }
    .map_err(|_| payload)
}
