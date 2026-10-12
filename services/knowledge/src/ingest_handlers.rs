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
    ArchiveEventAdmission, ArchiveEventConsumer, ArchiveEventError, DocumentIntake,
    DocumentIntakeError, EventCause, EventOutbox, EventOutboxError, FamilyPipeline,
    FamilyPipelineError, LlmProvider, OutboxFact, OutboxSubject, RepositoryAnalysisConsumer,
    RepositoryReadmeError, SourceInbox, SourceInboxAdmission, SourceInboxError,
    collect_pending_blobs,
};
use ratatoskr_social_contracts::{
    SocialSourceAnalysisCompleted, SocialSourceCaptured, SocialSourceRemoved, SocialSourceSnapshot,
    SocialSourceUpdated,
};

use crate::ingest::{IngestContext, TERMINAL_DELIVERY};

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
    /// The delivery failed in a way a later delivery may not; redeliver it after a delay that
    /// grows with the delivery count, until it succeeds or turns out to be a [`Outcome::Reject`].
    /// The class is logged, never content.
    ///
    /// The repository family is the one exception to the missing delivery budget: its handler
    /// stops returning this outcome at [`TERMINAL_DELIVERY`] and settles a failed fact instead.
    Retry(&'static str),
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
struct QueueFailure {
    /// The same fact can never be queued: it cannot be built or the schema cannot hold it.
    permanent: bool,
}

impl QueueFailure {
    /// The fact itself is not representable.
    const UNREPRESENTABLE: Self = Self { permanent: true };

    fn outcome(self, retry_class: &'static str) -> Outcome {
        if self.permanent {
            Outcome::Reject("unqueueable")
        } else {
            Outcome::Retry(retry_class)
        }
    }
}

impl From<EventOutboxError> for QueueFailure {
    fn from(error: EventOutboxError) -> Self {
        Self {
            permanent: error.is_permanent(),
        }
    }
}

/// A failed inbox write or read: a delivery the schema cannot hold is rejected, anything else is
/// the database and is redelivered.
fn inbox_failure(error: &SourceInboxError, retry_class: &'static str) -> Outcome {
    if error.is_permanent() {
        Outcome::Reject("inbox_invalid")
    } else {
        Outcome::Retry(retry_class)
    }
}

/// A failed analysis: a source that can never be analysed is rejected, anything else is the
/// provider, a timeout, a held run or the database and is redelivered.
fn analysis_failure(error: &FamilyPipelineError, retry_class: &'static str) -> Outcome {
    if error.is_permanent() {
        Outcome::Reject("unanalysable")
    } else {
        Outcome::Retry(retry_class)
    }
}

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
            Err(error) if error.is_permanent() => Outcome::Reject(match error {
                DocumentIntakeError::Persistence(_) | DocumentIntakeError::Analysis(_) => {
                    "unanalysable"
                }
                _ => "document",
            }),
            Err(_) => Outcome::Retry("document_analysis"),
        }
    }

    /// `social.source.*`: claim the delivery, analyse the current head, queue the completion.
    pub(crate) async fn social(&self, bytes: &[u8]) -> Outcome {
        let (envelope, tenant) = match decode(bytes, SOCIAL_PRODUCERS) {
            Ok(decoded) => decoded,
            Err(outcome) => return outcome,
        };
        let subject = envelope.event_type.to_wire();
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
                    Err(error) => return inbox_failure(&error, "social_inbox"),
                }
            }
            Ok(SourceInboxAdmission::AcceptedHistorical | SourceInboxAdmission::Tombstoned) => {
                false
            }
            Err(error) => return inbox_failure(&error, "social_inbox"),
        };
        if !analyse {
            return Outcome::Done;
        }
        if let Err(error) = self
            .pipeline()
            .execute_social_event(&inbox, envelope.event_id.0)
            .await
        {
            return analysis_failure(&error, "social_analysis");
        }
        match self.queue_social_completion(&envelope, &snapshot).await {
            Ok(()) => Outcome::Done,
            Err(failure) => failure.outcome("social_outbox"),
        }
    }

    async fn social_removed(&self, envelope: &EventEnvelope, tenant: TenantRef) -> Outcome {
        let Ok(removed) = envelope.payload_as::<SocialSourceRemoved>() else {
            return Outcome::Reject("payload");
        };
        if removed.owner != tenant {
            return Outcome::Reject("owner");
        }
        if let Err(error) = SourceInbox::new(&self.database)
            .accept_social_removed(envelope.event_id.0, &removed)
            .await
        {
            return inbox_failure(&error, "social_removal");
        }
        // The removal committed its row deletions and its erasure queue; a redelivery is a
        // duplicate that drains the queue again, so a failed erasure is finished by the next
        // delivery.
        match collect_pending_blobs(&self.database, &self.blobs).await {
            Ok(_) => Outcome::Done,
            Err(error) if error.is_permanent() => Outcome::Reject("unprocessable"),
            Err(_) => Outcome::Retry("social_removal"),
        }
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
                .map_err(|_| QueueFailure::UNREPRESENTABLE)?,
            correlation_id: cause.correlation_id.clone(),
            causation_id: Some(
                cause
                    .causation_ref()
                    .map_err(|_| QueueFailure::UNREPRESENTABLE)?,
            ),
            payload: serde_json::to_value(&completed).map_err(|_| QueueFailure::UNREPRESENTABLE)?,
            dedupe_key: format!(
                "social:{}:{}:{}",
                snapshot.owner, snapshot.social_source_id, snapshot.content_digest.hex
            ),
        };
        EventOutbox::new(&self.database)
            .enqueue(&fact)
            .await
            .map(|_| ())
            .map_err(QueueFailure::from)
    }

    /// `ai_archive.*`: claim the delivery, analyse conversations and projects, apply tombstones.
    ///
    /// Every transient failure is redelivered without a delivery budget (XR-021 CONTRACTS.md R4-05),
    /// and a tombstone, a privacy deletion, always reports the class `archive_tombstone`
    /// (R3-08). A fact that can never succeed is rejected at its first delivery.
    pub(crate) async fn ai_archive(&self, bytes: &[u8]) -> Outcome {
        let (envelope, tenant) = match decode(bytes, ARCHIVE_PRODUCERS) {
            Ok(decoded) => decoded,
            Err(outcome) => return outcome,
        };
        let tombstone = envelope.event_type.to_wire() == AiArchiveTombstone::EVENT_TYPE;
        let retry = |class| {
            Outcome::Retry(if tombstone {
                "archive_tombstone"
            } else {
                class
            })
        };
        match archive_owner(&envelope) {
            Ok(owner) if owner == tenant => {}
            Ok(_) => return Outcome::Reject("owner"),
            Err(outcome) => return outcome,
        }
        let admission = match ArchiveEventConsumer::new(&self.database)
            .accept(&envelope)
            .await
        {
            Ok(admission) => admission,
            Err(error) => return archive_accept_failure(&error, tombstone, &retry),
        };
        let inbox = SourceInbox::new(&self.database);
        let (conversation, admitted) = match admission {
            ArchiveEventAdmission::Conversation(admitted) => (true, admitted),
            ArchiveEventAdmission::Project(admitted) => (false, admitted),
            // A duplicate tombstone still drains the erasure queue: the delivery that committed
            // the row deletion may have failed or died before the blobs were erased.
            ArchiveEventAdmission::Tombstone | ArchiveEventAdmission::TombstoneDuplicate => {
                return match collect_pending_blobs(&self.database, &self.blobs).await {
                    Ok(_) => Outcome::Done,
                    Err(error) if error.is_permanent() => Outcome::Reject("unprocessable"),
                    Err(_) => retry("archive_tombstone"),
                };
            }
            ArchiveEventAdmission::ObjectRecorded | ArchiveEventAdmission::ObjectDuplicate => {
                return Outcome::Done;
            }
        };
        let analyse = match admitted {
            SourceInboxAdmission::AcceptedCurrent => true,
            SourceInboxAdmission::Duplicate => {
                match inbox.is_current_head(envelope.event_id.0).await {
                    Ok(head) => head,
                    Err(error) if error.is_permanent() => return Outcome::Reject("inbox_invalid"),
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
                    Err(failure) => failure.outcome("archive_outbox"),
                },
                Err(error) => analysis_failure(&error, "archive_analysis"),
            }
        } else {
            match pipeline
                .execute_archive_project_event(&inbox, envelope.event_id.0)
                .await
            {
                Ok(_) => Outcome::Done,
                Err(error) => analysis_failure(&error, "archive_analysis"),
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
            return Err(QueueFailure::UNREPRESENTABLE);
        };
        let fact = OutboxFact {
            subject: OutboxSubject::AiArchiveAnalysisCompleted,
            tenant,
            aggregate_id: EntityRef::parse(&format!("ai_conversation:{ai_conversation_id}"))
                .map_err(|_| QueueFailure::UNREPRESENTABLE)?,
            correlation_id: cause.correlation_id.clone(),
            causation_id: Some(
                cause
                    .causation_ref()
                    .map_err(|_| QueueFailure::UNREPRESENTABLE)?,
            ),
            payload: serde_json::to_value(completion).map_err(|_| QueueFailure::UNREPRESENTABLE)?,
            dedupe_key: format!(
                "ai_archive:{tenant}:conversation:{ai_conversation_id}:{}",
                completion.content_digest.hex
            ),
        };
        EventOutbox::new(&self.database)
            .enqueue(&fact)
            .await
            .map(|_| ())
            .map_err(QueueFailure::from)
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
        if transient && delivered < TERMINAL_DELIVERY {
            return Outcome::Retry("repository_analysis");
        }
        let settlement_retry = Outcome::Retry("repository_settlement");
        let consumer = RepositoryAnalysisConsumer::new(&self.database);
        // The pipeline can fail before it stored the request, and a request that is not stored
        // cannot be settled: record it first (a no-op when it exists) so the terminal fact always
        // has a row to hang on. A request that can never be recorded has no fact to carry.
        match consumer.accept(&request, &cause).await {
            Ok(_) => {}
            Err(error) if error.is_permanent() => return Outcome::Reject("repository_request"),
            Err(_) => return settlement_retry,
        }
        match consumer.fail(&request, code, retryable).await {
            // `None` is a request that already reached a terminal state: its fact exists.
            Ok(_) => Outcome::Done,
            Err(_) => settlement_retry,
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

/// Maps a failed `accept` to a disposition: a fact that can never succeed is rejected with a
/// closed class, and a storage failure is left to `retry`.
fn archive_accept_failure(
    error: &ArchiveEventError,
    tombstone: bool,
    retry: &impl Fn(&'static str) -> Outcome,
) -> Outcome {
    match error {
        ArchiveEventError::Envelope(_)
        | ArchiveEventError::Inbox(SourceInboxError::Envelope(_)) => Outcome::Reject("undecodable"),
        ArchiveEventError::Unsupported => Outcome::Reject("unsupported"),
        ArchiveEventError::Inbox(SourceInboxError::InvalidArchiveFact) => {
            Outcome::Reject(if tombstone {
                "foreign_subject"
            } else {
                "invalid_fact"
            })
        }
        _ if error.is_permanent() => Outcome::Reject("inbox_invalid"),
        _ => retry("archive_inbox"),
    }
}

/// The owner an archive event names, from its typed payload.
fn archive_owner(envelope: &EventEnvelope) -> Result<TenantRef, Outcome> {
    let payload = Outcome::Reject("undecodable");
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
        _ => return Err(Outcome::Reject("unsupported")),
    }
    .map_err(|_| payload)
}

#[cfg(test)]
mod tests {
    use ratatoskr_github_contracts::AnalysisFailureCode;
    use ratatoskr_knowledge::{FamilyPipelineError, ProviderError};

    use super::classify_repository_failure;

    /// A run another executor holds, a provider that failed and a response that was invalid are
    /// all things a later delivery may not meet again; only a source that can never be analysed is
    /// a permanent contract failure.
    #[test]
    fn only_a_source_error_is_a_permanent_contract_failure() {
        for transient in [
            FamilyPipelineError::Conflict,
            FamilyPipelineError::Provider(ProviderError::Transient),
            FamilyPipelineError::Timeout,
            FamilyPipelineError::Invalid,
        ] {
            assert_eq!(
                classify_repository_failure(&transient),
                (AnalysisFailureCode::DependencyUnavailable, true, true),
                "{transient:?}"
            );
        }
        assert_eq!(
            classify_repository_failure(&FamilyPipelineError::Source),
            (AnalysisFailureCode::ContractInvalid, false, false)
        );
    }
}
