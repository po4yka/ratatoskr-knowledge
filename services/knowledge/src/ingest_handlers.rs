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
    FamilyPipelineError, LlmProvider, OutboxFact, OutboxSubject, ProviderError,
    RepositoryAnalysisConsumer, RepositoryReadmeError, SourceInbox, SourceInboxAdmission,
    SourceInboxError, collect_pending_blobs,
};
use ratatoskr_social_contracts::{
    SocialSourceAnalysisCompleted, SocialSourceCaptured, SocialSourceRemoved, SocialSourceSnapshot,
    SocialSourceUpdated,
};

use crate::ingest::{IngestContext, TERMINAL_DELIVERY};
use crate::ingest_deletion::{archive_acknowledgement, social_acknowledgement};

const DOCUMENT_PRODUCERS: &[&str] = &["ratatoskr-extractor"];
const SOCIAL_PRODUCERS: &[&str] = &["ratatoskr-x", "ratatoskr-instagram", "ratatoskr-threads"];
const ARCHIVE_PRODUCERS: &[&str] = &["ratatoskr-chatgpt", "ratatoskr-claude"];
const REPOSITORY_PRODUCERS: &[&str] = &["ratatoskr-github"];

/// The class logged when the provider refused a call for a reason only an operator can fix, such as
/// a rejected credential or an empty balance (XR-021 CONTRACTS.md R5-08 rule 1).
const OPERATOR_ACTION: &str = "provider_operator_action";

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
    /// The repository family is the one exception to the missing delivery budget, and the budget
    /// covers plain transient faults only: its handler stops returning this outcome for such a
    /// fault at [`TERMINAL_DELIVERY`] and settles a failed fact instead. An operator-fixable
    /// provider failure and a spend-budget refusal keep returning it at every delivery.
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
pub(crate) struct QueueFailure {
    /// The same fact can never be queued: it cannot be built or the schema cannot hold it.
    permanent: bool,
}

impl QueueFailure {
    /// The fact itself is not representable.
    const UNREPRESENTABLE: Self = Self { permanent: true };

    pub(crate) fn outcome(self, retry_class: &'static str) -> Outcome {
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
    } else if error.needs_operator() {
        Outcome::Retry(OPERATOR_ACTION)
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
            Err(error) if error.needs_operator() => Outcome::Retry(OPERATOR_ACTION),
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
            Ok(_) => {}
            Err(error) if error.is_permanent() => return Outcome::Reject("unprocessable"),
            Err(_) => return Outcome::Retry("social_removal"),
        }
        // The rows and the bytes are gone, or the delivery repeats a removal that was applied:
        // tell the producer (XR-021 CONTRACTS.md R5-11).
        self.acknowledge(social_acknowledgement(envelope, &removed), "social_removal")
            .await
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
            Err(ArchiveEventError::Inbox(SourceInboxError::ForeignSubject)) => {
                return self.refuse_foreign_tombstone(&envelope).await;
            }
            Err(error) => return archive_accept_failure(&error, &retry),
        };
        let inbox = SourceInbox::new(&self.database);
        let (conversation, admitted) = match admission {
            ArchiveEventAdmission::Conversation(admitted) => (true, admitted),
            ArchiveEventAdmission::Project(admitted) => (false, admitted),
            // A duplicate tombstone still drains the erasure queue: the delivery that committed
            // the row deletion may have failed or died before the blobs were erased.
            ArchiveEventAdmission::Tombstone | ArchiveEventAdmission::TombstoneDuplicate => {
                match collect_pending_blobs(&self.database, &self.blobs).await {
                    Ok(_) => {}
                    Err(error) if error.is_permanent() => return Outcome::Reject("unprocessable"),
                    Err(_) => return retry("archive_tombstone"),
                }
                // The rows and the bytes are gone, or the delivery repeats a tombstone that was
                // applied: tell the producer (XR-021 CONTRACTS.md R5-11).
                return self
                    .acknowledge(
                        archive_acknowledgement(&envelope, false),
                        "archive_tombstone",
                    )
                    .await;
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

    /// A tombstone whose subject belongs only to another owner: acknowledge it as refused, so the
    /// producer stops re-sending it, and terminate the delivery. The data stays where it is and
    /// an operator looks.
    async fn refuse_foreign_tombstone(&self, envelope: &EventEnvelope) -> Outcome {
        match self
            .acknowledge(archive_acknowledgement(envelope, true), "archive_tombstone")
            .await
        {
            Outcome::Done => Outcome::Reject("foreign_subject"),
            other => other,
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
        let (code, retryable) = match repository_disposition(&failure, delivered) {
            RepositoryDisposition::Wait(class) => return Outcome::Retry(class),
            RepositoryDisposition::RetryWithinBudget => {
                return Outcome::Retry("repository_analysis");
            }
            RepositoryDisposition::Settle(code, retryable) => (code, retryable),
        };
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

/// What the repository handler does with one failed delivery (XR-021 CONTRACTS.md R6-01 rule 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepositoryDisposition {
    /// The provider cannot answer until an operator or the spend window changes; redeliver at
    /// every delivery under this log class, and never settle.
    Wait(&'static str),
    /// A plain transient fault with budget left; redeliver.
    RetryWithinBudget,
    /// Record the request and settle this failure code; the flag says whether a later fresh
    /// request may succeed.
    Settle(AnalysisFailureCode, bool),
}

/// Decides the disposition of a failed repository delivery.
///
/// The five-delivery budget covers plain transient faults only: a transient provider failure, a
/// timeout, a held run, an invalid response and an unavailable README. A rejected credential, an
/// empty balance, a withdrawn model and a spend-budget refusal are waited out instead, because the
/// requester is told only by the terminal fact and GitHub never re-queues a request that settled
/// as failed (XR-021 CONTRACTS.md R6-01).
fn repository_disposition(error: &FamilyPipelineError, delivered: i64) -> RepositoryDisposition {
    if error.needs_operator() {
        return RepositoryDisposition::Wait(OPERATOR_ACTION);
    }
    if matches!(
        error,
        FamilyPipelineError::Provider(ProviderError::BudgetExhausted)
    ) {
        return RepositoryDisposition::Wait("repository_analysis");
    }
    let (code, retryable, transient) = classify_repository_failure(error);
    if transient && delivered < TERMINAL_DELIVERY {
        RepositoryDisposition::RetryWithinBudget
    } else {
        RepositoryDisposition::Settle(code, retryable)
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
    retry: &impl Fn(&'static str) -> Outcome,
) -> Outcome {
    match error {
        ArchiveEventError::Envelope(_)
        | ArchiveEventError::Inbox(SourceInboxError::Envelope(_)) => Outcome::Reject("undecodable"),
        ArchiveEventError::Unsupported => Outcome::Reject("unsupported"),
        ArchiveEventError::Inbox(SourceInboxError::InvalidArchiveFact) => {
            Outcome::Reject("invalid_fact")
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
    use ratatoskr_knowledge::{FamilyPipelineError, ProviderError, RepositoryReadmeError};

    use super::{
        RepositoryDisposition, TERMINAL_DELIVERY, classify_repository_failure,
        repository_disposition,
    };

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

    /// The table of R6-01 rule 2: only a refusal that waiting can cure never settles; a plain
    /// transient fault keeps the five-delivery budget; a source that can never be analysed settles
    /// at once.
    #[test]
    fn repository_disposition_table() {
        for delivered in [1, 5, 6, 1000] {
            assert_eq!(
                repository_disposition(
                    &FamilyPipelineError::Provider(ProviderError::OperatorAction),
                    delivered
                ),
                RepositoryDisposition::Wait("provider_operator_action"),
                "an operator-fixable failure waits at delivery {delivered}"
            );
            assert_eq!(
                repository_disposition(
                    &FamilyPipelineError::Provider(ProviderError::BudgetExhausted),
                    delivered
                ),
                RepositoryDisposition::Wait("repository_analysis"),
                "a spend-budget refusal waits at delivery {delivered}"
            );
        }
        let unavailable =
            || FamilyPipelineError::RepositorySource(RepositoryReadmeError::Unavailable);
        let budgeted: [(FamilyPipelineError, AnalysisFailureCode); 5] = [
            (
                FamilyPipelineError::Provider(ProviderError::Transient),
                AnalysisFailureCode::DependencyUnavailable,
            ),
            (
                FamilyPipelineError::Timeout,
                AnalysisFailureCode::DependencyUnavailable,
            ),
            (
                FamilyPipelineError::Conflict,
                AnalysisFailureCode::DependencyUnavailable,
            ),
            (
                FamilyPipelineError::Invalid,
                AnalysisFailureCode::DependencyUnavailable,
            ),
            (unavailable(), AnalysisFailureCode::SourceUnavailable),
        ];
        for (failure, code) in budgeted {
            for delivered in 1..TERMINAL_DELIVERY {
                assert_eq!(
                    repository_disposition(&failure, delivered),
                    RepositoryDisposition::RetryWithinBudget,
                    "{failure:?} retries at delivery {delivered}"
                );
            }
            for delivered in [TERMINAL_DELIVERY, TERMINAL_DELIVERY + 1, 1000] {
                assert_eq!(
                    repository_disposition(&failure, delivered),
                    RepositoryDisposition::Settle(code, true),
                    "{failure:?} settles at delivery {delivered}"
                );
            }
        }
        for permanent in [
            FamilyPipelineError::Source,
            FamilyPipelineError::RepositorySource(RepositoryReadmeError::Integrity),
        ] {
            assert_eq!(
                repository_disposition(&permanent, 1),
                RepositoryDisposition::Settle(AnalysisFailureCode::ContractInvalid, false),
                "{permanent:?}"
            );
        }
    }
}
