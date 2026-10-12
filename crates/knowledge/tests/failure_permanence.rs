//! Which failures can never succeed on a redelivery (XR-021 CONTRACTS.md R4-05 rule 1).
//!
//! The ingest worker redelivers a retryable failure without a delivery budget, so every failure
//! that a later delivery cannot fix must say so, or it would be redelivered for ever. Each variant
//! of the failure types the unbudgeted families can see is decided here.

use ratatoskr_knowledge::{
    ArchiveEventError, ArticleValidationError, BlobError, ContextError, DeletionError,
    DocumentIntakeError, EventOutboxError, FamilyPipelineError, PersistenceError, PipelineError,
    ProviderError, RepositoryReadmeError, SourceInboxError,
};

fn encode_error() -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom("not encodable")
}

fn database_outage() -> PersistenceError {
    PersistenceError::Query(sqlx::Error::PoolTimedOut)
}

fn io() -> BlobError {
    BlobError::Io(std::io::Error::other("disk"))
}

#[test]
fn only_a_transient_budget_or_operator_provider_failure_is_worth_another_delivery() {
    // A spend ceiling is a daily and monthly window: it clears at the rollover or when the
    // operator raises it, so a later delivery can succeed. So does a rejected credential, an
    // empty balance or a withdrawn model, once the operator has fixed it (XR-021 CONTRACTS.md
    // R5-08 rule 1).
    for recovers in [
        ProviderError::Transient,
        ProviderError::BudgetExhausted,
        ProviderError::OperatorAction,
    ] {
        assert!(!recovers.is_permanent(), "{recovers:?}");
    }
    for permanent in [
        ProviderError::Permanent,
        ProviderError::Exhausted,
        ProviderError::Internal,
    ] {
        assert!(permanent.is_permanent(), "{permanent:?}");
    }
}

#[test]
fn a_storage_outage_recovers_and_an_unrepresentable_input_does_not() {
    for recovers in [
        database_outage(),
        PersistenceError::Connect(sqlx::Error::PoolClosed),
        PersistenceError::Schema(sqlx::Error::PoolClosed),
        PersistenceError::Blob(io()),
    ] {
        assert!(!recovers.is_permanent(), "{recovers:?}");
    }
    for never in [
        PersistenceError::InvalidSource,
        PersistenceError::InvalidAnalysisIdentity,
        PersistenceError::AttemptBudgetExhausted,
        PersistenceError::ValueOverflow,
        PersistenceError::Encode(encode_error()),
        PersistenceError::Blob(BlobError::TooLarge),
    ] {
        assert!(never.is_permanent(), "{never:?}");
    }
}

#[test]
fn a_blob_that_is_too_large_never_fits_and_a_filesystem_failure_recovers() {
    assert!(BlobError::TooLarge.is_permanent());
    assert!(BlobError::InvalidReference.is_permanent());
    assert!(!BlobError::Unavailable.is_permanent());
    assert!(!io().is_permanent());
}

#[test]
fn an_article_pipeline_failure_is_permanent_only_when_the_run_cannot_recover() {
    for recovers in [
        PipelineError::Timeout,
        PipelineError::Conflict,
        PipelineError::Provider(ProviderError::Transient),
        PipelineError::Provider(ProviderError::BudgetExhausted),
        PipelineError::Provider(ProviderError::OperatorAction),
        PipelineError::Persistence(database_outage()),
        PipelineError::Blob(io()),
    ] {
        assert!(!recovers.is_permanent(), "{recovers:?}");
    }
    for never in [
        PipelineError::Invalid,
        PipelineError::Provider(ProviderError::Permanent),
        PipelineError::Persistence(PersistenceError::AttemptBudgetExhausted),
        PipelineError::Blob(BlobError::TooLarge),
    ] {
        assert!(never.is_permanent(), "{never:?}");
    }
}

#[test]
fn a_family_pipeline_failure_is_permanent_only_when_the_source_cannot_recover() {
    for recovers in [
        FamilyPipelineError::Timeout,
        FamilyPipelineError::Conflict,
        FamilyPipelineError::Provider(ProviderError::Transient),
        FamilyPipelineError::Provider(ProviderError::BudgetExhausted),
        FamilyPipelineError::Provider(ProviderError::OperatorAction),
        FamilyPipelineError::Persistence(database_outage()),
        FamilyPipelineError::Blob(io()),
        FamilyPipelineError::Inbox(SourceInboxError::Persistence(database_outage())),
        FamilyPipelineError::RepositorySource(RepositoryReadmeError::Unavailable),
    ] {
        assert!(!recovers.is_permanent(), "{recovers:?}");
    }
    for never in [
        FamilyPipelineError::Invalid,
        FamilyPipelineError::Source,
        FamilyPipelineError::Contract(encode_error()),
        FamilyPipelineError::Provider(ProviderError::Permanent),
        FamilyPipelineError::Persistence(PersistenceError::InvalidAnalysisIdentity),
        FamilyPipelineError::Blob(BlobError::TooLarge),
        FamilyPipelineError::Inbox(SourceInboxError::InvalidArchiveFact),
        FamilyPipelineError::RepositorySource(RepositoryReadmeError::Integrity),
    ] {
        assert!(never.is_permanent(), "{never:?}");
    }
}

#[test]
fn a_document_intake_failure_is_permanent_when_the_delivery_or_the_analysis_is() {
    for recovers in [
        DocumentIntakeError::Persistence(database_outage()),
        DocumentIntakeError::Analysis(PipelineError::Timeout),
        DocumentIntakeError::Analysis(PipelineError::Provider(ProviderError::Transient)),
        DocumentIntakeError::Analysis(PipelineError::Provider(ProviderError::OperatorAction)),
        DocumentIntakeError::Analysis(PipelineError::Conflict),
    ] {
        assert!(!recovers.is_permanent(), "{recovers:?}");
    }
    for never in [
        DocumentIntakeError::UnsupportedEvent,
        DocumentIntakeError::WrongProducer,
        DocumentIntakeError::MissingTenant,
        DocumentIntakeError::InvalidPayload,
        DocumentIntakeError::Context(ContextError::BudgetTooSmall),
        DocumentIntakeError::Request(ArticleValidationError::SchemaDefinition),
        DocumentIntakeError::Persistence(PersistenceError::InvalidSource),
        DocumentIntakeError::Analysis(PipelineError::Invalid),
        DocumentIntakeError::Analysis(PipelineError::Provider(ProviderError::Permanent)),
    ] {
        assert!(never.is_permanent(), "{never:?}");
    }
}

#[test]
fn an_inbox_or_deletion_failure_is_permanent_only_when_the_delivery_is() {
    for recovers in [
        SourceInboxError::Persistence(database_outage()),
        SourceInboxError::Deletion(DeletionError::Query(sqlx::Error::PoolTimedOut)),
        SourceInboxError::Deletion(DeletionError::Blob(io())),
        SourceInboxError::Deletion(DeletionError::VerificationFailed),
    ] {
        assert!(!recovers.is_permanent(), "{recovers:?}");
    }
    for never in [
        SourceInboxError::Encode(encode_error()),
        SourceInboxError::InvalidArchiveFact,
        SourceInboxError::Persistence(PersistenceError::ValueOverflow),
        SourceInboxError::Deletion(DeletionError::ValueOverflow),
    ] {
        assert!(never.is_permanent(), "{never:?}");
    }
    assert!(!ArchiveEventError::Persistence(database_outage()).is_permanent());
    assert!(
        !ArchiveEventError::Inbox(SourceInboxError::Persistence(database_outage())).is_permanent()
    );
    assert!(ArchiveEventError::Unsupported.is_permanent());
    assert!(ArchiveEventError::Inbox(SourceInboxError::InvalidArchiveFact).is_permanent());
}

#[test]
fn an_outbox_row_that_is_not_representable_is_never_queued() {
    assert!(EventOutboxError::InvalidRow.is_permanent());
    assert!(EventOutboxError::Persistence(PersistenceError::ValueOverflow).is_permanent());
    assert!(!EventOutboxError::Persistence(database_outage()).is_permanent());
}
