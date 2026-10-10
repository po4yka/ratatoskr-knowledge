//! Durable intake and terminal-fact construction for repository-analysis requests.

use ratatoskr_github_contracts::{
    AnalysisFailureCode, RepositoryAnalysisCompleted, RepositoryAnalysisFailed,
    RepositoryAnalysisRequested,
};
use ratatoskr_identifiers::{EntityRef, Extensions};

use crate::{
    Database, EventCause, EventOutbox, EventOutboxError, OutboxFact, OutboxSubject,
    PersistenceError,
};

/// Outcome of accepting one repository-analysis request delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryAnalysisAdmission {
    /// The delivery created the one durable pending request for its idempotency key.
    Accepted,
    /// The idempotency key was already present; no second request was created.
    Duplicate,
}

/// Safe repository-analysis intake failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RepositoryAnalysisError {
    /// The supplied idempotency digest identifies different immutable request data.
    #[error("the repository-analysis idempotency digest conflicts with an existing request")]
    IdempotencyConflict,
    /// Knowledge-owned persistence failed.
    #[error("the repository-analysis request could not be persisted")]
    Persistence(#[from] PersistenceError),
    /// A contract value could not be encoded into Knowledge-owned storage.
    #[error("the repository-analysis contract value could not be encoded")]
    Encode(#[source] serde_json::Error),
    /// The terminal fact could not be committed to the outbox.
    #[error("the repository-analysis terminal fact could not be queued")]
    Outbox(#[from] EventOutboxError),
}

impl RepositoryAnalysisError {
    /// Whether the same request can never be stored or settled, however often it is retried.
    ///
    /// A permanent error is a property of the request: a conflicting idempotency digest, an
    /// identity or number the schema cannot represent, or a value that cannot be encoded. Every
    /// other error is the database or the outbox failing and is worth another delivery.
    #[must_use]
    pub const fn is_permanent(&self) -> bool {
        match self {
            Self::IdempotencyConflict
            | Self::Encode(_)
            | Self::Outbox(EventOutboxError::InvalidRow) => true,
            Self::Persistence(error) | Self::Outbox(EventOutboxError::Persistence(error)) => {
                error.is_permanent()
            }
        }
    }
}

/// Consumer and terminal-fact constructor over Knowledge's owned durable state.
#[derive(Debug)]
pub struct RepositoryAnalysisConsumer<'a> {
    database: &'a Database,
}

impl<'a> RepositoryAnalysisConsumer<'a> {
    /// Creates a consumer backed by the supplied Knowledge database.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Persists one request exactly once by its contract idempotency digest.
    ///
    /// Inference is deliberately not started here: the separate worker applies Knowledge's ledger,
    /// model and blob-access policies after this durable intake has succeeded.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryAnalysisError`] when the immutable contract input cannot be persisted.
    pub async fn accept(
        &self,
        request: &RepositoryAnalysisRequested,
        cause: &EventCause,
    ) -> Result<RepositoryAnalysisAdmission, RepositoryAnalysisError> {
        let request_id = request
            .request_id
            .to_string()
            .parse::<uuid::Uuid>()
            .map_err(|_| PersistenceError::InvalidAnalysisIdentity)?;
        let repository_id = request
            .repository_id
            .to_string()
            .parse::<uuid::Uuid>()
            .map_err(|_| PersistenceError::InvalidAnalysisIdentity)?;
        let github_repository_numeric_id = i64::try_from(request.github_repository_numeric_id)
            .map_err(|_| PersistenceError::ValueOverflow)?;
        let owner = request.owner.to_string();
        let source_revision = serde_json::to_value(&request.source_revision)
            .map_err(RepositoryAnalysisError::Encode)?;
        let repository_attributes = serde_json::to_value(&request.repository_attributes)
            .map_err(RepositoryAnalysisError::Encode)?;
        let inserted = sqlx::query_scalar::<_, uuid::Uuid>(
            "insert into knowledge.repository_analysis_requests (
                request_id, tenant_ref, repository_id, github_repository_numeric_id,
                source_revision, repository_attributes, requested_contract, idempotency_digest_hex,
                cause_event_id, cause_correlation_id
             ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             on conflict (idempotency_digest_hex) do nothing
             returning request_id",
        )
        .bind(request_id)
        .bind(&owner)
        .bind(repository_id)
        .bind(github_repository_numeric_id)
        .bind(source_revision.clone())
        .bind(repository_attributes.clone())
        .bind("repository_analysis")
        .bind(request.idempotency_key.hex.as_str())
        .bind(cause.event_id)
        .bind(cause.correlation_id.to_wire())
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if inserted.is_some() {
            return Ok(RepositoryAnalysisAdmission::Accepted);
        }

        let existing = sqlx::query_as::<
            _,
            (
                uuid::Uuid,
                String,
                uuid::Uuid,
                i64,
                serde_json::Value,
                serde_json::Value,
                String,
            ),
        >(
            "select request_id, tenant_ref, repository_id, github_repository_numeric_id,
                    source_revision, repository_attributes, requested_contract
             from knowledge.repository_analysis_requests
             where idempotency_digest_hex = $1",
        )
        .bind(request.idempotency_key.hex.as_str())
        .fetch_one(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let is_same_request = existing
            == (
                request_id,
                owner,
                repository_id,
                github_repository_numeric_id,
                source_revision,
                repository_attributes,
                "repository_analysis".to_owned(),
            );
        if is_same_request {
            Ok(RepositoryAnalysisAdmission::Duplicate)
        } else {
            Err(RepositoryAnalysisError::IdempotencyConflict)
        }
    }

    /// Marks the exact pending request complete, commits the terminal fact to the outbox in the
    /// same transaction, and returns the fact.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryAnalysisError`] when owned state cannot be updated.
    pub async fn complete(
        &self,
        request: &RepositoryAnalysisRequested,
        analysis_result_ref: EntityRef,
    ) -> Result<Option<RepositoryAnalysisCompleted>, RepositoryAnalysisError> {
        let fact = RepositoryAnalysisCompleted {
            owner: request.owner,
            repository_id: request.repository_id,
            github_repository_numeric_id: request.github_repository_numeric_id,
            request_id: request.request_id,
            source_revision: request.source_revision.clone(),
            analysis_result_ref: analysis_result_ref.clone(),
            completed_at: ratatoskr_identifiers::WireTimestamp::now(),
            extensions: Extensions::new(),
        };
        let payload = serde_json::to_value(&fact).map_err(RepositoryAnalysisError::Encode)?;
        let settled = self
            .settle(
                request,
                Terminal {
                    state: "completed",
                    analysis_result_ref: Some(&analysis_result_ref),
                    failure_code: None,
                    retryable: None,
                    subject: OutboxSubject::RepositoryAnalysisCompleted,
                    dedupe_suffix: "completed",
                    payload,
                },
            )
            .await?;
        Ok(settled.then_some(fact))
    }

    /// Marks the exact pending request failed, commits the terminal fact to the outbox in the
    /// same transaction, and returns the fact.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryAnalysisError`] when owned state cannot be updated.
    pub async fn fail(
        &self,
        request: &RepositoryAnalysisRequested,
        failure_code: AnalysisFailureCode,
        retryable: bool,
    ) -> Result<Option<RepositoryAnalysisFailed>, RepositoryAnalysisError> {
        let fact = RepositoryAnalysisFailed {
            owner: request.owner,
            repository_id: request.repository_id,
            github_repository_numeric_id: request.github_repository_numeric_id,
            request_id: request.request_id,
            source_revision: request.source_revision.clone(),
            failure_code,
            retryable,
            failed_at: ratatoskr_identifiers::WireTimestamp::now(),
            extensions: Extensions::new(),
        };
        let payload = serde_json::to_value(&fact).map_err(RepositoryAnalysisError::Encode)?;
        let settled = self
            .settle(
                request,
                Terminal {
                    state: "failed",
                    analysis_result_ref: None,
                    failure_code: Some(failure_code),
                    retryable: Some(retryable),
                    subject: OutboxSubject::RepositoryAnalysisFailed,
                    dedupe_suffix: "failed",
                    payload,
                },
            )
            .await?;
        Ok(settled.then_some(fact))
    }

    /// Settles the pending request and queues its fact in one transaction.
    async fn settle(
        &self,
        request: &RepositoryAnalysisRequested,
        terminal: Terminal<'_>,
    ) -> Result<bool, RepositoryAnalysisError> {
        let request_id = request
            .request_id
            .to_string()
            .parse::<uuid::Uuid>()
            .map_err(|_| PersistenceError::InvalidAnalysisIdentity)?;
        let source_revision = serde_json::to_value(&request.source_revision)
            .map_err(RepositoryAnalysisError::Encode)?;
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let settled: Option<(uuid::Uuid, String)> = sqlx::query_as(
            "update knowledge.repository_analysis_requests
             set state = $2, analysis_result_ref = $3, failure_code = $4, retryable = $5,
                 terminal_at = now()
             where request_id = $1 and tenant_ref = $6 and repository_id = $7
               and github_repository_numeric_id = $8 and source_revision = $9 and state = 'pending'
             returning cause_event_id, cause_correlation_id",
        )
        .bind(request_id)
        .bind(terminal.state)
        .bind(terminal.analysis_result_ref.map(EntityRef::to_wire))
        .bind(terminal.failure_code.map(|code| match code {
            AnalysisFailureCode::SourceUnavailable => "source_unavailable",
            AnalysisFailureCode::ContractInvalid => "contract_invalid",
            AnalysisFailureCode::DependencyUnavailable => "dependency_unavailable",
            AnalysisFailureCode::NotAuthorized => "not_authorized",
            _ => "unknown",
        }))
        .bind(terminal.retryable)
        .bind(request.owner.to_string())
        .bind(
            request
                .repository_id
                .to_string()
                .parse::<uuid::Uuid>()
                .map_err(|_| PersistenceError::InvalidAnalysisIdentity)?,
        )
        .bind(
            i64::try_from(request.github_repository_numeric_id)
                .map_err(|_| PersistenceError::ValueOverflow)?,
        )
        .bind(source_revision)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let Some((cause_event_id, cause_correlation_id)) = settled else {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(false);
        };
        let cause = EventCause {
            event_id: cause_event_id,
            correlation_id: EntityRef::parse(&cause_correlation_id)
                .map_err(|_| EventOutboxError::InvalidRow)?,
        };
        EventOutbox::enqueue_in(
            &mut transaction,
            &OutboxFact {
                subject: terminal.subject,
                tenant: request.owner,
                aggregate_id: EntityRef::parse(&format!("repository:{}", request.repository_id))
                    .map_err(|_| EventOutboxError::InvalidRow)?,
                correlation_id: cause.correlation_id.clone(),
                causation_id: Some(cause.causation_ref()?),
                payload: terminal.payload,
                dedupe_key: format!("repo:{}:{}", request.request_id, terminal.dedupe_suffix),
            },
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(true)
    }
}

/// The terminal state of a request together with the fact that announces it.
struct Terminal<'a> {
    state: &'static str,
    analysis_result_ref: Option<&'a EntityRef>,
    failure_code: Option<AnalysisFailureCode>,
    retryable: Option<bool>,
    subject: OutboxSubject,
    dedupe_suffix: &'static str,
    payload: serde_json::Value,
}
