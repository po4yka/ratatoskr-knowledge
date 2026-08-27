//! Durable intake and terminal-fact construction for repository-analysis requests.

use ratatoskr_github_contracts::{
    AnalysisFailureCode, RepositoryAnalysisCompleted, RepositoryAnalysisFailed,
    RepositoryAnalysisRequested,
};
use ratatoskr_identifiers::{EntityRef, Extensions};

use crate::{Database, PersistenceError};

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
                source_revision, repository_attributes, requested_contract, idempotency_digest_hex
             ) values ($1, $2, $3, $4, $5, $6, $7, $8)
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

    /// Marks the exact pending request complete and returns the terminal fact to publish once.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryAnalysisError`] when owned state cannot be updated.
    pub async fn complete(
        &self,
        request: &RepositoryAnalysisRequested,
        analysis_result_ref: EntityRef,
    ) -> Result<Option<RepositoryAnalysisCompleted>, RepositoryAnalysisError> {
        let changed = self
            .terminal_update(request, "completed", Some(&analysis_result_ref), None, None)
            .await?;
        Ok(changed.then(|| RepositoryAnalysisCompleted {
            owner: request.owner,
            repository_id: request.repository_id,
            github_repository_numeric_id: request.github_repository_numeric_id,
            request_id: request.request_id,
            source_revision: request.source_revision.clone(),
            analysis_result_ref,
            completed_at: ratatoskr_identifiers::WireTimestamp::now(),
            extensions: Extensions::new(),
        }))
    }

    /// Marks the exact pending request failed and returns the terminal fact to publish once.
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
        let changed = self
            .terminal_update(request, "failed", None, Some(failure_code), Some(retryable))
            .await?;
        Ok(changed.then(|| RepositoryAnalysisFailed {
            owner: request.owner,
            repository_id: request.repository_id,
            github_repository_numeric_id: request.github_repository_numeric_id,
            request_id: request.request_id,
            source_revision: request.source_revision.clone(),
            failure_code,
            retryable,
            failed_at: ratatoskr_identifiers::WireTimestamp::now(),
            extensions: Extensions::new(),
        }))
    }

    async fn terminal_update(
        &self,
        request: &RepositoryAnalysisRequested,
        state: &str,
        analysis_result_ref: Option<&EntityRef>,
        failure_code: Option<AnalysisFailureCode>,
        retryable: Option<bool>,
    ) -> Result<bool, RepositoryAnalysisError> {
        let source_revision = serde_json::to_value(&request.source_revision)
            .map_err(RepositoryAnalysisError::Encode)?;
        let result = sqlx::query(
            "update knowledge.repository_analysis_requests
             set state = $2, analysis_result_ref = $3, failure_code = $4, retryable = $5,
                 terminal_at = now()
             where request_id = $1 and tenant_ref = $6 and repository_id = $7
               and github_repository_numeric_id = $8 and source_revision = $9 and state = 'pending'",
        )
        .bind(request.request_id.to_string().parse::<uuid::Uuid>().map_err(|_| PersistenceError::InvalidAnalysisIdentity)?)
        .bind(state)
        .bind(analysis_result_ref.map(EntityRef::to_wire))
        .bind(failure_code.map(|code| match code {
            AnalysisFailureCode::SourceUnavailable => "source_unavailable",
            AnalysisFailureCode::ContractInvalid => "contract_invalid",
            AnalysisFailureCode::DependencyUnavailable => "dependency_unavailable",
            AnalysisFailureCode::NotAuthorized => "not_authorized",
            _ => "unknown",
        }))
        .bind(retryable)
        .bind(request.owner.to_string())
        .bind(request.repository_id.to_string().parse::<uuid::Uuid>().map_err(|_| PersistenceError::InvalidAnalysisIdentity)?)
        .bind(i64::try_from(request.github_repository_numeric_id).map_err(|_| PersistenceError::ValueOverflow)?)
        .bind(source_revision)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        Ok(result.rows_affected() == 1)
    }
}
