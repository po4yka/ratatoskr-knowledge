use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DocumentId, TenantRef,
};
use uuid::Uuid;

use crate::{Database, PersistenceError};

pub(crate) struct AttemptUpdate<'a> {
    pub raw_response: &'a BlobRef,
    pub request_id: Option<&'a str>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub outcome: AttemptOutcome,
    pub validation_code: Option<&'a str>,
    pub duration_ms: i32,
}

/// Immutable source evidence presented to Knowledge.
#[derive(Debug, Clone)]
pub struct SourceReference {
    /// Authorization owner of the source.
    pub tenant: TenantRef,
    /// Bounded context that owns the source document.
    pub owner_context: String,
    /// Archive snapshot identity when this source was derived from an AI archive event.
    pub ai_archive_id: String,
    /// Stable normalized document identity.
    pub document_id: DocumentId,
    /// Digest of the exact Document IR revision.
    pub content_digest: ContentDigest,
    /// Provenance bytes owned by the source service.
    pub source_blob: BlobRef,
}

/// Stored immutable source revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRevision {
    /// Knowledge-owned revision identity.
    pub id: Uuid,
}

/// Complete immutable identity of one analysis run.
#[derive(Debug, Clone)]
pub struct AnalysisIdentity {
    /// Stored source revision.
    pub source_revision_id: Uuid,
    /// Typed result contract version.
    pub contract_version: String,
    /// Fixed prompt version.
    pub prompt_version: String,
    /// Deterministic context-builder version.
    pub context_builder_version: String,
    /// Provider-neutral model policy identity.
    pub model_policy: String,
}

/// Stored analysis run identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalysisRun {
    /// Knowledge-owned run identity.
    pub id: Uuid,
}

/// Persisted monotonic analysis state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    /// The complete identity was accepted.
    Queued,
    /// Deterministic source context was prepared.
    ContextPrepared,
    /// A provider request was started.
    ModelRequested,
    /// Raw response bytes were stored.
    ResponseReceived,
    /// Structural and semantic validation passed.
    SchemaValidated,
    /// One repair call was authorized.
    Repaired,
    /// Accepted output was committed.
    Persisted,
    /// Embeddings were persisted for the accepted output.
    Indexed,
    /// The run reached successful terminal state.
    Completed,
    /// The run reached failed terminal state.
    Failed,
}

/// Why one provider attempt exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptReason {
    /// First provider request.
    Initial,
    /// One retry after a transient failure.
    Retry,
    /// One repair after invalid output.
    Repair,
}

/// Safe durable outcome of one provider attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The request started and has no response yet.
    Requested,
    /// A retryable provider failure occurred.
    TransientFailure,
    /// A permanent provider failure occurred.
    PermanentFailure,
    /// Raw response bytes were stored.
    ResponseReceived,
    /// Stored response failed validation.
    Invalid,
    /// Stored response was accepted.
    Accepted,
}

/// Safe metadata persisted for one provider call.
#[derive(Debug, Clone)]
pub struct AttemptInput {
    /// Why this call was made.
    pub reason: AttemptReason,
    /// Provider adapter identity.
    pub provider: String,
    /// Concrete upstream model id.
    pub model: String,
    /// Provider-neutral model policy identity.
    pub model_policy: String,
    /// Provider request identity when received.
    pub provider_request_id: Option<String>,
    /// Safe attempt outcome.
    pub outcome: AttemptOutcome,
}

/// Stored provider-attempt identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attempt {
    /// One-based ordinal unique within the run.
    pub ordinal: i16,
}

impl RunState {
    /// Returns the stable database spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::ContextPrepared => "context_prepared",
            Self::ModelRequested => "model_requested",
            Self::ResponseReceived => "response_received",
            Self::SchemaValidated => "schema_validated",
            Self::Repaired => "repaired",
            Self::Persisted => "persisted",
            Self::Indexed => "indexed",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl AttemptReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Retry => "retry",
            Self::Repair => "repair",
        }
    }
}

impl AttemptOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::TransientFailure => "transient_failure",
            Self::PermanentFailure => "permanent_failure",
            Self::ResponseReceived => "response_received",
            Self::Invalid => "invalid",
            Self::Accepted => "accepted",
        }
    }
}

impl Database {
    pub(crate) async fn update_attempt_failure(
        &self,
        run_id: Uuid,
        ordinal: i16,
        outcome: AttemptOutcome,
        error_class: Option<&'static str>,
        http_status: Option<i16>,
        duration_ms: i32,
    ) -> Result<(), PersistenceError> {
        if !matches!(
            outcome,
            AttemptOutcome::TransientFailure | AttemptOutcome::PermanentFailure
        ) {
            return Err(PersistenceError::InvalidAnalysisIdentity);
        }
        let result = sqlx::query(
            "update knowledge.analysis_attempts
             set outcome = $3, error_class = $4, http_status = $5, duration_ms = $6
             where run_id = $1 and ordinal = $2",
        )
        .bind(run_id)
        .bind(ordinal)
        .bind(outcome.as_str())
        .bind(error_class)
        .bind(http_status)
        .bind(duration_ms)
        .execute(self.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(PersistenceError::InvalidAnalysisIdentity)
        }
    }

    pub(crate) async fn update_attempt(
        &self,
        run_id: Uuid,
        ordinal: i16,
        update: &AttemptUpdate<'_>,
    ) -> Result<(), PersistenceError> {
        let raw_response =
            serde_json::to_value(update.raw_response).map_err(PersistenceError::Encode)?;
        let input_tokens =
            i64::try_from(update.input_tokens).map_err(|_| PersistenceError::ValueOverflow)?;
        let output_tokens =
            i64::try_from(update.output_tokens).map_err(|_| PersistenceError::ValueOverflow)?;
        let result = sqlx::query(
            "update knowledge.analysis_attempts
             set provider_request_id = $3, raw_response = $4,
                 input_tokens = $5, output_tokens = $6,
                 outcome = $7, validation_code = $8, duration_ms = $9
             where run_id = $1 and ordinal = $2",
        )
        .bind(run_id)
        .bind(ordinal)
        .bind(update.request_id)
        .bind(raw_response)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(update.outcome.as_str())
        .bind(update.validation_code)
        .bind(update.duration_ms)
        .execute(self.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(PersistenceError::InvalidAnalysisIdentity)
        }
    }

    /// Persists the next attempt ordinal and its safe metadata.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when validation or persistence fails.
    pub async fn record_attempt(
        &self,
        run_id: Uuid,
        input: &AttemptInput,
    ) -> Result<Attempt, PersistenceError> {
        validate_version(&input.provider)?;
        validate_model(&input.model)?;
        validate_version(&input.model_policy)?;
        if input
            .provider_request_id
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 128)
        {
            return Err(PersistenceError::InvalidAnalysisIdentity);
        }

        let mut transaction = self.pool().begin().await.map_err(PersistenceError::Query)?;
        let present = sqlx::query_scalar::<_, Uuid>(
            "select run_id from knowledge.analysis_runs where run_id = $1 for update",
        )
        .bind(run_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if present.is_none() {
            return Err(PersistenceError::InvalidAnalysisIdentity);
        }
        let previous: i16 = sqlx::query_scalar(
            "select coalesce(max(ordinal), 0)::smallint
             from knowledge.analysis_attempts where run_id = $1",
        )
        .bind(run_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let ordinal = previous
            .checked_add(1)
            .ok_or(PersistenceError::AttemptBudgetExhausted)?;
        if ordinal > 2 {
            return Err(PersistenceError::AttemptBudgetExhausted);
        }
        sqlx::query(
            "insert into knowledge.analysis_attempts (
                run_id, ordinal, reason, provider, model_policy, model,
                provider_request_id, outcome
             ) values ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(run_id)
        .bind(ordinal)
        .bind(input.reason.as_str())
        .bind(&input.provider)
        .bind(&input.model_policy)
        .bind(&input.model)
        .bind(&input.provider_request_id)
        .bind(input.outcome.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(Attempt { ordinal })
    }

    /// Applies one expected-state transition.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when the update fails.
    pub async fn transition_run(
        &self,
        run_id: Uuid,
        expected: RunState,
        next: RunState,
    ) -> Result<bool, PersistenceError> {
        if !legal_transition(expected, next) {
            return Ok(false);
        }
        let result = sqlx::query(
            "update knowledge.analysis_runs
             set state = $3, updated_at = now()
             where run_id = $1 and state = $2",
        )
        .bind(run_id)
        .bind(expected.as_str())
        .bind(next.as_str())
        .execute(self.pool())
        .await
        .map_err(PersistenceError::Query)?;
        Ok(result.rows_affected() == 1)
    }

    /// Returns the existing run for a complete identity or creates it once.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when identity validation or persistence fails.
    pub async fn create_run(
        &self,
        identity: &AnalysisIdentity,
    ) -> Result<AnalysisRun, PersistenceError> {
        for value in [
            &identity.contract_version,
            &identity.prompt_version,
            &identity.context_builder_version,
            &identity.model_policy,
        ] {
            validate_version(value)?;
        }
        let id = Uuid::now_v7();
        let stored_id = sqlx::query_scalar(
            "insert into knowledge.analysis_runs (
                run_id, source_ref_id, contract_version, prompt_version,
                context_builder_version, model_policy
             ) values ($1, $2, $3, $4, $5, $6)
             on conflict (
                source_ref_id, contract_version, prompt_version,
                context_builder_version, model_policy
             ) do update set run_id = knowledge.analysis_runs.run_id
             returning run_id",
        )
        .bind(id)
        .bind(identity.source_revision_id)
        .bind(&identity.contract_version)
        .bind(&identity.prompt_version)
        .bind(&identity.context_builder_version)
        .bind(&identity.model_policy)
        .fetch_one(self.pool())
        .await
        .map_err(PersistenceError::Query)?;
        Ok(AnalysisRun { id: stored_id })
    }

    /// Registers an immutable source revision.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when persistence fails.
    pub async fn register_source(
        &self,
        source: &SourceReference,
    ) -> Result<SourceRevision, PersistenceError> {
        let owner =
            BlobOwner::parse(&source.owner_context).map_err(|_| PersistenceError::InvalidSource)?;
        if owner.as_str() != source.source_blob.owner_service.as_str() {
            return Err(PersistenceError::InvalidSource);
        }
        let algorithm = match source.content_digest.algorithm {
            DigestAlgorithm::Sha256 => "sha256",
            _ => return Err(PersistenceError::InvalidSource),
        };
        let blob = serde_json::to_value(&source.source_blob).map_err(PersistenceError::Encode)?;
        let id = Uuid::now_v7();
        let stored_id = sqlx::query_scalar(
            "insert into knowledge.source_refs (
                source_ref_id, tenant_ref, owner_context, ai_archive_id, source_document_id,
                content_digest_algorithm, content_digest_hex, source_blob
             ) values ($1, $2, $3, $4, $5, $6, $7, $8)
             on conflict (
                tenant_ref, owner_context, ai_archive_id, source_document_id,
                content_digest_algorithm, content_digest_hex
             ) do update set source_ref_id = knowledge.source_refs.source_ref_id
             returning source_ref_id",
        )
        .bind(id)
        .bind(source.tenant.to_string())
        .bind(owner.as_str())
        .bind(&source.ai_archive_id)
        .bind(source.document_id.to_string())
        .bind(algorithm)
        .bind(source.content_digest.hex.as_str())
        .bind(blob)
        .fetch_one(self.pool())
        .await
        .map_err(PersistenceError::Query)?;
        Ok(SourceRevision { id: stored_id })
    }
}

fn validate_version(value: &str) -> Result<(), PersistenceError> {
    let mut characters = value.chars();
    let starts_correctly = characters
        .next()
        .is_some_and(|character| character.is_ascii_lowercase());
    let rest_is_valid = characters.all(|character| {
        character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '_' | '-')
    });
    if starts_correctly && rest_is_valid && value.len() <= 64 {
        Ok(())
    } else {
        Err(PersistenceError::InvalidAnalysisIdentity)
    }
}

fn validate_model(value: &str) -> Result<(), PersistenceError> {
    let printable = value.len() <= 128 && value.bytes().all(|byte| (33..=126).contains(&byte));
    if printable && !value.is_empty() {
        Ok(())
    } else {
        Err(PersistenceError::InvalidAnalysisIdentity)
    }
}

const fn legal_transition(from: RunState, to: RunState) -> bool {
    matches!(
        (from, to),
        (
            RunState::Queued,
            RunState::ContextPrepared | RunState::Failed
        ) | (
            RunState::ContextPrepared | RunState::Repaired,
            RunState::ModelRequested | RunState::Failed
        ) | (
            RunState::ModelRequested,
            RunState::ResponseReceived | RunState::Failed
        ) | (
            RunState::ResponseReceived,
            RunState::SchemaValidated | RunState::Repaired | RunState::Failed
        ) | (
            RunState::SchemaValidated,
            RunState::Persisted | RunState::Failed
        ) | (RunState::Persisted, RunState::Indexed | RunState::Completed)
    )
}
