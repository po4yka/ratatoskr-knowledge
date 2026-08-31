use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DocumentId, TenantRef,
};
use uuid::Uuid;

use crate::{Database, PersistenceError, ProviderRetrySafety};

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
    /// The provider may have accepted a billable request, but no durable response exists.
    ProviderOutcomeUnknown,
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
    /// One explicit operator-authorized replay after provider reconciliation.
    OperatorReplay,
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

/// Durable facts needed to resume one analysis state without repeating an external effect.
#[derive(Debug, Clone)]
pub(crate) struct RunRecovery {
    pub state: RunState,
    pub output: Option<serde_json::Value>,
    pub attempt: Option<RecoveredAttempt>,
    pub replay_authorized: bool,
}

/// Latest durable provider-attempt facts used by both article and family pipelines.
#[derive(Debug, Clone)]
pub(crate) struct RecoveredAttempt {
    pub ordinal: i16,
    pub outcome: AttemptOutcome,
    pub raw_response: Option<BlobRef>,
    pub request_id: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub validation_code: Option<String>,
    pub duration_ms: i32,
}

/// One shared next action for the article and family state machines.
#[derive(Debug, Clone)]
pub(crate) enum RunResumeAction {
    Call {
        first_call: u8,
        call_limit: u8,
        reason: AttemptReason,
        repair_code: Option<String>,
    },
    StoredResponse {
        state: RunState,
        attempt: RecoveredAttempt,
    },
    Output(serde_json::Value),
    ProviderOutcomeUnknown,
    Failed,
}

impl RunState {
    /// Returns the stable database spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::ContextPrepared => "context_prepared",
            Self::ModelRequested => "model_requested",
            Self::ProviderOutcomeUnknown => "provider_outcome_unknown",
            Self::ResponseReceived => "response_received",
            Self::SchemaValidated => "schema_validated",
            Self::Repaired => "repaired",
            Self::Persisted => "persisted",
            Self::Indexed => "indexed",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    fn from_str(value: &str) -> Result<Self, PersistenceError> {
        match value {
            "queued" => Ok(Self::Queued),
            "context_prepared" => Ok(Self::ContextPrepared),
            "model_requested" => Ok(Self::ModelRequested),
            "provider_outcome_unknown" => Ok(Self::ProviderOutcomeUnknown),
            "response_received" => Ok(Self::ResponseReceived),
            "schema_validated" => Ok(Self::SchemaValidated),
            "repaired" => Ok(Self::Repaired),
            "persisted" => Ok(Self::Persisted),
            "indexed" => Ok(Self::Indexed),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(PersistenceError::InvalidAnalysisIdentity),
        }
    }
}

impl AttemptReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Retry => "retry",
            Self::Repair => "repair",
            Self::OperatorReplay => "operator_replay",
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

    fn from_str(value: &str) -> Result<Self, PersistenceError> {
        match value {
            "requested" => Ok(Self::Requested),
            "transient_failure" => Ok(Self::TransientFailure),
            "permanent_failure" => Ok(Self::PermanentFailure),
            "response_received" => Ok(Self::ResponseReceived),
            "invalid" => Ok(Self::Invalid),
            "accepted" => Ok(Self::Accepted),
            _ => Err(PersistenceError::InvalidAnalysisIdentity),
        }
    }
}

impl Database {
    /// Advances deterministic pre-call states and returns one shared recovery action.
    #[allow(
        clippy::too_many_lines,
        reason = "one shared recovery table exhaustively dispatches every persisted analysis state"
    )]
    pub(crate) async fn prepare_run_resume(
        &self,
        run_id: Uuid,
        retry_safety: ProviderRetrySafety,
    ) -> Result<RunResumeAction, PersistenceError> {
        let recovery = self.recover_run(run_id).await?;
        match recovery.state {
            RunState::Persisted | RunState::Indexed | RunState::Completed => recovery
                .output
                .map(RunResumeAction::Output)
                .ok_or(PersistenceError::InvalidAnalysisIdentity),
            RunState::ProviderOutcomeUnknown => Ok(RunResumeAction::ProviderOutcomeUnknown),
            RunState::Failed => Ok(RunResumeAction::Failed),
            RunState::Queued => {
                self.require_run_transition(run_id, RunState::Queued, RunState::ContextPrepared)
                    .await?;
                self.require_run_transition(
                    run_id,
                    RunState::ContextPrepared,
                    RunState::ModelRequested,
                )
                .await?;
                Ok(RunResumeAction::Call {
                    first_call: 0,
                    call_limit: 2,
                    reason: AttemptReason::Initial,
                    repair_code: None,
                })
            }
            RunState::ContextPrepared => {
                self.require_run_transition(
                    run_id,
                    RunState::ContextPrepared,
                    RunState::ModelRequested,
                )
                .await?;
                Ok(RunResumeAction::Call {
                    first_call: 0,
                    call_limit: 2,
                    reason: AttemptReason::Initial,
                    repair_code: None,
                })
            }
            RunState::ResponseReceived | RunState::SchemaValidated => {
                Ok(RunResumeAction::StoredResponse {
                    state: recovery.state,
                    attempt: recovery
                        .attempt
                        .ok_or(PersistenceError::InvalidAnalysisIdentity)?,
                })
            }
            RunState::Repaired => {
                let attempt = recovery
                    .attempt
                    .ok_or(PersistenceError::InvalidAnalysisIdentity)?;
                self.require_run_transition(run_id, RunState::Repaired, RunState::ModelRequested)
                    .await?;
                Ok(RunResumeAction::Call {
                    first_call: u8::try_from(attempt.ordinal).unwrap_or(2),
                    call_limit: 2,
                    reason: AttemptReason::Repair,
                    repair_code: Some(
                        attempt
                            .validation_code
                            .unwrap_or_else(|| "schema".to_owned()),
                    ),
                })
            }
            RunState::ModelRequested => {
                let Some(attempt) = recovery.attempt else {
                    return Ok(RunResumeAction::Call {
                        first_call: 0,
                        call_limit: 2,
                        reason: AttemptReason::Initial,
                        repair_code: None,
                    });
                };
                if attempt.raw_response.is_some() {
                    return Ok(RunResumeAction::StoredResponse {
                        state: RunState::ModelRequested,
                        attempt,
                    });
                }
                if recovery.replay_authorized {
                    let changed = sqlx::query(
                        "update knowledge.analysis_runs
                         set provider_replay_authorized = false, updated_at = now()
                         where run_id = $1 and state = 'model_requested'
                           and provider_replay_authorized",
                    )
                    .bind(run_id)
                    .execute(self.pool())
                    .await
                    .map_err(PersistenceError::Query)?;
                    if changed.rows_affected() != 1 {
                        return Err(PersistenceError::InvalidAnalysisIdentity);
                    }
                    return Ok(RunResumeAction::Call {
                        first_call: u8::try_from(attempt.ordinal).unwrap_or(3),
                        call_limit: u8::try_from(attempt.ordinal.saturating_add(1)).unwrap_or(3),
                        reason: AttemptReason::OperatorReplay,
                        repair_code: None,
                    });
                }
                match attempt.outcome {
                    AttemptOutcome::Requested | AttemptOutcome::TransientFailure
                        if retry_safety == ProviderRetrySafety::Idempotent
                            && attempt.ordinal < 2 =>
                    {
                        Ok(RunResumeAction::Call {
                            first_call: u8::try_from(attempt.ordinal).unwrap_or(2),
                            call_limit: 2,
                            reason: AttemptReason::Retry,
                            repair_code: None,
                        })
                    }
                    AttemptOutcome::Invalid if attempt.ordinal < 2 => Ok(RunResumeAction::Call {
                        first_call: u8::try_from(attempt.ordinal).unwrap_or(2),
                        call_limit: 2,
                        reason: AttemptReason::Repair,
                        repair_code: Some(
                            attempt
                                .validation_code
                                .unwrap_or_else(|| "schema".to_owned()),
                        ),
                    }),
                    AttemptOutcome::Requested | AttemptOutcome::TransientFailure => {
                        self.require_run_transition(
                            run_id,
                            RunState::ModelRequested,
                            RunState::ProviderOutcomeUnknown,
                        )
                        .await?;
                        Ok(RunResumeAction::ProviderOutcomeUnknown)
                    }
                    _ => {
                        self.require_run_transition(
                            run_id,
                            RunState::ModelRequested,
                            RunState::Failed,
                        )
                        .await?;
                        Ok(RunResumeAction::Failed)
                    }
                }
            }
        }
    }

    async fn require_run_transition(
        &self,
        run_id: Uuid,
        expected: RunState,
        next: RunState,
    ) -> Result<(), PersistenceError> {
        if self.transition_run(run_id, expected, next).await? {
            Ok(())
        } else {
            Err(PersistenceError::InvalidAnalysisIdentity)
        }
    }

    /// Loads the complete bounded resume record shared by every analysis family.
    pub(crate) async fn recover_run(&self, run_id: Uuid) -> Result<RunRecovery, PersistenceError> {
        type RecoveryRow = (
            String,
            Option<serde_json::Value>,
            Option<i16>,
            Option<String>,
            Option<serde_json::Value>,
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<i32>,
            bool,
        );
        let row: RecoveryRow = sqlx::query_as(
            "select r.state, o.result, a.ordinal, a.outcome, a.raw_response,
                    a.provider_request_id, a.input_tokens, a.output_tokens,
                    a.validation_code, a.duration_ms, r.provider_replay_authorized
             from knowledge.analysis_runs r
             left join knowledge.analysis_outputs o
               on o.run_id = r.run_id and o.accepted
             left join lateral (
                 select ordinal, outcome, raw_response, provider_request_id,
                        input_tokens, output_tokens, validation_code, duration_ms
                 from knowledge.analysis_attempts
                 where run_id = r.run_id order by ordinal desc limit 1
             ) a on true
             where r.run_id = $1",
        )
        .bind(run_id)
        .fetch_optional(self.pool())
        .await
        .map_err(PersistenceError::Query)?
        .ok_or(PersistenceError::InvalidAnalysisIdentity)?;
        let attempt = match (row.2, row.3) {
            (Some(ordinal), Some(outcome)) => Some(RecoveredAttempt {
                ordinal,
                outcome: AttemptOutcome::from_str(&outcome)?,
                raw_response: row
                    .4
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(PersistenceError::Encode)?,
                request_id: row.5,
                input_tokens: u64::try_from(row.6.unwrap_or_default())
                    .map_err(|_| PersistenceError::ValueOverflow)?,
                output_tokens: u64::try_from(row.7.unwrap_or_default())
                    .map_err(|_| PersistenceError::ValueOverflow)?,
                validation_code: row.8,
                duration_ms: row.9.unwrap_or_default(),
            }),
            (None, None) => None,
            _ => return Err(PersistenceError::InvalidAnalysisIdentity),
        };
        Ok(RunRecovery {
            state: RunState::from_str(&row.0)?,
            output: row.1,
            attempt,
            replay_authorized: row.10,
        })
    }

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
        let replay_key = sqlx::query_scalar::<_, Option<String>>(
            "select provider_replay_key from knowledge.analysis_runs where run_id = $1 for update",
        )
        .bind(run_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let Some(replay_key) = replay_key else {
            return Err(PersistenceError::InvalidAnalysisIdentity);
        };
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
        let attempt_limit = if replay_key.is_some() { 3 } else { 2 };
        if ordinal > attempt_limit {
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
            RunState::ProviderOutcomeUnknown | RunState::ResponseReceived | RunState::Failed
        ) | (
            RunState::ResponseReceived,
            RunState::SchemaValidated | RunState::Repaired | RunState::Failed
        ) | (
            RunState::SchemaValidated,
            RunState::Persisted | RunState::Failed
        ) | (RunState::Persisted, RunState::Indexed | RunState::Completed)
    )
}
