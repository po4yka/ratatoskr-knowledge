//! Durable inbox, run state, and terminal outbox for channel recap analysis.

use ratatoskr_channel_digest_contracts::{
    ChannelDigestRecapFailureCode, KnowledgeChannelDigestRecapCompleted,
    KnowledgeChannelDigestRecapFailed, KnowledgeChannelDigestRecapRequested,
};
use ratatoskr_event_envelope::CommandEnvelope;
use ratatoskr_identifiers::WireTimestamp;
use sha2::{Digest as _, Sha256};
use std::time::Duration;

use crate::{
    ChannelDigestRecap, Database, PersistenceError, VerifiedDigestManifest,
    channel_digest_recap_schema,
};

/// Completed, integrity-checked channel recap projection owned by Knowledge.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChannelRecapResultProjection {
    /// Opaque Knowledge analysis identifier used by internal consumers.
    pub analysis_id: uuid::Uuid,
    /// Exact SHA-256 digest of the canonical stored recap JSON.
    pub result_digest_hex: String,
    /// Closed typed recap result; provider attempts and source content are excluded.
    pub recap: ChannelDigestRecap,
}

/// Safe result-reader failure without owner data or recap content.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChannelRecapResultReadError {
    /// The identifier does not name a completed channel recap result.
    #[error("the channel recap result was not found")]
    NotFound,
    /// Durable recap JSON or its digest failed closed integrity validation.
    #[error("the channel recap result failed integrity validation")]
    Integrity,
    /// Knowledge-owned durable state could not be read.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

/// Safe, content-free recap command admission failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ChannelRecapAdmissionError {
    /// The command subject is not the recap subject owned by Knowledge.
    #[error("the command subject is unsupported")]
    UnsupportedSubject,
    /// The command payload does not satisfy the published typed contract.
    #[error("the recap command payload is invalid")]
    InvalidPayload,
    /// The envelope tenant does not match the typed payload owner.
    #[error("the recap command owner is invalid")]
    OwnerMismatch,
}

/// Outcome of transactionally claiming one at-least-once recap request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelRecapInboxAdmission {
    /// A new semantic request was inserted as one durable work item.
    Accepted,
    /// The transport or semantic request identity was already present.
    Duplicate,
}

/// Durable recap execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelRecapRunState {
    /// Typed command was transactionally admitted.
    Received,
    /// Manifest retrieval is waiting for a bounded retry.
    ManifestRetry,
    /// Manifest bytes and immutable references were verified.
    ManifestVerified,
    /// Deterministic provider context is durable.
    ContextPrepared,
    /// A bounded provider request was issued.
    ModelRequested,
    /// Raw bounded provider output is durable.
    ResponseReceived,
    /// Structured output passed schema and grounding validation.
    SchemaValidated,
    /// One bounded repair attempt was recorded.
    Repaired,
    /// Typed result and digest are durable.
    Persisted,
    /// Completion fact is committed in the outbox.
    Completed,
    /// Safe terminal failure fact is committed in the outbox.
    Failed,
}

impl ChannelRecapRunState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::ManifestRetry => "manifest_retry",
            Self::ManifestVerified => "manifest_verified",
            Self::ContextPrepared => "context_prepared",
            Self::ModelRequested => "model_requested",
            Self::ResponseReceived => "response_received",
            Self::SchemaValidated => "schema_validated",
            Self::Repaired => "repaired",
            Self::Persisted => "persisted",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

/// Safe recap state-transition failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChannelRecapRunError {
    /// The requested state edge is not part of the durable lifecycle.
    #[error("the recap state transition is invalid")]
    InvalidTransition,
    /// The expected state did not match current durable state.
    #[error("the recap state changed concurrently")]
    StateConflict,
    /// The terminal typed fact contradicted its run or durable result linkage.
    #[error("the recap terminal fact is inconsistent")]
    InconsistentFact,
    /// A terminal fact could not be encoded for the outbox.
    #[error("the recap terminal fact could not be encoded")]
    Encode(#[source] serde_json::Error),
    /// Knowledge-owned durable state could not be written.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

/// Expected-state transition and terminal-outbox repository for recap runs.
#[derive(Debug)]
pub struct ChannelRecapRunStore<'a> {
    database: &'a Database,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ManifestAttemptStatus {
    pub(super) state: ChannelRecapRunState,
    pub(super) attempt_count: i16,
    pub(super) retry_ready: bool,
}

struct TerminalSettlement {
    terminal: ChannelRecapRunState,
    subject: &'static str,
    payload: serde_json::Value,
    failure_code: Option<String>,
    result_id: Option<uuid::Uuid>,
}

impl<'a> ChannelRecapRunStore<'a> {
    /// Creates a run repository over the process-owned database pool.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Reads one completed typed recap by its opaque analysis identifier.
    ///
    /// # Errors
    ///
    /// Returns a scoped absence for every identifier outside the completed recap family.
    pub async fn read_completed_result(
        &self,
        analysis_id: uuid::Uuid,
    ) -> Result<ChannelRecapResultProjection, ChannelRecapResultReadError> {
        let stored: Option<(serde_json::Value, String)> = sqlx::query_as(
            "select result.result, result.result_digest_hex
             from knowledge.channel_recap_results result
             inner join knowledge.channel_recap_runs run
                on run.recap_run_id = result.recap_run_id
             where result.result_id = $1 and run.state = 'completed'",
        )
        .bind(analysis_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let Some((value, result_digest_hex)) = stored else {
            return Err(ChannelRecapResultReadError::NotFound);
        };
        let canonical =
            serde_json::to_vec(&value).map_err(|_| ChannelRecapResultReadError::Integrity)?;
        let observed_digest_hex = format!("{:x}", Sha256::digest(canonical));
        if observed_digest_hex != result_digest_hex {
            return Err(ChannelRecapResultReadError::Integrity);
        }
        let schema =
            channel_digest_recap_schema().map_err(|_| ChannelRecapResultReadError::Integrity)?;
        let validator = jsonschema::options()
            .should_validate_formats(true)
            .build(&schema)
            .map_err(|_| ChannelRecapResultReadError::Integrity)?;
        validator
            .validate(&value)
            .map_err(|_| ChannelRecapResultReadError::Integrity)?;
        let recap =
            serde_json::from_value(value).map_err(|_| ChannelRecapResultReadError::Integrity)?;
        Ok(ChannelRecapResultProjection {
            analysis_id,
            result_digest_hex,
            recap,
        })
    }

    /// Applies one legal non-terminal expected-state transition.
    ///
    /// # Errors
    ///
    /// Returns [`ChannelRecapRunError::InvalidTransition`] for an illegal edge and
    /// [`ChannelRecapRunError::StateConflict`] when durable state no longer equals `expected`.
    pub async fn transition(
        &self,
        recap_run_id: uuid::Uuid,
        expected: ChannelRecapRunState,
        next: ChannelRecapRunState,
    ) -> Result<(), ChannelRecapRunError> {
        if !allowed_transition(expected, next) || next.is_terminal() {
            return Err(ChannelRecapRunError::InvalidTransition);
        }
        let result = sqlx::query(
            "update knowledge.channel_recap_runs
             set state = $3,
                 manifest_retry_not_before = case when $3 = 'manifest_retry' then now() else null end,
                 updated_at = now()
             where recap_run_id = $1 and state = $2
               and state not in ('completed', 'failed')",
        )
        .bind(recap_run_id)
        .bind(expected.as_str())
        .bind(next.as_str())
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if result.rows_affected() != 1 {
            return Err(ChannelRecapRunError::StateConflict);
        }
        Ok(())
    }

    pub(super) async fn manifest_attempt_status(
        &self,
        recap_run_id: uuid::Uuid,
    ) -> Result<ManifestAttemptStatus, ChannelRecapRunError> {
        let row: Option<(String, i16, bool)> = sqlx::query_as(
            "select state, manifest_attempt_count,
                    coalesce(manifest_retry_not_before <= now(), true)
             from knowledge.channel_recap_runs where recap_run_id = $1",
        )
        .bind(recap_run_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let Some((state, attempt_count, retry_ready)) = row else {
            return Err(ChannelRecapRunError::StateConflict);
        };
        Ok(ManifestAttemptStatus {
            state: parse_run_state(&state)?,
            attempt_count,
            retry_ready,
        })
    }

    pub(super) async fn schedule_manifest_retry(
        &self,
        recap_run_id: uuid::Uuid,
        expected: ChannelRecapRunState,
        attempt_count: i16,
        delay: Duration,
    ) -> Result<(), ChannelRecapRunError> {
        let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
        let updated = sqlx::query(
            "update knowledge.channel_recap_runs
             set state = 'manifest_retry',
                 manifest_attempt_count = manifest_attempt_count + 1,
                 manifest_retry_not_before = now() + ($4 * interval '1 millisecond'),
                 updated_at = now()
             where recap_run_id = $1 and state = $2 and manifest_attempt_count = $3
               and manifest_attempt_count < 1",
        )
        .bind(recap_run_id)
        .bind(expected.as_str())
        .bind(attempt_count)
        .bind(delay_ms)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if updated.rows_affected() != 1 {
            return Err(ChannelRecapRunError::StateConflict);
        }
        Ok(())
    }

    pub(super) async fn settle_manifest_failure(
        &self,
        recap_run_id: uuid::Uuid,
        expected: ChannelRecapRunState,
        request: &KnowledgeChannelDigestRecapRequested,
        failure_code: ChannelDigestRecapFailureCode,
    ) -> Result<(), ChannelRecapRunError> {
        let fact: KnowledgeChannelDigestRecapFailed = serde_json::from_value(serde_json::json!({
            "owner": request.owner,
            "operation_id": request.operation_id,
            "digest_run_id": request.digest_run_id,
            "manifest_digest": request.manifest_digest,
            "failure_code": failure_code,
            "failed_at": WireTimestamp::now(),
        }))
        .map_err(ChannelRecapRunError::Encode)?;
        self.settle_failed(recap_run_id, expected, &fact).await
    }

    /// Atomically persists one verified immutable manifest and advances its run.
    ///
    /// # Errors
    ///
    /// Returns a safe consistency, expected-state, or persistence failure.
    pub async fn accept_verified_manifest(
        &self,
        recap_run_id: uuid::Uuid,
        expected: ChannelRecapRunState,
        verified: &VerifiedDigestManifest,
    ) -> Result<(), ChannelRecapRunError> {
        if !matches!(
            expected,
            ChannelRecapRunState::Received | ChannelRecapRunState::ManifestRetry
        ) {
            return Err(ChannelRecapRunError::InvalidTransition);
        }
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let linkage: Option<(String, uuid::Uuid, String)> = sqlx::query_as(
            "select owner_ref, digest_run_id, manifest_digest_hex
             from knowledge.channel_recap_runs where recap_run_id = $1 and state = $2",
        )
        .bind(recap_run_id)
        .bind(expected.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let Some((owner_ref, digest_run_id, manifest_digest_hex)) = linkage else {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapRunError::StateConflict);
        };
        if verified.manifest.owner != owner_ref
            || verified.manifest.digest_run_id != digest_run_id
            || verified.digest_hex != manifest_digest_hex
        {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapRunError::InconsistentFact);
        }
        let channels = verified
            .manifest
            .sources
            .iter()
            .map(|source| source.channel_ref.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let manifest =
            serde_json::to_value(&verified.manifest).map_err(ChannelRecapRunError::Encode)?;
        let inserted = sqlx::query(
            "insert into knowledge.channel_recap_manifests
                (recap_run_id, owner_ref, digest_run_id, manifest_ref, manifest_digest_hex,
                 window_start_at, window_end_at, source_count, channel_count, manifest)
             values ($1, $2, $3, $4, $5, $6::timestamptz, $7::timestamptz, $8, $9, $10)
             on conflict do nothing",
        )
        .bind(recap_run_id)
        .bind(owner_ref)
        .bind(digest_run_id)
        .bind(verified.manifest.manifest_ref.as_str())
        .bind(verified.digest_hex.as_str())
        .bind(verified.manifest.window.start_at.to_wire())
        .bind(verified.manifest.window.end_at.to_wire())
        .bind(i32::try_from(verified.manifest.sources.len()).unwrap_or(i32::MAX))
        .bind(i32::try_from(channels).unwrap_or(i32::MAX))
        .bind(manifest)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if inserted.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapRunError::StateConflict);
        }
        let updated = sqlx::query(
            "update knowledge.channel_recap_runs
             set state = 'manifest_verified', manifest_retry_not_before = null, updated_at = now()
             where recap_run_id = $1 and state = $2",
        )
        .bind(recap_run_id)
        .bind(expected.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if updated.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapRunError::StateConflict);
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }

    /// Atomically commits a typed completion fact with the terminal state.
    ///
    /// # Errors
    ///
    /// Returns a safe error for inconsistent linkage, encoding, persistence, or expected-state
    /// conflict.
    pub async fn settle_completed(
        &self,
        recap_run_id: uuid::Uuid,
        expected: ChannelRecapRunState,
        fact: &KnowledgeChannelDigestRecapCompleted,
    ) -> Result<(), ChannelRecapRunError> {
        fact.validate_for_publish()
            .map_err(|_| ChannelRecapRunError::InconsistentFact)?;
        self.settle(
            recap_run_id,
            expected,
            TerminalSettlement {
                terminal: ChannelRecapRunState::Completed,
                subject: "knowledge.channel_digest_recap.completed.v1",
                payload: serde_json::to_value(fact).map_err(ChannelRecapRunError::Encode)?,
                failure_code: None,
                result_id: Some(fact.digest_result_id.as_uuid()),
            },
        )
        .await
    }

    /// Atomically commits a typed safe failure fact with the terminal state.
    ///
    /// # Errors
    ///
    /// Returns a safe error for inconsistent linkage, encoding, persistence, or expected-state
    /// conflict.
    pub async fn settle_failed(
        &self,
        recap_run_id: uuid::Uuid,
        expected: ChannelRecapRunState,
        fact: &KnowledgeChannelDigestRecapFailed,
    ) -> Result<(), ChannelRecapRunError> {
        fact.validate_for_publish()
            .map_err(|_| ChannelRecapRunError::InconsistentFact)?;
        let failure_code = serde_json::to_value(fact.failure_code)
            .map_err(ChannelRecapRunError::Encode)?
            .as_str()
            .ok_or(ChannelRecapRunError::InconsistentFact)?
            .to_owned();
        self.settle(
            recap_run_id,
            expected,
            TerminalSettlement {
                terminal: ChannelRecapRunState::Failed,
                subject: "knowledge.channel_digest_recap.failed.v1",
                payload: serde_json::to_value(fact).map_err(ChannelRecapRunError::Encode)?,
                failure_code: Some(failure_code),
                result_id: None,
            },
        )
        .await
    }

    async fn settle(
        &self,
        recap_run_id: uuid::Uuid,
        expected: ChannelRecapRunState,
        settlement: TerminalSettlement,
    ) -> Result<(), ChannelRecapRunError> {
        if expected.is_terminal() || !settlement.terminal.is_terminal() {
            return Err(ChannelRecapRunError::InvalidTransition);
        }
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let linkage: Option<(String, uuid::Uuid, String)> = sqlx::query_as(
            "select owner_ref, digest_run_id, manifest_digest_hex
             from knowledge.channel_recap_runs where recap_run_id = $1 and state = $2",
        )
        .bind(recap_run_id)
        .bind(expected.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let Some((owner_ref, digest_run_id, manifest_digest_hex)) = linkage else {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapRunError::StateConflict);
        };
        let digest_run_id_text = digest_run_id.to_string();
        if !settlement_matches_linkage(
            &settlement,
            &owner_ref,
            &digest_run_id_text,
            &manifest_digest_hex,
        ) {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapRunError::InconsistentFact);
        }
        if let Some(result_id) = settlement.result_id
            && !settlement_result_exists(&mut transaction, recap_run_id, result_id).await?
        {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapRunError::InconsistentFact);
        }
        let updated = sqlx::query(
            "update knowledge.channel_recap_runs
             set state = $3,
                 failure_code = $4,
                 manifest_attempt_count = case
                     when $4 in ('manifest_unavailable', 'manifest_integrity')
                     then least(manifest_attempt_count + 1, 2)
                     else manifest_attempt_count
                 end,
                 manifest_retry_not_before = null,
                 updated_at = now()
             where recap_run_id = $1 and state = $2
               and state not in ('completed', 'failed')",
        )
        .bind(recap_run_id)
        .bind(expected.as_str())
        .bind(settlement.terminal.as_str())
        .bind(settlement.failure_code)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if updated.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapRunError::StateConflict);
        }
        sqlx::query(
            "insert into knowledge.channel_recap_outbox
                 (outbox_id, recap_run_id, subject, payload)
             values ($1, $2, $3, $4)",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(recap_run_id)
        .bind(settlement.subject)
        .bind(settlement.payload)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }
}

async fn settlement_result_exists(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    recap_run_id: uuid::Uuid,
    result_id: uuid::Uuid,
) -> Result<bool, ChannelRecapRunError> {
    sqlx::query_scalar(
        "select exists (
            select 1 from knowledge.channel_recap_results
            where recap_run_id = $1 and result_id = $2
        )",
    )
    .bind(recap_run_id)
    .bind(result_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)
    .map_err(ChannelRecapRunError::Persistence)
}

fn settlement_matches_linkage(
    settlement: &TerminalSettlement,
    owner_ref: &str,
    digest_run_id: &str,
    manifest_digest_hex: &str,
) -> bool {
    settlement
        .payload
        .get("owner")
        .and_then(serde_json::Value::as_str)
        == Some(owner_ref)
        && settlement
            .payload
            .get("digest_run_id")
            .and_then(serde_json::Value::as_str)
            == Some(digest_run_id)
        && settlement
            .payload
            .get("manifest_digest")
            .and_then(|digest| digest.get("hex"))
            .and_then(serde_json::Value::as_str)
            == Some(manifest_digest_hex)
}

fn parse_run_state(state: &str) -> Result<ChannelRecapRunState, ChannelRecapRunError> {
    match state {
        "received" => Ok(ChannelRecapRunState::Received),
        "manifest_retry" => Ok(ChannelRecapRunState::ManifestRetry),
        "manifest_verified" => Ok(ChannelRecapRunState::ManifestVerified),
        "context_prepared" => Ok(ChannelRecapRunState::ContextPrepared),
        "model_requested" => Ok(ChannelRecapRunState::ModelRequested),
        "response_received" => Ok(ChannelRecapRunState::ResponseReceived),
        "schema_validated" => Ok(ChannelRecapRunState::SchemaValidated),
        "repaired" => Ok(ChannelRecapRunState::Repaired),
        "persisted" => Ok(ChannelRecapRunState::Persisted),
        "completed" => Ok(ChannelRecapRunState::Completed),
        "failed" => Ok(ChannelRecapRunState::Failed),
        _ => Err(ChannelRecapRunError::InconsistentFact),
    }
}

const fn allowed_transition(expected: ChannelRecapRunState, next: ChannelRecapRunState) -> bool {
    matches!(
        (expected, next),
        (
            ChannelRecapRunState::Received,
            ChannelRecapRunState::ManifestRetry | ChannelRecapRunState::ManifestVerified
        ) | (
            ChannelRecapRunState::ManifestRetry,
            ChannelRecapRunState::ManifestVerified
        ) | (
            ChannelRecapRunState::ManifestVerified,
            ChannelRecapRunState::ContextPrepared
        ) | (
            ChannelRecapRunState::ContextPrepared | ChannelRecapRunState::Repaired,
            ChannelRecapRunState::ModelRequested
        ) | (
            ChannelRecapRunState::ModelRequested,
            ChannelRecapRunState::ResponseReceived
        ) | (
            ChannelRecapRunState::ResponseReceived,
            ChannelRecapRunState::SchemaValidated | ChannelRecapRunState::Repaired
        ) | (
            ChannelRecapRunState::SchemaValidated,
            ChannelRecapRunState::Persisted
        )
    )
}

/// Safe durable recap-inbox failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChannelRecapInboxError {
    /// Typed command admission failed.
    #[error(transparent)]
    Admission(#[from] ChannelRecapAdmissionError),
    /// The typed request could not be encoded for durable storage.
    #[error("the recap command could not be encoded")]
    Encode(#[source] serde_json::Error),
    /// The transport command id was already bound to another semantic request.
    #[error("the recap command identity conflicts with a previous request")]
    IdentityConflict,
    /// Knowledge-owned durable state could not be written.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

/// Transactional consumer for typed channel-digest recap commands.
#[derive(Debug)]
pub struct ChannelRecapInbox<'a> {
    database: &'a Database,
}

impl<'a> ChannelRecapInbox<'a> {
    /// Creates a recap inbox over the process-owned database pool.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Claims one transport delivery and converges semantic redelivery into one work item.
    ///
    /// # Errors
    ///
    /// Returns [`ChannelRecapInboxError`] for invalid commands, identity collisions, encoding, or
    /// transactional persistence failures.
    pub async fn accept(
        &self,
        envelope: &CommandEnvelope,
    ) -> Result<ChannelRecapInboxAdmission, ChannelRecapInboxError> {
        let request = admit_channel_digest_recap(envelope)?;
        let output_language = recap_output_language(request.output_language);
        let request_payload =
            serde_json::to_value(&request).map_err(ChannelRecapInboxError::Encode)?;
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let inserted: Option<uuid::Uuid> = sqlx::query_scalar(
            "insert into knowledge.channel_recap_inbox
                 (command_id, owner_ref, operation_id, digest_run_id, manifest_ref,
                  manifest_digest_hex, window_start_at, window_end_at, source_count,
                  channel_count, analysis_family, analysis_contract, output_language,
                  request_payload)
             values ($1, $2, $3, $4, $5, $6, $7::timestamptz, $8::timestamptz,
                     $9, $10, 'channel_digest_recap', 'channel_digest_recap.v1', $11, $12)
             on conflict do nothing
             returning command_id",
        )
        .bind(envelope.command_id.0)
        .bind(request.owner.to_string())
        .bind(request.operation_id.0)
        .bind(request.digest_run_id.as_uuid())
        .bind(request.manifest_ref.to_string())
        .bind(request.manifest_digest.hex.as_str())
        .bind(request.window.start_at.to_string())
        .bind(request.window.end_at.to_string())
        .bind(i32::from(request.source_count.get()))
        .bind(i32::from(request.channel_count.get()))
        .bind(output_language)
        .bind(request_payload)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if inserted.is_some() {
            sqlx::query(
                "insert into knowledge.channel_recap_runs
                     (recap_run_id, inbox_command_id, owner_ref, digest_run_id,
                      manifest_digest_hex, analysis_family, analysis_contract,
                      prompt_version, context_version, output_language)
                 values ($1, $2, $3, $4, $5, 'channel_digest_recap',
                         'channel_digest_recap.v1', 'channel_recap_prompt_v1',
                         'channel_recap_context_v1', $6)",
            )
            .bind(uuid::Uuid::now_v7())
            .bind(envelope.command_id.0)
            .bind(request.owner.to_string())
            .bind(request.digest_run_id.as_uuid())
            .bind(request.manifest_digest.hex.as_str())
            .bind(output_language)
            .execute(&mut *transaction)
            .await
            .map_err(PersistenceError::Query)?;
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(ChannelRecapInboxAdmission::Accepted);
        }

        let exact_identity: bool = sqlx::query_scalar(
            "select exists (
                select 1 from knowledge.channel_recap_inbox
                where (command_id = $1 or (
                    owner_ref = $2 and digest_run_id = $3 and manifest_digest_hex = $4
                    and analysis_contract = 'channel_digest_recap.v1'
                    and output_language = $5
                ))
                and owner_ref = $2 and digest_run_id = $3 and manifest_digest_hex = $4
                and analysis_contract = 'channel_digest_recap.v1' and output_language = $5
            )",
        )
        .bind(envelope.command_id.0)
        .bind(request.owner.to_string())
        .bind(request.digest_run_id.as_uuid())
        .bind(request.manifest_digest.hex.as_str())
        .bind(output_language)
        .fetch_one(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if !exact_identity {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapInboxError::IdentityConflict);
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(ChannelRecapInboxAdmission::Duplicate)
    }
}

const fn recap_output_language(
    language: ratatoskr_channel_digest_contracts::OutputLanguage,
) -> &'static str {
    match language {
        ratatoskr_channel_digest_contracts::OutputLanguage::Ru => "ru",
        ratatoskr_channel_digest_contracts::OutputLanguage::En => "en",
    }
}

/// Admits exactly one owner-scoped typed channel-digest recap command.
///
/// # Errors
///
/// Returns a closed [`ChannelRecapAdmissionError`] without copying any envelope or payload value
/// into the diagnostic.
pub fn admit_channel_digest_recap(
    envelope: &CommandEnvelope,
) -> Result<KnowledgeChannelDigestRecapRequested, ChannelRecapAdmissionError> {
    let request = envelope
        .payload_as::<KnowledgeChannelDigestRecapRequested>()
        .map_err(|error| match error {
            ratatoskr_event_envelope::CommandError::PayloadType { .. } => {
                ChannelRecapAdmissionError::UnsupportedSubject
            }
            _ => ChannelRecapAdmissionError::InvalidPayload,
        })?;
    if envelope.tenant_id.as_ref() != Some(&request.owner) {
        return Err(ChannelRecapAdmissionError::OwnerMismatch);
    }
    Ok(request)
}
