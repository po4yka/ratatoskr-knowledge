use std::time::Duration;

use ratatoskr_event_envelope::EventEnvelope;
use uuid::Uuid;

use crate::{Database, PersistenceError};

/// One pending canonical terminal envelope.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    /// Stable outbox row identity.
    pub outbox_id: Uuid,
    /// Logical work whose terminal transition created this row.
    pub work_id: Uuid,
    /// Exact allowed NATS subject.
    pub subject: String,
    /// Canonical terminal envelope.
    pub envelope: serde_json::Value,
    /// Stable broker deduplication identity.
    pub message_id: Uuid,
}

/// Safe terminal-outbox persistence failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TerminalOutboxError {
    /// Knowledge-owned state could not be read or written.
    #[error("terminal publication state could not be persisted")]
    Persistence(#[from] PersistenceError),
    /// The terminal transition was stale or duplicated with another fact.
    #[error("terminal transition was refused")]
    Transition,
}

/// General transactional outbox for Knowledge terminal facts.
#[derive(Debug)]
pub struct TerminalOutbox<'a> {
    database: &'a Database,
}

impl<'a> TerminalOutbox<'a> {
    /// Creates an outbox over Knowledge-owned storage.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Commits a terminal state for a family with no declared terminal bus contract.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalOutboxError::Transition`] if work is no longer owned or terminal.
    pub async fn finish_without_fact(
        &self,
        work_id: Uuid,
        worker: &str,
        success: bool,
        failure_code: Option<&str>,
    ) -> Result<(), TerminalOutboxError> {
        let state = if success { "completed" } else { "failed" };
        let code = if success {
            None
        } else {
            failure_code.or(Some("analysis_failed"))
        };
        let changed = sqlx::query(
            "update knowledge.analysis_work set state = $3, terminal_code = $4,
                 lease_owner = null, lease_expires_at = null, updated_at = now()
             where work_id = $1 and lease_owner = $2
               and state not in ('completed', 'failed', 'suppressed')",
        )
        .bind(work_id)
        .bind(worker)
        .bind(state)
        .bind(code)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(TerminalOutboxError::Transition)
        }
    }

    /// Commits one terminal work state and publication intent in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalOutboxError::Transition`] if work was already terminal or is not owned
    /// by `worker`.
    #[allow(
        clippy::too_many_lines,
        reason = "one transaction visibly owns terminal validation, state, and publication intent"
    )]
    pub async fn settle(
        &self,
        work_id: Uuid,
        worker: &str,
        success: bool,
        event_type: &str,
        subject: &str,
        envelope: &serde_json::Value,
    ) -> Result<Uuid, TerminalOutboxError> {
        validate_terminal_fact(event_type, subject, envelope)?;
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let input: Option<(Uuid, String, serde_json::Value)> = sqlx::query_as(
            "select event_id, family, input_envelope from knowledge.analysis_work
             where work_id = $1 and lease_owner = $2
               and state not in ('completed', 'failed', 'suppressed')
             for update",
        )
        .bind(work_id)
        .bind(worker)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let (input_event_id, family, input_envelope) =
            input.ok_or(TerminalOutboxError::Transition)?;
        let expected_causation = format!("event:{input_event_id}");
        if envelope
            .get("causation_id")
            .and_then(serde_json::Value::as_str)
            != Some(expected_causation.as_str())
        {
            return Err(TerminalOutboxError::Transition);
        }
        let terminal_type = envelope
            .get("event_type")
            .and_then(serde_json::Value::as_str)
            .ok_or(TerminalOutboxError::Transition)?;
        let family_matches = match family.as_str() {
            "social" => terminal_type == "knowledge.analysis.completed.v1",
            "ai_archive" => terminal_type == "knowledge.ai_archive_analysis.completed.v1",
            "repository" => matches!(
                terminal_type,
                "knowledge.repository_analysis.completed.v1"
                    | "knowledge.repository_analysis.failed.v1"
            ),
            _ => false,
        };
        if !family_matches
            || envelope.get("tenant_id") != input_envelope.get("tenant_id")
            || envelope.get("aggregate_id") != input_envelope.get("aggregate_id")
            || envelope.get("correlation_id") != input_envelope.get("correlation_id")
        {
            return Err(TerminalOutboxError::Transition);
        }
        if family == "repository"
            && envelope.pointer("/payload/request_id")
                != input_envelope.pointer("/payload/request_id")
        {
            return Err(TerminalOutboxError::Transition);
        }
        if event_type == "knowledge.repository_analysis.completed.v1"
            || event_type == "knowledge.repository_analysis.failed.v1"
        {
            settle_repository_request(&mut transaction, event_type, envelope).await?;
        }
        let state = if success { "completed" } else { "failed" };
        let terminal_code = (!success).then_some("analysis_failed");
        let changed = sqlx::query(
            "update knowledge.analysis_work set state = $3, terminal_code = $4,
                 lease_owner = null, lease_expires_at = null, updated_at = now()
             where work_id = $1 and lease_owner = $2
               and state not in ('completed', 'failed', 'suppressed')",
        )
        .bind(work_id)
        .bind(worker)
        .bind(state)
        .bind(terminal_code)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(TerminalOutboxError::Transition);
        }
        let outbox_id = Uuid::now_v7();
        let message_id = envelope
            .get("event_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or(TerminalOutboxError::Transition)?;
        sqlx::query(
            "insert into knowledge.knowledge_outbox (
                 outbox_id, work_id, event_type, subject, envelope, message_id
             ) values ($1, $2, $3, $4, $5, $6)",
        )
        .bind(outbox_id)
        .bind(work_id)
        .bind(event_type)
        .bind(subject)
        .bind(envelope)
        .bind(message_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(outbox_id)
    }

    /// Returns the oldest eligible unsent row, including rows present before process startup.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalOutboxError`] for storage failure.
    pub async fn next_pending(&self) -> Result<Option<OutboxEntry>, TerminalOutboxError> {
        let row: Option<(Uuid, Uuid, String, serde_json::Value, Uuid)> = sqlx::query_as(
            "select outbox_id, work_id, subject, envelope, message_id
             from knowledge.knowledge_outbox
             where published_at is null and next_attempt_at <= now()
             order by next_attempt_at, created_at limit 1",
        )
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        Ok(row.map(|row| OutboxEntry {
            outbox_id: row.0,
            work_id: row.1,
            subject: row.2,
            envelope: row.3,
            message_id: row.4,
        }))
    }

    /// Marks a row sent only after its broker publish acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalOutboxError::Transition`] if another publisher already settled it.
    pub async fn mark_published(&self, outbox_id: Uuid) -> Result<(), TerminalOutboxError> {
        let changed = sqlx::query(
            "update knowledge.knowledge_outbox set published_at = now()
             where outbox_id = $1 and published_at is null",
        )
        .bind(outbox_id)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(TerminalOutboxError::Transition)
        }
    }

    /// Retains publication intent and schedules a bounded reconnect retry.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalOutboxError`] for storage failure.
    pub async fn retry_after(
        &self,
        outbox_id: Uuid,
        delay: Duration,
    ) -> Result<(), TerminalOutboxError> {
        let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
        sqlx::query(
            "update knowledge.knowledge_outbox set publish_attempts = publish_attempts + 1,
                 next_attempt_at = now() + ($2 * interval '1 millisecond')
             where outbox_id = $1 and published_at is null",
        )
        .bind(outbox_id)
        .bind(delay_ms)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        Ok(())
    }
}

fn validate_terminal_fact(
    event_type: &str,
    subject: &str,
    envelope: &serde_json::Value,
) -> Result<(), TerminalOutboxError> {
    let bytes = serde_json::to_vec(envelope).map_err(PersistenceError::Encode)?;
    let parsed = EventEnvelope::from_json(&bytes).map_err(|_| TerminalOutboxError::Transition)?;
    let expected_subject = match event_type {
        "knowledge.analysis.completed.v1" => "evt.knowledge.analysis.completed.v1",
        "knowledge.ai_archive_analysis.completed.v1" => {
            "evt.knowledge.ai_archive_analysis.completed.v1"
        }
        "knowledge.repository_analysis.completed.v1" => {
            "evt.knowledge.repository_analysis.completed.v1"
        }
        "knowledge.repository_analysis.failed.v1" => "evt.knowledge.repository_analysis.failed.v1",
        _ => return Err(TerminalOutboxError::Transition),
    };
    if parsed.event_type.to_wire() != event_type
        || parsed.producer.as_str() != "ratatoskr-knowledge"
        || subject != expected_subject
        || envelope
            .get("event_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            != Some(parsed.event_id.0)
    {
        return Err(TerminalOutboxError::Transition);
    }
    Ok(())
}

async fn settle_repository_request(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event_type: &str,
    envelope: &serde_json::Value,
) -> Result<(), TerminalOutboxError> {
    let payload = envelope
        .get("payload")
        .ok_or(TerminalOutboxError::Transition)?;
    let request_id = payload
        .get("request_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(TerminalOutboxError::Transition)?;
    let (state, result_ref, failure_code, retryable) =
        if event_type == "knowledge.repository_analysis.completed.v1" {
            (
                "completed",
                payload
                    .get("analysis_result_ref")
                    .and_then(serde_json::Value::as_str),
                None,
                None,
            )
        } else {
            (
                "failed",
                None,
                payload
                    .get("failure_code")
                    .and_then(serde_json::Value::as_str),
                payload
                    .get("retryable")
                    .and_then(serde_json::Value::as_bool),
            )
        };
    let changed = sqlx::query(
        "update knowledge.repository_analysis_requests set state = $2,
             analysis_result_ref = $3, failure_code = $4, retryable = $5, terminal_at = now()
         where request_id = $1 and state = 'pending'",
    )
    .bind(request_id)
    .bind(state)
    .bind(result_ref)
    .bind(failure_code)
    .bind(retryable)
    .execute(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    if changed.rows_affected() == 1 {
        Ok(())
    } else {
        Err(TerminalOutboxError::Transition)
    }
}
