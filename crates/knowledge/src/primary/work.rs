use std::time::Duration;

use uuid::Uuid;

use crate::{Database, PersistenceError};

type ClaimedRow = (
    Uuid,
    Uuid,
    String,
    String,
    String,
    serde_json::Value,
    String,
    i32,
    i32,
);

/// Durable primary-work state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisWorkState {
    /// Transport admission committed; preparation has not started.
    Admitted,
    /// Deterministic context preparation is in progress or resumable.
    Preparing,
    /// A provider request is eligible and has not been observed as accepted.
    ProviderPending,
    /// The provider may have accepted a billable request; explicit requeue is required.
    ProviderOutcomeUnknown,
    /// A durable provider response exists and must be reused.
    ResponseReceived,
    /// Validated output persistence is in progress or resumable.
    Persisting,
    /// A bounded retry is waiting for its eligibility instant.
    RetryWait,
    /// Work completed durably.
    Completed,
    /// Work reached one final failure.
    Failed,
    /// A newer removal or tombstone suppresses this work.
    Suppressed,
}

impl AnalysisWorkState {
    fn from_database(value: &str) -> Result<Self, WorkQueueError> {
        match value {
            "admitted" => Ok(Self::Admitted),
            "preparing" => Ok(Self::Preparing),
            "provider_pending" => Ok(Self::ProviderPending),
            "provider_outcome_unknown" => Ok(Self::ProviderOutcomeUnknown),
            "response_received" => Ok(Self::ResponseReceived),
            "persisting" => Ok(Self::Persisting),
            "retry_wait" => Ok(Self::RetryWait),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "suppressed" => Ok(Self::Suppressed),
            _ => Err(WorkQueueError::InvalidState),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Preparing => "preparing",
            Self::ProviderPending => "provider_pending",
            Self::ProviderOutcomeUnknown => "provider_outcome_unknown",
            Self::ResponseReceived => "response_received",
            Self::Persisting => "persisting",
            Self::RetryWait => "retry_wait",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Suppressed => "suppressed",
        }
    }
}

/// One leased, immutable primary work item.
#[derive(Debug, Clone)]
pub struct AnalysisWork {
    /// Stable logical work identity.
    pub work_id: Uuid,
    /// Source event identity.
    pub event_id: Uuid,
    /// Closed analysis family name.
    pub family: String,
    /// Owner-scoped source identity.
    pub source_key: String,
    /// Immutable source revision digest/idempotency value.
    pub source_revision: String,
    /// Canonical event envelope retained for deterministic resume.
    pub input_envelope: serde_json::Value,
    /// Persisted resume state.
    pub state: AnalysisWorkState,
    /// Provider/dependency attempt count already consumed.
    pub attempt_count: i32,
    /// Maximum bounded attempts.
    pub max_attempts: i32,
}

/// Safe leased-work persistence failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkQueueError {
    /// Knowledge-owned state could not be read or written.
    #[error("analysis work could not be persisted")]
    Persistence(#[from] PersistenceError),
    /// Owned storage contained a state outside the first-version schema.
    #[error("analysis work contained an invalid state")]
    InvalidState,
    /// The requested state transition was stale or illegal.
    #[error("analysis work state transition was refused")]
    Transition,
}

/// Atomic leasing and recovery operations for primary analysis work.
#[derive(Debug)]
pub struct WorkQueue<'a> {
    database: &'a Database,
}

impl<'a> WorkQueue<'a> {
    /// Creates a queue over Knowledge-owned storage.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Claims the oldest eligible row, including an expired lease, with `SKIP LOCKED`.
    ///
    /// Uncertain provider outcomes and terminal rows are intentionally excluded.
    ///
    /// # Errors
    ///
    /// Returns [`WorkQueueError`] for a persistence or stored-state failure.
    pub async fn claim(
        &self,
        worker: &str,
        lease: Duration,
    ) -> Result<Option<AnalysisWork>, WorkQueueError> {
        let lease_ms = i64::try_from(lease.as_millis()).unwrap_or(i64::MAX);
        let row: Option<ClaimedRow> = sqlx::query_as(
            "with candidate as (
                     select work_id from knowledge.analysis_work
                     where state in (
                         'admitted', 'preparing', 'provider_pending', 'response_received',
                         'persisting', 'retry_wait'
                     ) and next_eligible_at <= now()
                       and (lease_expires_at is null or lease_expires_at <= now())
                     order by next_eligible_at, created_at
                     for update skip locked limit 1
                 )
                 update knowledge.analysis_work work set
                     lease_owner = $1,
                     lease_expires_at = now() + ($2 * interval '1 millisecond'),
                     updated_at = now()
                 from candidate where work.work_id = candidate.work_id
                 returning work.work_id, work.event_id, work.family, work.source_key,
                     work.source_revision, work.input_envelope, work.state,
                     work.attempt_count, work.max_attempts",
        )
        .bind(worker)
        .bind(lease_ms)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        row.map(|row| {
            Ok(AnalysisWork {
                work_id: row.0,
                event_id: row.1,
                family: row.2,
                source_key: row.3,
                source_revision: row.4,
                input_envelope: row.5,
                state: AnalysisWorkState::from_database(&row.6)?,
                attempt_count: row.7,
                max_attempts: row.8,
            })
        })
        .transpose()
    }

    /// Advances one leased row and releases the lease.
    ///
    /// # Errors
    ///
    /// Returns [`WorkQueueError::Transition`] if the lease owner or expected state no longer
    /// matches, preventing a stale worker from overwriting a reclaimed item.
    pub async fn transition(
        &self,
        work_id: Uuid,
        worker: &str,
        expected: AnalysisWorkState,
        next: AnalysisWorkState,
    ) -> Result<(), WorkQueueError> {
        let changed = sqlx::query(
            "update knowledge.analysis_work set state = $4, lease_owner = null,
                 lease_expires_at = null, updated_at = now()
             where work_id = $1 and lease_owner = $2 and state = $3",
        )
        .bind(work_id)
        .bind(worker)
        .bind(expected.as_str())
        .bind(next.as_str())
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(WorkQueueError::Transition)
        }
    }

    /// Releases a lease without changing the persisted resume state.
    ///
    /// # Errors
    ///
    /// Returns [`WorkQueueError::Transition`] if ownership has already changed.
    pub async fn release(&self, work_id: Uuid, worker: &str) -> Result<(), WorkQueueError> {
        let changed = sqlx::query(
            "update knowledge.analysis_work set lease_owner = null, lease_expires_at = null,
                 updated_at = now() where work_id = $1 and lease_owner = $2",
        )
        .bind(work_id)
        .bind(worker)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(WorkQueueError::Transition)
        }
    }

    /// Deletes derivatives created by a worker whose source revision was suppressed mid-call.
    ///
    /// The digest and tenant fence keep cleanup scoped to the superseded revision; a newer source
    /// revision and another family sharing the same UUID remain intact.
    ///
    /// # Errors
    ///
    /// Returns [`WorkQueueError`] when the suppressed work or its derived rows cannot be read.
    pub async fn discard_suppressed_derivatives(
        &self,
        work_id: Uuid,
    ) -> Result<(), WorkQueueError> {
        let row: Option<(String, String, String)> = sqlx::query_as(
            "select tenant_ref, source_key, source_revision
             from knowledge.analysis_work where work_id = $1 and state = 'suppressed'",
        )
        .bind(work_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let Some((tenant, source_key, revision)) = row else {
            return Ok(());
        };
        let source_id = source_key
            .split_once(':')
            .map_or(source_key.as_str(), |(_, id)| id);
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let source_ids: Vec<Uuid> = sqlx::query_scalar(
            "select source_ref_id from knowledge.source_refs
             where tenant_ref = $1 and source_document_id = $2
               and content_digest_hex = $3",
        )
        .bind(tenant)
        .bind(source_id)
        .bind(revision)
        .fetch_all(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if source_ids.is_empty() {
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(());
        }
        for table in [
            "embedding_failures",
            "embedding_chunks",
            "search_documents",
            "search_projection_inputs",
        ] {
            let query = format!("delete from knowledge.{table} where source_ref_id = any($1)");
            sqlx::query(&query)
                .bind(&source_ids)
                .execute(&mut *transaction)
                .await
                .map_err(PersistenceError::Query)?;
        }
        for table in ["analysis_outputs", "analysis_attempts"] {
            let query = format!(
                "delete from knowledge.{table} where run_id in (
                     select run_id from knowledge.analysis_runs where source_ref_id = any($1)
                 )"
            );
            sqlx::query(&query)
                .bind(&source_ids)
                .execute(&mut *transaction)
                .await
                .map_err(PersistenceError::Query)?;
        }
        sqlx::query("delete from knowledge.analysis_runs where source_ref_id = any($1)")
            .bind(&source_ids)
            .execute(&mut *transaction)
            .await
            .map_err(PersistenceError::Query)?;
        sqlx::query("delete from knowledge.source_refs where source_ref_id = any($1)")
            .bind(&source_ids)
            .execute(&mut *transaction)
            .await
            .map_err(PersistenceError::Query)?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }

    /// Records an uncertain external-provider outcome and releases the lease without retrying.
    ///
    /// # Errors
    ///
    /// Returns [`WorkQueueError`] when the caller no longer owns the pending request state.
    pub async fn mark_provider_unknown(
        &self,
        work_id: Uuid,
        worker: &str,
    ) -> Result<(), WorkQueueError> {
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let run_id: Option<Uuid> = sqlx::query_scalar(
            "select r.run_id
             from knowledge.analysis_work w
             join knowledge.source_refs s
               on s.tenant_ref = w.tenant_ref
              and s.source_document_id = case
                    when position(':' in w.source_key) > 0
                    then split_part(w.source_key, ':', 2)
                    else w.source_key
                  end
             join knowledge.analysis_runs r on r.source_ref_id = s.source_ref_id
             where w.work_id = $1 and w.lease_owner = $2
               and w.state = 'provider_pending'
               and r.state = 'provider_outcome_unknown'
               and r.contract_version = case
                    when w.family = 'document' then 'article_v1'
                    when w.family = 'social' then 'social_analysis_v1'
                    when w.family = 'repository' then 'repository_analysis_v1'
                    when w.source_key like 'conversation:%' then 'archive_analysis_v1'
                    when w.source_key like 'project:%' then 'archive_project_analysis_v1'
                    else ''
                  end
             order by r.updated_at desc limit 1
             for update of w, r",
        )
        .bind(work_id)
        .bind(worker)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let run_id = run_id.ok_or(WorkQueueError::Transition)?;
        let changed = sqlx::query(
            "update knowledge.analysis_work set state = 'provider_outcome_unknown',
                 analysis_run_id = $3, lease_owner = null, lease_expires_at = null,
                 updated_at = now()
             where work_id = $1 and lease_owner = $2 and state = 'provider_pending'",
        )
        .bind(work_id)
        .bind(worker)
        .bind(run_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() != 1 {
            return Err(WorkQueueError::Transition);
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }

    /// Explicitly authorizes replay of an uncertain provider call with a stable request key.
    ///
    /// This is deliberately never automatic. An operator or provider reconciliation workflow
    /// must prove that `request_key` identifies the same external effect before calling it.
    ///
    /// # Errors
    ///
    /// Returns [`WorkQueueError::Transition`] for an invalid key or a row not currently in the
    /// uncertain state.
    pub async fn requeue_provider_unknown(
        &self,
        work_id: Uuid,
        request_key: &str,
    ) -> Result<(), WorkQueueError> {
        if request_key.is_empty()
            || request_key.len() > 128
            || !request_key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':'))
        {
            return Err(WorkQueueError::Transition);
        }
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let run_id: Option<Uuid> = sqlx::query_scalar(
            "select analysis_run_id from knowledge.analysis_work
             where work_id = $1 and state = 'provider_outcome_unknown'
               and lease_owner is null and lease_expires_at is null
             for update",
        )
        .bind(work_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?
        .flatten();
        let run_id = run_id.ok_or(WorkQueueError::Transition)?;
        let run_changed = sqlx::query(
            "update knowledge.analysis_runs set state = 'model_requested',
                 provider_replay_key = $2, provider_replay_authorized = true,
                 updated_at = now()
             where run_id = $1 and state = 'provider_outcome_unknown'
               and (select coalesce(max(ordinal), 0) from knowledge.analysis_attempts
                    where run_id = $1) < 3",
        )
        .bind(run_id)
        .bind(request_key)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let work_changed = sqlx::query(
            "update knowledge.analysis_work set state = 'provider_pending',
                 provider_request_key = $2, next_eligible_at = now(), updated_at = now()
             where work_id = $1 and state = 'provider_outcome_unknown'",
        )
        .bind(work_id)
        .bind(request_key)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if run_changed.rows_affected() != 1 || work_changed.rows_affected() != 1 {
            return Err(WorkQueueError::Transition);
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }

    /// Schedules one bounded retry after releasing the current lease.
    ///
    /// # Errors
    ///
    /// Returns [`WorkQueueError::Transition`] when the attempt bound is exhausted or ownership
    /// changed.
    pub async fn retry_after(
        &self,
        work_id: Uuid,
        worker: &str,
        delay: Duration,
    ) -> Result<(), WorkQueueError> {
        let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
        let changed = sqlx::query(
            "update knowledge.analysis_work set state = 'retry_wait',
                 attempt_count = attempt_count + 1,
                 next_eligible_at = now() + ($3 * interval '1 millisecond'),
                 lease_owner = null, lease_expires_at = null, updated_at = now()
             where work_id = $1 and lease_owner = $2 and attempt_count < max_attempts",
        )
        .bind(work_id)
        .bind(worker)
        .bind(delay_ms)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(WorkQueueError::Transition)
        }
    }
}
