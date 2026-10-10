//! Transactional outbox of Knowledge's completion facts (XR-021 CONTRACTS.md S09).
//!
//! A fact is inserted in the same transaction as the state change that produced it, so a crash
//! after the change cannot lose the fact, and published afterwards by the ingest worker. The
//! unique dedupe key makes enqueueing a redelivery idempotent.

use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_identifiers::{EntityRef, TenantRef, WireTimestamp};
use sqlx::{Postgres, Row as _, Transaction};
use uuid::Uuid;

use crate::{Database, PersistenceError};

/// The inbound event a completion fact is caused by.
///
/// The fact carries the same correlation as the event that started the work, and names the event
/// as its cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCause {
    /// Identity of the inbound event.
    pub event_id: Uuid,
    /// Correlation reference of the inbound event, carried unchanged.
    pub correlation_id: EntityRef,
}

impl EventCause {
    /// Takes the cause from an inbound envelope.
    #[must_use]
    pub fn of(envelope: &EventEnvelope) -> Self {
        Self {
            event_id: envelope.event_id.0,
            correlation_id: envelope.correlation_id.clone(),
        }
    }

    /// The `event:<id>` reference a fact names as its causation.
    ///
    /// # Errors
    ///
    /// Returns [`EventOutboxError::InvalidRow`] if the reference cannot be formed.
    pub fn causation_ref(&self) -> Result<EntityRef, EventOutboxError> {
        EntityRef::parse(&format!("event:{}", self.event_id))
            .map_err(|_| EventOutboxError::InvalidRow)
    }
}

/// The closed set of completion facts Knowledge publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxSubject {
    /// `knowledge.analysis.completed.v1`: a social source was analysed.
    AnalysisCompleted,
    /// `knowledge.ai_archive_analysis.completed.v1`: an archive subject was analysed.
    AiArchiveAnalysisCompleted,
    /// `knowledge.repository_analysis.completed.v1`: a repository request completed.
    RepositoryAnalysisCompleted,
    /// `knowledge.repository_analysis.failed.v1`: a repository request failed.
    RepositoryAnalysisFailed,
}

impl OutboxSubject {
    /// The event type, which is also the stored subject.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AnalysisCompleted => "knowledge.analysis.completed.v1",
            Self::AiArchiveAnalysisCompleted => "knowledge.ai_archive_analysis.completed.v1",
            Self::RepositoryAnalysisCompleted => "knowledge.repository_analysis.completed.v1",
            Self::RepositoryAnalysisFailed => "knowledge.repository_analysis.failed.v1",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        [
            Self::AnalysisCompleted,
            Self::AiArchiveAnalysisCompleted,
            Self::RepositoryAnalysisCompleted,
            Self::RepositoryAnalysisFailed,
        ]
        .into_iter()
        .find(|subject| subject.as_str() == value)
    }
}

/// One fact to publish, as the producing transaction knows it.
#[derive(Debug, Clone)]
pub struct OutboxFact {
    /// Which fact this is.
    pub subject: OutboxSubject,
    /// The owner of the data the fact concerns.
    pub tenant: TenantRef,
    /// The entity the fact is about.
    pub aggregate_id: EntityRef,
    /// Correlation carried from the inbound event.
    pub correlation_id: EntityRef,
    /// The inbound event that caused the fact.
    pub causation_id: Option<EntityRef>,
    /// The typed payload as a JSON object.
    pub payload: serde_json::Value,
    /// Natural identity of the fact; a second fact with the same key is dropped.
    pub dedupe_key: String,
}

/// One committed, not yet published fact.
#[derive(Debug, Clone)]
pub struct OutboxRow {
    /// Identity of the fact, which is also its envelope's event id.
    pub outbox_id: Uuid,
    /// Which fact this is.
    pub subject: OutboxSubject,
    /// The owner of the data the fact concerns.
    pub tenant: TenantRef,
    /// The entity the fact is about.
    pub aggregate_id: EntityRef,
    /// Correlation carried from the inbound event.
    pub correlation_id: EntityRef,
    /// The inbound event that caused the fact.
    pub causation_id: Option<EntityRef>,
    /// The typed payload as a JSON object.
    pub payload: serde_json::Value,
    /// When the fact was committed.
    pub created_at: WireTimestamp,
}

/// Safe outbox failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EventOutboxError {
    /// Knowledge-owned storage could not be read or written.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    /// A stored row or a fact to store is not representable.
    #[error("an outbox row is invalid")]
    InvalidRow,
}

/// Outbox over Knowledge's owned storage.
#[derive(Debug)]
pub struct EventOutbox<'a> {
    database: &'a Database,
}

impl<'a> EventOutbox<'a> {
    /// Creates an outbox over the process-owned database.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Inserts `fact` inside the caller's transaction.
    ///
    /// Returns whether a row was inserted; a fact whose dedupe key is already present is dropped.
    ///
    /// # Errors
    ///
    /// Returns [`EventOutboxError`] when the fact is not an object or the insert fails.
    pub async fn enqueue_in(
        transaction: &mut Transaction<'_, Postgres>,
        fact: &OutboxFact,
    ) -> Result<bool, EventOutboxError> {
        if !fact.payload.is_object() {
            return Err(EventOutboxError::InvalidRow);
        }
        let inserted: Option<Uuid> = sqlx::query_scalar(
            "insert into knowledge.event_outbox
                 (outbox_id, subject, tenant_ref, aggregate_id, correlation_id, causation_id,
                  payload, dedupe_key)
             values ($1, $2, $3, $4, $5, $6, $7, $8)
             on conflict (dedupe_key) do nothing
             returning outbox_id",
        )
        .bind(Uuid::now_v7())
        .bind(fact.subject.as_str())
        .bind(fact.tenant.to_string())
        .bind(fact.aggregate_id.to_wire())
        .bind(fact.correlation_id.to_wire())
        .bind(fact.causation_id.as_ref().map(EntityRef::to_wire))
        .bind(&fact.payload)
        .bind(&fact.dedupe_key)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?;
        Ok(inserted.is_some())
    }

    /// Inserts `fact` in its own transaction.
    ///
    /// # Errors
    ///
    /// Returns [`EventOutboxError`] when the fact is not an object or the insert fails.
    pub async fn enqueue(&self, fact: &OutboxFact) -> Result<bool, EventOutboxError> {
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let inserted = Self::enqueue_in(&mut transaction, fact).await?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(inserted)
    }

    /// Reads up to `limit` unpublished facts, oldest first.
    ///
    /// Publishing is idempotent on the broker by message id, so concurrent publishers need no
    /// row claim. There is no per-row failure state: [`Self::enqueue_in`] is the only writer and
    /// the table constrains the rest, so every stored row decodes and a batch fails only when
    /// the database or the broker does, which holds back every row alike.
    ///
    /// # Errors
    ///
    /// Returns [`EventOutboxError`] when the rows cannot be read or decoded.
    pub async fn claim_unpublished(&self, limit: i64) -> Result<Vec<OutboxRow>, EventOutboxError> {
        let rows = sqlx::query(
            "select outbox_id, subject, tenant_ref, aggregate_id, correlation_id, causation_id,
                    payload, to_char(created_at at time zone 'UTC',
                                     'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') as created_text
             from knowledge.event_outbox
             where published_at is null
             order by created_at, outbox_id
             limit $1",
        )
        .bind(limit)
        .fetch_all(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        rows.iter().map(decode_row).collect()
    }

    /// Records that the broker acknowledged the fact.
    ///
    /// # Errors
    ///
    /// Returns [`EventOutboxError`] when the update fails.
    pub async fn mark_published(&self, outbox_id: Uuid) -> Result<(), EventOutboxError> {
        sqlx::query(
            "update knowledge.event_outbox set published_at = now()
             where outbox_id = $1 and published_at is null",
        )
        .bind(outbox_id)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        Ok(())
    }
}

fn decode_row(row: &sqlx::postgres::PgRow) -> Result<OutboxRow, EventOutboxError> {
    let get = |name: &str| -> Result<String, EventOutboxError> {
        row.try_get(name)
            .map_err(|error| EventOutboxError::Persistence(PersistenceError::Query(error)))
    };
    let causation: Option<String> = row
        .try_get("causation_id")
        .map_err(|error| EventOutboxError::Persistence(PersistenceError::Query(error)))?;
    Ok(OutboxRow {
        outbox_id: row
            .try_get("outbox_id")
            .map_err(|error| EventOutboxError::Persistence(PersistenceError::Query(error)))?,
        subject: OutboxSubject::parse(&get("subject")?).ok_or(EventOutboxError::InvalidRow)?,
        tenant: get("tenant_ref")?
            .parse()
            .map_err(|_| EventOutboxError::InvalidRow)?,
        aggregate_id: EntityRef::parse(&get("aggregate_id")?)
            .map_err(|_| EventOutboxError::InvalidRow)?,
        correlation_id: EntityRef::parse(&get("correlation_id")?)
            .map_err(|_| EventOutboxError::InvalidRow)?,
        causation_id: causation
            .map(|value| EntityRef::parse(&value))
            .transpose()
            .map_err(|_| EventOutboxError::InvalidRow)?,
        payload: row
            .try_get("payload")
            .map_err(|error| EventOutboxError::Persistence(PersistenceError::Query(error)))?,
        created_at: canonical_instant(&get("created_text")?)?,
    })
}

/// Parses a fixed-width UTC rendering into the one canonical wire spelling.
fn canonical_instant(text: &str) -> Result<WireTimestamp, EventOutboxError> {
    let body = text.strip_suffix('Z').ok_or(EventOutboxError::InvalidRow)?;
    let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
    let fraction = fraction.trim_end_matches('0');
    let canonical = if fraction.is_empty() {
        format!("{whole}Z")
    } else {
        format!("{whole}.{fraction}Z")
    };
    WireTimestamp::parse(&canonical).map_err(|_| EventOutboxError::InvalidRow)
}
