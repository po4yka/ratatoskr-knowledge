//! Durable inbox intake for state-carried social and AI-archive source facts.

use ratatoskr_ai_archive_contracts::AiConversation;
use ratatoskr_identifiers::WireTimestamp;
use ratatoskr_social_contracts::SocialSourceSnapshot;

use crate::{Database, PersistenceError};

/// Result of claiming one at-least-once source delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceInboxAdmission {
    /// A new delivery became the current source head.
    AcceptedCurrent,
    /// A new but delayed delivery was retained without regressing the current head.
    AcceptedHistorical,
    /// The exact delivery was already claimed.
    Duplicate,
}

/// Safe source-inbox failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceInboxError {
    /// Knowledge-owned durable state could not be written.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    /// A contract snapshot could not be encoded for durable storage.
    #[error("the source snapshot could not be encoded")]
    Encode(#[source] serde_json::Error),
}

/// Consumer that claims social and AI-archive source facts before analysis scheduling.
#[derive(Debug)]
pub struct SourceInbox<'a> {
    database: &'a Database,
}

impl<'a> SourceInbox<'a> {
    /// Creates an inbox consumer over Knowledge-owned storage.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Claims a captured or updated social snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] when the delivery cannot be persisted.
    pub async fn accept_social(
        &self,
        event_id: uuid::Uuid,
        subject: &str,
        snapshot: &SocialSourceSnapshot,
    ) -> Result<SourceInboxAdmission, SourceInboxError> {
        self.accept(Delivery {
            event_id,
            subject,
            family: "social",
            tenant_ref: snapshot.owner.to_string(),
            source_id: snapshot.social_source_id.to_string(),
            content_digest_hex: snapshot.content_digest.hex.to_string(),
            observed_at: snapshot.captured_at,
            snapshot: serde_json::to_value(snapshot).map_err(SourceInboxError::Encode)?,
        })
        .await
    }

    /// Claims an AI conversation added or updated source fact.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] when the delivery cannot be persisted.
    pub async fn accept_ai_conversation(
        &self,
        event_id: uuid::Uuid,
        subject: &str,
        observed_at: WireTimestamp,
        conversation: &AiConversation,
    ) -> Result<SourceInboxAdmission, SourceInboxError> {
        self.accept(Delivery {
            event_id,
            subject,
            family: "ai_archive",
            tenant_ref: conversation.owner.to_string(),
            source_id: conversation.ai_conversation_id.to_string(),
            content_digest_hex: conversation.content_digest.hex.to_string(),
            observed_at,
            snapshot: serde_json::to_value(conversation).map_err(SourceInboxError::Encode)?,
        })
        .await
    }

    /// Loads one social snapshot after durable inbox admission.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] if the delivery is absent, belongs to another family, or
    /// the owned snapshot can no longer be decoded as its published contract.
    pub async fn social_snapshot(
        &self,
        event_id: uuid::Uuid,
    ) -> Result<SocialSourceSnapshot, SourceInboxError> {
        self.snapshot(event_id, "social").await
    }

    /// Loads one archive conversation after durable inbox admission.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] if the delivery is absent, belongs to another family, or
    /// the owned snapshot can no longer be decoded as its published contract.
    pub async fn archive_conversation(
        &self,
        event_id: uuid::Uuid,
    ) -> Result<AiConversation, SourceInboxError> {
        self.snapshot(event_id, "ai_archive").await
    }

    async fn accept(
        &self,
        delivery: Delivery<'_>,
    ) -> Result<SourceInboxAdmission, SourceInboxError> {
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let inserted = sqlx::query_scalar::<_, uuid::Uuid>(
            "insert into knowledge.source_analysis_inbox
                 (event_id, subject, family, tenant_ref, source_id, content_digest_hex, observed_at, snapshot)
             values ($1, $2, $3, $4, $5, $6, $7::timestamptz, $8)
             on conflict (event_id) do nothing returning event_id",
        )
        .bind(delivery.event_id).bind(delivery.subject).bind(delivery.family).bind(&delivery.tenant_ref).bind(&delivery.source_id)
        .bind(&delivery.content_digest_hex).bind(delivery.observed_at.to_string()).bind(delivery.snapshot)
        .fetch_optional(&mut *transaction).await.map_err(PersistenceError::Query)?;
        if inserted.is_none() {
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(SourceInboxAdmission::Duplicate);
        }
        let updated = sqlx::query(
            "insert into knowledge.source_analysis_heads
                 (family, tenant_ref, source_id, content_digest_hex, observed_at, inbox_event_id)
             values ($1, $2, $3, $4, $5::timestamptz, $6)
             on conflict (family, tenant_ref, source_id) do update set
                 content_digest_hex = excluded.content_digest_hex, observed_at = excluded.observed_at,
                 inbox_event_id = excluded.inbox_event_id
             where excluded.observed_at > knowledge.source_analysis_heads.observed_at",
        )
        .bind(delivery.family).bind(&delivery.tenant_ref).bind(&delivery.source_id).bind(&delivery.content_digest_hex)
        .bind(delivery.observed_at.to_string()).bind(delivery.event_id)
        .execute(&mut *transaction).await.map_err(PersistenceError::Query)?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(if updated.rows_affected() == 1 {
            SourceInboxAdmission::AcceptedCurrent
        } else {
            SourceInboxAdmission::AcceptedHistorical
        })
    }

    async fn snapshot<T: serde::de::DeserializeOwned>(
        &self,
        event_id: uuid::Uuid,
        family: &str,
    ) -> Result<T, SourceInboxError> {
        let snapshot: Option<serde_json::Value> = sqlx::query_scalar(
            "select snapshot from knowledge.source_analysis_inbox where event_id = $1 and family = $2",
        )
        .bind(event_id)
        .bind(family)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        serde_json::from_value(snapshot.ok_or(PersistenceError::InvalidSource)?)
            .map_err(SourceInboxError::Encode)
    }
}

#[derive(Debug)]
struct Delivery<'a> {
    event_id: uuid::Uuid,
    subject: &'a str,
    family: &'a str,
    tenant_ref: String,
    source_id: String,
    content_digest_hex: String,
    observed_at: WireTimestamp,
    snapshot: serde_json::Value,
}
