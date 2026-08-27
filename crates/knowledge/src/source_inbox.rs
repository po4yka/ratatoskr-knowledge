//! Durable inbox intake for state-carried social and AI-archive source facts.

use ratatoskr_ai_archive_contracts::{
    AiArchiveProvenance, AiArchiveTombstone, AiArchiveTombstoneSubject, AiConversation, AiProject,
};
use ratatoskr_identifiers::{ContentDigest, WireTimestamp};
use ratatoskr_social_contracts::SocialSourceSnapshot;

use crate::{Database, DeletionError, DeletionScope, PersistenceError, execute_deletion};

/// Complete source fact for an archived conversation, including immutable import provenance.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArchiveConversationSource {
    /// Immutable source import evidence.
    pub provenance: AiArchiveProvenance,
    /// Current normalized conversation revision.
    pub conversation: AiConversation,
}

/// Complete source fact for an archived project, including immutable import provenance.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArchiveProjectSource {
    /// Immutable source import evidence.
    pub provenance: AiArchiveProvenance,
    /// Current normalized project revision.
    pub project: AiProject,
    /// Digest of the canonical normalized project representation.
    pub content_digest: ContentDigest,
}

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
    /// An archive payload contradicted its immutable import provenance.
    #[error("the archive source fact contradicted its import provenance")]
    InvalidArchiveFact,
    /// The durable deletion of derived state failed.
    #[error(transparent)]
    Deletion(#[from] DeletionError),
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
        provenance: &AiArchiveProvenance,
        conversation: &AiConversation,
    ) -> Result<SourceInboxAdmission, SourceInboxError> {
        provenance
            .validate_conversation(conversation)
            .map_err(|_| SourceInboxError::InvalidArchiveFact)?;
        let source = ArchiveConversationSource {
            provenance: provenance.clone(),
            conversation: conversation.clone(),
        };
        self.accept(Delivery {
            event_id,
            subject,
            family: "ai_archive",
            tenant_ref: provenance.owner.to_string(),
            source_id: conversation.ai_conversation_id.to_string(),
            content_digest_hex: conversation.content_digest.hex.to_string(),
            observed_at: provenance.imported_at,
            snapshot: serde_json::to_value(source).map_err(SourceInboxError::Encode)?,
        })
        .await
    }

    /// Claims an AI project added or updated source fact.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] when the delivery cannot be persisted.
    pub async fn accept_ai_project(
        &self,
        event_id: uuid::Uuid,
        subject: &str,
        provenance: &AiArchiveProvenance,
        project: &AiProject,
        content_digest: &ContentDigest,
    ) -> Result<SourceInboxAdmission, SourceInboxError> {
        provenance
            .validate_project(project)
            .map_err(|_| SourceInboxError::InvalidArchiveFact)?;
        self.accept(Delivery {
            event_id,
            subject,
            family: "ai_archive",
            tenant_ref: provenance.owner.to_string(),
            source_id: project.ai_project_id.to_string(),
            content_digest_hex: content_digest.hex.to_string(),
            observed_at: provenance.imported_at,
            snapshot: serde_json::to_value(ArchiveProjectSource {
                provenance: provenance.clone(),
                project: project.clone(),
                content_digest: content_digest.clone(),
            })
            .map_err(SourceInboxError::Encode)?,
        })
        .await
    }

    /// Claims an authoritative archive tombstone without creating a new source head.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] when the delivery cannot be persisted.
    pub async fn accept_ai_tombstone(
        &self,
        event_id: uuid::Uuid,
        subject: &str,
        tombstone: &AiArchiveTombstone,
    ) -> Result<SourceInboxAdmission, SourceInboxError> {
        let (source_id, deletion_scope) = match &tombstone.subject {
            AiArchiveTombstoneSubject::Archive => (
                tombstone.ai_archive_id.to_string(),
                DeletionScope::Archive {
                    tenant_ref: tombstone.owner.to_string(),
                    ai_archive_id: tombstone.ai_archive_id.to_string(),
                },
            ),
            AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => (
                ai_conversation_id.to_string(),
                DeletionScope::Source {
                    tenant_ref: tombstone.owner.to_string(),
                    owner_context: "ratatoskr-knowledge".to_owned(),
                    source_document_id: ai_conversation_id.to_string(),
                },
            ),
            AiArchiveTombstoneSubject::Project { ai_project_id } => (
                ai_project_id.to_string(),
                DeletionScope::Source {
                    tenant_ref: tombstone.owner.to_string(),
                    owner_context: "ratatoskr-knowledge".to_owned(),
                    source_document_id: ai_project_id.to_string(),
                },
            ),
            AiArchiveTombstoneSubject::Artifact {
                external_artifact_id,
            } => (
                external_artifact_id.to_string(),
                DeletionScope::Source {
                    tenant_ref: tombstone.owner.to_string(),
                    owner_context: "ratatoskr-knowledge".to_owned(),
                    source_document_id: external_artifact_id.to_string(),
                },
            ),
        };
        let delivery = Delivery {
            event_id,
            subject,
            family: "ai_archive",
            tenant_ref: tombstone.owner.to_string(),
            source_id,
            content_digest_hex: tombstone.evidence_ref.digest.hex.to_string(),
            observed_at: tombstone.observed_at,
            snapshot: serde_json::to_value(tombstone).map_err(SourceInboxError::Encode)?,
        };
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        if !insert_receipt(&mut transaction, delivery).await? {
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(SourceInboxAdmission::Duplicate);
        }
        execute_deletion(&mut transaction, &deletion_scope).await?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(SourceInboxAdmission::AcceptedCurrent)
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
    ) -> Result<ArchiveConversationSource, SourceInboxError> {
        self.snapshot(event_id, "ai_archive").await
    }

    /// Loads one archive project after durable inbox admission.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] if the delivery is absent, belongs to another family, or
    /// the owned snapshot can no longer be decoded as its published contract.
    pub async fn archive_project(
        &self,
        event_id: uuid::Uuid,
    ) -> Result<ArchiveProjectSource, SourceInboxError> {
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
}

async fn insert_receipt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    delivery: Delivery<'_>,
) -> Result<bool, SourceInboxError> {
    let inserted = sqlx::query_scalar::<_, uuid::Uuid>(
            "insert into knowledge.source_analysis_inbox
                 (event_id, subject, family, tenant_ref, source_id, content_digest_hex, observed_at, snapshot)
             values ($1, $2, $3, $4, $5, $6, $7::timestamptz, $8)
             on conflict (event_id) do nothing returning event_id",
        )
        .bind(delivery.event_id)
        .bind(delivery.subject)
        .bind(delivery.family)
        .bind(&delivery.tenant_ref)
        .bind(&delivery.source_id)
        .bind(&delivery.content_digest_hex)
        .bind(delivery.observed_at.to_string())
        .bind(delivery.snapshot)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?;
    Ok(inserted.is_some())
}

impl<'a> SourceInbox<'a> {
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
