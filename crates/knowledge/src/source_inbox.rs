//! Durable inbox intake for state-carried social and AI-archive source facts.

use ratatoskr_ai_archive_contracts::{
    AiArchiveProvenance, AiArchiveTombstone, AiArchiveTombstoneSubject, AiConversation,
    AiConversationAdded, AiConversationUpdated, AiProject,
};
use ratatoskr_event_envelope::{EnvelopeError, EventEnvelope, EventPayload};
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
    /// An authoritative tombstone is newer than this archive revision.
    Tombstoned,
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
    /// The envelope is not a supported AI-archive conversation lifecycle fact.
    #[error("the archive event could not be decoded")]
    Envelope(#[from] EnvelopeError),
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
            archive_id: None,
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
            archive_id: Some(provenance.ai_archive_id.to_string()),
            source_id: conversation.ai_conversation_id.to_string(),
            content_digest_hex: conversation.content_digest.hex.to_string(),
            observed_at: provenance.imported_at,
            snapshot: serde_json::to_value(source).map_err(SourceInboxError::Encode)?,
        })
        .await
    }

    /// Claims one published AI-archive conversation envelope.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] when the event is not a supported conversation lifecycle
    /// fact, provenance disagrees with its subject, or durable admission fails.
    pub async fn accept_ai_envelope(
        &self,
        envelope: &EventEnvelope,
    ) -> Result<SourceInboxAdmission, SourceInboxError> {
        let event_id = envelope.event_id.0;
        let subject = envelope.event_type.to_wire();
        match subject.as_str() {
            AiConversationAdded::EVENT_TYPE => {
                let payload = envelope.payload_as::<AiConversationAdded>()?;
                payload
                    .validate()
                    .map_err(|_| SourceInboxError::InvalidArchiveFact)?;
                self.accept_ai_conversation(
                    event_id,
                    &subject,
                    &payload.import_provenance,
                    &payload.conversation,
                )
                .await
            }
            AiConversationUpdated::EVENT_TYPE => {
                let payload = envelope.payload_as::<AiConversationUpdated>()?;
                payload
                    .validate()
                    .map_err(|_| SourceInboxError::InvalidArchiveFact)?;
                self.accept_ai_conversation(
                    event_id,
                    &subject,
                    &payload.import_provenance,
                    &payload.conversation,
                )
                .await
            }
            _ => Err(EnvelopeError::PayloadType {
                expected: AiConversationAdded::EVENT_TYPE,
                found: subject,
            }
            .into()),
        }
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
            archive_id: Some(provenance.ai_archive_id.to_string()),
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
            archive_id: Some(tombstone.ai_archive_id.to_string()),
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
        if subject_belongs_only_to_another_tenant(&mut transaction, &deletion_scope).await? {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(SourceInboxError::InvalidArchiveFact);
        }
        let duplicate: bool = sqlx::query_scalar(
            "select exists (select 1 from knowledge.source_analysis_inbox where event_id = $1)",
        )
        .bind(event_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if duplicate {
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(SourceInboxAdmission::Duplicate);
        }
        self.remove_tombstoned_inbox_sources(&mut transaction, tombstone)
            .await?;
        if !insert_receipt(&mut transaction, &delivery).await? {
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(SourceInboxAdmission::Duplicate);
        }
        record_tombstone(&mut transaction, event_id, tombstone).await?;
        execute_deletion(&mut transaction, &deletion_scope).await?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(SourceInboxAdmission::AcceptedCurrent)
    }

    async fn remove_tombstoned_inbox_sources(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tombstone: &AiArchiveTombstone,
    ) -> Result<(), SourceInboxError> {
        let tenant_ref = tombstone.owner.to_string();
        match &tombstone.subject {
            AiArchiveTombstoneSubject::Archive => {
                let archive_id = tombstone.ai_archive_id.to_string();
                sqlx::query(
                    "delete from knowledge.source_analysis_heads
                     where family = 'ai_archive' and tenant_ref = $1 and exists (
                         select 1 from knowledge.source_analysis_inbox inbox
                         where inbox.event_id = knowledge.source_analysis_heads.inbox_event_id
                           and inbox.archive_id = $2
                     )",
                )
                .bind(&tenant_ref)
                .bind(&archive_id)
                .execute(&mut **transaction)
                .await
                .map_err(PersistenceError::Query)?;
                sqlx::query(
                    "delete from knowledge.source_analysis_inbox
                     where family = 'ai_archive' and tenant_ref = $1 and archive_id = $2",
                )
                .bind(&tenant_ref)
                .bind(archive_id)
                .execute(&mut **transaction)
                .await
                .map_err(PersistenceError::Query)?;
            }
            AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => {
                self.remove_tombstoned_source(
                    transaction,
                    &tenant_ref,
                    &ai_conversation_id.to_string(),
                )
                .await?;
            }
            AiArchiveTombstoneSubject::Project { ai_project_id } => {
                self.remove_tombstoned_source(transaction, &tenant_ref, &ai_project_id.to_string())
                    .await?;
            }
            AiArchiveTombstoneSubject::Artifact {
                external_artifact_id,
            } => {
                self.remove_tombstoned_source(
                    transaction,
                    &tenant_ref,
                    &external_artifact_id.to_string(),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn remove_tombstoned_source(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant_ref: &str,
        source_id: &str,
    ) -> Result<(), SourceInboxError> {
        sqlx::query(
            "delete from knowledge.source_analysis_heads
             where family = 'ai_archive' and tenant_ref = $1 and source_id = $2",
        )
        .bind(tenant_ref)
        .bind(source_id)
        .execute(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?;
        sqlx::query(
            "delete from knowledge.source_analysis_inbox
             where family = 'ai_archive' and tenant_ref = $1 and source_id = $2",
        )
        .bind(tenant_ref)
        .bind(source_id)
        .execute(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?;
        Ok(())
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
        let blocked = delivery.family == "ai_archive"
            && tombstone_blocks(&mut transaction, &delivery).await?;
        if !insert_receipt(&mut transaction, &delivery).await? {
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(SourceInboxAdmission::Duplicate);
        }
        if blocked {
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(SourceInboxAdmission::Tombstoned);
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

async fn subject_belongs_only_to_another_tenant(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
) -> Result<bool, SourceInboxError> {
    let (known, owned): (bool, bool) = match scope {
        DeletionScope::Archive {
            tenant_ref,
            ai_archive_id,
        } => {
            sqlx::query_as(
                "select
                 exists(select 1 from knowledge.source_refs where ai_archive_id = $1),
                 exists(select 1 from knowledge.source_refs
                        where ai_archive_id = $1 and tenant_ref = $2)",
            )
            .bind(ai_archive_id)
            .bind(tenant_ref)
            .fetch_one(&mut **transaction)
            .await
        }
        DeletionScope::Source {
            tenant_ref,
            owner_context,
            source_document_id,
        } => {
            sqlx::query_as(
                "select
                 exists(select 1 from knowledge.source_refs
                        where owner_context = $1 and source_document_id = $2),
                 exists(select 1 from knowledge.source_refs
                        where owner_context = $1 and source_document_id = $2 and tenant_ref = $3)",
            )
            .bind(owner_context)
            .bind(source_document_id)
            .bind(tenant_ref)
            .fetch_one(&mut **transaction)
            .await
        }
        DeletionScope::Tenant { .. } => return Ok(false),
    }
    .map_err(PersistenceError::Query)?;
    Ok(known && !owned)
}

async fn tombstone_blocks(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    delivery: &Delivery<'_>,
) -> Result<bool, SourceInboxError> {
    let blocked: bool = sqlx::query_scalar(
        "select exists (
                 select 1 from knowledge.ai_archive_tombstones
                 where tenant_ref = $1 and (
                       (subject_kind = 'archive' and archive_id = $2)
                    or (subject_kind in ('conversation', 'project', 'artifact') and subject_id = $3)
                 ) and observed_at >= $4::timestamptz
             )",
    )
    .bind(&delivery.tenant_ref)
    .bind(delivery.archive_id.as_deref().unwrap_or_default())
    .bind(&delivery.source_id)
    .bind(delivery.observed_at.to_string())
    .fetch_one(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(blocked)
}

fn tombstone_subject_parts(subject: &AiArchiveTombstoneSubject) -> (&'static str, Option<String>) {
    match subject {
        AiArchiveTombstoneSubject::Archive => ("archive", None),
        AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => {
            ("conversation", Some(ai_conversation_id.to_string()))
        }
        AiArchiveTombstoneSubject::Project { ai_project_id } => {
            ("project", Some(ai_project_id.to_string()))
        }
        AiArchiveTombstoneSubject::Artifact {
            external_artifact_id,
        } => ("artifact", Some(external_artifact_id.to_string())),
    }
}

async fn record_tombstone(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event_id: uuid::Uuid,
    tombstone: &AiArchiveTombstone,
) -> Result<(), SourceInboxError> {
    let (subject_kind, subject_id) = tombstone_subject_parts(&tombstone.subject);
    sqlx::query(
        "insert into knowledge.ai_archive_tombstones
             (event_id, tenant_ref, archive_id, subject_kind, subject_id, observed_at)
         values ($1, $2, $3, $4, $5, $6::timestamptz)",
    )
    .bind(event_id)
    .bind(tombstone.owner.to_string())
    .bind(tombstone.ai_archive_id.to_string())
    .bind(subject_kind)
    .bind(subject_id)
    .bind(tombstone.observed_at.to_string())
    .execute(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(())
}

async fn insert_receipt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    delivery: &Delivery<'_>,
) -> Result<bool, SourceInboxError> {
    let inserted = sqlx::query_scalar::<_, uuid::Uuid>(
            "insert into knowledge.source_analysis_inbox
                 (event_id, subject, family, tenant_ref, archive_id, source_id, content_digest_hex, observed_at, snapshot)
             values ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz, $9)
             on conflict (event_id) do nothing returning event_id",
        )
        .bind(delivery.event_id)
        .bind(delivery.subject)
        .bind(delivery.family)
        .bind(&delivery.tenant_ref)
        .bind(&delivery.archive_id)
        .bind(&delivery.source_id)
        .bind(&delivery.content_digest_hex)
        .bind(delivery.observed_at.to_string())
        .bind(&delivery.snapshot)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?;
    Ok(inserted.is_some())
}

impl SourceInbox<'_> {
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
    archive_id: Option<String>,
    source_id: String,
    content_digest_hex: String,
    observed_at: WireTimestamp,
    snapshot: serde_json::Value,
}
