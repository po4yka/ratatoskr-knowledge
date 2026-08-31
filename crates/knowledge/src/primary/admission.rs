use ratatoskr_ai_archive_contracts::{
    AiArchiveImport, AiArchiveTombstone, AiArchiveTombstoneSubject, AiArtifactAdded,
    AiArtifactUpdated, AiConversationAdded, AiConversationUpdated, AiProjectAdded,
    AiProjectUpdated,
};
use ratatoskr_document_contracts::Document;
use ratatoskr_event_envelope::{EventEnvelope, EventPayload};
use ratatoskr_github_contracts::RepositoryAnalysisRequested;
use ratatoskr_social_contracts::{SocialSourceCaptured, SocialSourceRemoved, SocialSourceUpdated};
use sha2::{Digest as _, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{Database, PersistenceError};

mod metadata;

use metadata::{
    archive_artifact_metadata, archive_conversation_metadata, archive_import_metadata,
    archive_project_metadata, archive_tombstone_metadata, social_metadata,
};

/// Exact subjects accepted by the primary Knowledge durable.
pub const PRIMARY_EVENT_SUBJECTS: [&str; 13] = [
    "evt.content.document.extracted.v1",
    "evt.social.source.captured.v1",
    "evt.social.source.updated.v1",
    "evt.social.source.removed.v1",
    "evt.ai_archive.archive.imported.v1",
    "evt.ai_archive.conversation.added.v1",
    "evt.ai_archive.conversation.updated.v1",
    "evt.ai_archive.project.added.v1",
    "evt.ai_archive.project.updated.v1",
    "evt.ai_archive.artifact.added.v1",
    "evt.ai_archive.artifact.updated.v1",
    "evt.ai_archive.subject.tombstoned.v1",
    "evt.knowledge.repository_analysis.requested.v1",
];

/// Durable consequence used by the transport adapter to settle one delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDisposition {
    /// New immutable input and discoverable work committed; ACK the delivery.
    Accepted,
    /// Exact redelivery already committed; ACK without creating another work row.
    Duplicate,
    /// The fact was durably observed but is stale or suppressed; ACK without work.
    Suppressed,
    /// Permanently invalid input was recorded without content; terminate the delivery.
    Rejected,
    /// An event identifier named a different immutable fact; terminate the delivery.
    Collision,
}

/// Admission failure that must leave the broker delivery unsettled for bounded redelivery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PrimaryAdmissionError {
    /// Knowledge-owned durable state could not be read or committed.
    #[error("primary event admission could not be persisted")]
    Persistence(#[from] PersistenceError),
    /// A validated envelope could not be encoded for owned durable storage.
    #[error("primary event admission could not encode its canonical input")]
    Encode(#[from] serde_json::Error),
}

/// Collision-checking transactional intake for the primary event stream.
#[derive(Debug)]
pub struct PrimaryAdmissionStore<'a> {
    database: &'a Database,
}

impl<'a> PrimaryAdmissionStore<'a> {
    /// Creates an admission boundary over Knowledge-owned storage.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Validates and commits one transport delivery.
    ///
    /// The transport subject must be the exact `evt.<event_type>` for the canonical typed
    /// payload. Permanent failures are retained as content-free rejection evidence. Storage
    /// failures are returned so the caller can NAK rather than falsely ACK.
    ///
    /// # Errors
    ///
    /// Returns [`PrimaryAdmissionError`] only for transient persistence or owned encoding errors.
    #[allow(
        clippy::too_many_lines,
        reason = "the ACK boundary keeps validation, collision checking, and one transaction visible together"
    )]
    pub async fn admit(
        &self,
        transport_subject: &str,
        bytes: &[u8],
    ) -> Result<AdmissionDisposition, PrimaryAdmissionError> {
        let delivery_digest = digest(bytes);
        let Ok(envelope) = EventEnvelope::from_json(bytes) else {
            return self
                .reject(&delivery_digest, transport_subject, "envelope")
                .await;
        };
        let canonical = envelope
            .to_canonical_json()
            .map_err(|error| serde_json::Error::io(std::io::Error::other(error)))?;
        let envelope_digest = digest(canonical.as_bytes());
        let expected_subject = format!("evt.{}", envelope.event_type.to_wire());
        if transport_subject != expected_subject
            || !PRIMARY_EVENT_SUBJECTS.contains(&transport_subject)
        {
            return self
                .reject(&delivery_digest, transport_subject, "transport_subject")
                .await;
        }
        let metadata = match validate_envelope(&envelope) {
            Ok(metadata) => metadata,
            Err(code) => {
                return self.reject(&delivery_digest, transport_subject, code).await;
            }
        };
        let event_id = envelope.event_id.0;
        let envelope_value = serde_json::to_value(&envelope)?;
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;

        if let Some(existing) = existing_receipt(&mut transaction, event_id).await? {
            let exact = existing
                == (
                    transport_subject.to_owned(),
                    envelope_digest.clone(),
                    metadata.producer.clone(),
                    metadata.tenant_ref.clone(),
                    metadata.aggregate_id.clone(),
                    metadata.family.to_owned(),
                );
            if exact {
                transaction
                    .commit()
                    .await
                    .map_err(PersistenceError::Query)?;
                return Ok(AdmissionDisposition::Duplicate);
            }
            record_rejection(
                &mut transaction,
                &delivery_digest,
                transport_subject,
                "event_id_collision",
            )
            .await?;
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(AdmissionDisposition::Collision);
        }

        sqlx::query(
            "insert into knowledge.primary_event_receipts (
                 event_id, subject, envelope_digest_hex, producer, tenant_ref, aggregate_id, family
             ) values ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(event_id)
        .bind(transport_subject)
        .bind(&envelope_digest)
        .bind(&metadata.producer)
        .bind(&metadata.tenant_ref)
        .bind(&metadata.aggregate_id)
        .bind(metadata.family)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;

        let current = advance_source_head(&mut transaction, event_id, &metadata).await?;
        if current && metadata.lifecycle == LifecycleFact::Removed {
            suppress_source_work(&mut transaction, &metadata).await?;
            delete_source_derivatives(&mut transaction, &metadata).await?;
        }
        if current && !metadata.schedulable {
            let lifecycle = if metadata.lifecycle == LifecycleFact::Active {
                "active"
            } else {
                "removed"
            };
            sqlx::query(
                "insert into knowledge.primary_source_state (
                     family, tenant_ref, source_key, event_id, lifecycle, input_envelope
                 ) values ($1, $2, $3, $4, $5, $6)
                 on conflict (family, tenant_ref, source_key) do update set
                     event_id = excluded.event_id, lifecycle = excluded.lifecycle,
                     input_envelope = excluded.input_envelope, updated_at = now()",
            )
            .bind(metadata.family)
            .bind(&metadata.tenant_ref)
            .bind(&metadata.source_key)
            .bind(event_id)
            .bind(lifecycle)
            .bind(&envelope_value)
            .execute(&mut *transaction)
            .await
            .map_err(PersistenceError::Query)?;
        }
        if current && metadata.lifecycle == LifecycleFact::Active && metadata.schedulable {
            suppress_source_work(&mut transaction, &metadata).await?;
            delete_search_derivatives(&mut transaction, &metadata).await?;
            sqlx::query(
                "insert into knowledge.analysis_work (
                     work_id, event_id, family, tenant_ref, source_key, parent_source_key,
                     source_revision, input_envelope
                 ) values ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(Uuid::now_v7())
            .bind(event_id)
            .bind(metadata.family)
            .bind(&metadata.tenant_ref)
            .bind(&metadata.source_key)
            .bind(&metadata.parent_source_key)
            .bind(&metadata.revision)
            .bind(envelope_value)
            .execute(&mut *transaction)
            .await
            .map_err(PersistenceError::Query)?;
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(
            if current && metadata.lifecycle == LifecycleFact::Active && metadata.schedulable {
                AdmissionDisposition::Accepted
            } else {
                AdmissionDisposition::Suppressed
            },
        )
    }

    async fn reject(
        &self,
        digest: &str,
        subject: &str,
        code: &'static str,
    ) -> Result<AdmissionDisposition, PrimaryAdmissionError> {
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        record_rejection(&mut transaction, digest, subject, code).await?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(AdmissionDisposition::Rejected)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleFact {
    Active,
    Removed,
}

#[derive(Debug)]
struct EventMetadata {
    family: &'static str,
    producer: String,
    tenant_ref: String,
    aggregate_id: String,
    source_key: String,
    parent_source_key: String,
    revision: String,
    observed_at: String,
    lifecycle: LifecycleFact,
    schedulable: bool,
    removal_scope: RemovalScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemovalScope {
    Exact,
    Archive,
}

type MetadataTuple = (
    &'static str,
    String,
    String,
    String,
    LifecycleFact,
    bool,
    String,
);

#[allow(
    clippy::too_many_lines,
    reason = "the closed event registry is intentionally exhaustive and auditable in one dispatch"
)]
fn validate_envelope(envelope: &EventEnvelope) -> Result<EventMetadata, &'static str> {
    let event_type = envelope.event_type.to_wire();
    let producer = envelope.producer.as_str().to_owned();
    let tenant_ref = envelope.tenant_id.as_ref().ok_or("tenant")?.to_string();
    let aggregate_id = envelope.aggregate_id.to_wire();
    let mut observed_at = envelope.occurred_at.to_string();
    let (family, payload_tenant, source_key, revision, lifecycle, schedulable, expected_aggregate) =
        match event_type.as_str() {
            Document::EVENT_TYPE => {
                if producer != "ratatoskr-extractor" {
                    return Err("producer");
                }
                let payload = envelope.payload_as::<Document>().map_err(|_| "payload")?;
                payload.validate().map_err(|_| "payload")?;
                let key = payload.document_id.to_string();
                (
                    "document",
                    tenant_ref.clone(),
                    key.clone(),
                    payload.content_digest.hex.to_string(),
                    LifecycleFact::Active,
                    true,
                    format!("document:{key}"),
                )
            }
            SocialSourceCaptured::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<SocialSourceCaptured>()
                    .map_err(|_| "payload")?;
                observed_at = payload.source.captured_at.to_string();
                social_metadata(&producer, &payload.source, LifecycleFact::Active)?
            }
            SocialSourceUpdated::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<SocialSourceUpdated>()
                    .map_err(|_| "payload")?;
                observed_at = payload.source.captured_at.to_string();
                social_metadata(&producer, &payload.source, LifecycleFact::Active)?
            }
            SocialSourceRemoved::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<SocialSourceRemoved>()
                    .map_err(|_| "payload")?;
                if !matches!(
                    producer.as_str(),
                    "ratatoskr-x" | "ratatoskr-instagram" | "ratatoskr-threads"
                ) {
                    return Err("producer");
                }
                observed_at = payload.removed_at.to_string();
                let key = payload.social_source_id.to_string();
                (
                    "social",
                    payload.owner.to_string(),
                    key.clone(),
                    digest(payload.removed_at.to_string().as_bytes()),
                    LifecycleFact::Removed,
                    false,
                    format!("social_source:{key}"),
                )
            }
            AiArchiveImport::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<AiArchiveImport>()
                    .map_err(|_| "payload")?;
                observed_at = payload.imported_at.to_string();
                archive_import_metadata(&producer, &payload)?
            }
            AiConversationAdded::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<AiConversationAdded>()
                    .map_err(|_| "payload")?;
                payload.validate().map_err(|_| "payload")?;
                observed_at = payload.import_provenance.imported_at.to_string();
                archive_conversation_metadata(
                    &producer,
                    &payload.import_provenance,
                    &payload.conversation,
                )?
            }
            AiConversationUpdated::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<AiConversationUpdated>()
                    .map_err(|_| "payload")?;
                payload.validate().map_err(|_| "payload")?;
                observed_at = payload.import_provenance.imported_at.to_string();
                archive_conversation_metadata(
                    &producer,
                    &payload.import_provenance,
                    &payload.conversation,
                )?
            }
            AiProjectAdded::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<AiProjectAdded>()
                    .map_err(|_| "payload")?;
                observed_at = payload.import_provenance.imported_at.to_string();
                archive_project_metadata(
                    &producer,
                    &payload.import_provenance,
                    &payload.project,
                    &payload.content_digest,
                )?
            }
            AiProjectUpdated::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<AiProjectUpdated>()
                    .map_err(|_| "payload")?;
                observed_at = payload.import_provenance.imported_at.to_string();
                archive_project_metadata(
                    &producer,
                    &payload.import_provenance,
                    &payload.project,
                    &payload.content_digest,
                )?
            }
            AiArtifactAdded::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<AiArtifactAdded>()
                    .map_err(|_| "payload")?;
                observed_at = payload.import_provenance.imported_at.to_string();
                archive_artifact_metadata(&producer, &payload.import_provenance, &payload.artifact)?
            }
            AiArtifactUpdated::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<AiArtifactUpdated>()
                    .map_err(|_| "payload")?;
                observed_at = payload.import_provenance.imported_at.to_string();
                archive_artifact_metadata(&producer, &payload.import_provenance, &payload.artifact)?
            }
            AiArchiveTombstone::EVENT_TYPE => {
                let payload = envelope
                    .payload_as::<AiArchiveTombstone>()
                    .map_err(|_| "payload")?;
                observed_at = payload.observed_at.to_string();
                archive_tombstone_metadata(&producer, &payload)?
            }
            RepositoryAnalysisRequested::EVENT_TYPE => {
                if producer != "ratatoskr-github" {
                    return Err("producer");
                }
                let payload = envelope
                    .payload_as::<RepositoryAnalysisRequested>()
                    .map_err(|_| "payload")?;
                let key = payload.repository_id.to_string();
                (
                    "repository",
                    payload.owner.to_string(),
                    key.clone(),
                    payload.idempotency_key.hex.to_string(),
                    LifecycleFact::Active,
                    true,
                    format!("repository:{key}"),
                )
            }
            _ => return Err("event_type"),
        };
    if payload_tenant != tenant_ref {
        return Err("tenant");
    }
    if aggregate_id != expected_aggregate {
        return Err("aggregate");
    }
    let (source_key, parent_source_key, removal_scope) =
        typed_source_scope(envelope, &event_type, source_key)?;
    Ok(EventMetadata {
        family,
        producer,
        tenant_ref,
        aggregate_id,
        source_key,
        parent_source_key,
        revision,
        observed_at,
        lifecycle,
        schedulable,
        removal_scope,
    })
}

fn typed_source_scope(
    envelope: &EventEnvelope,
    event_type: &str,
    source_key: String,
) -> Result<(String, String, RemovalScope), &'static str> {
    let exact = |kind: &str, id: String, archive_id: String| {
        (
            format!("{kind}:{id}"),
            format!("archive:{archive_id}"),
            RemovalScope::Exact,
        )
    };
    match event_type {
        AiArchiveImport::EVENT_TYPE => {
            let payload = envelope
                .payload_as::<AiArchiveImport>()
                .map_err(|_| "payload")?;
            let key = format!("archive:{}", payload.ai_archive_id);
            Ok((key.clone(), key, RemovalScope::Exact))
        }
        AiConversationAdded::EVENT_TYPE | AiConversationUpdated::EVENT_TYPE => {
            let (id, archive_id) = if event_type == AiConversationAdded::EVENT_TYPE {
                let payload = envelope
                    .payload_as::<AiConversationAdded>()
                    .map_err(|_| "payload")?;
                (
                    payload.conversation.ai_conversation_id.to_string(),
                    payload.import_provenance.ai_archive_id.to_string(),
                )
            } else {
                let payload = envelope
                    .payload_as::<AiConversationUpdated>()
                    .map_err(|_| "payload")?;
                (
                    payload.conversation.ai_conversation_id.to_string(),
                    payload.import_provenance.ai_archive_id.to_string(),
                )
            };
            Ok(exact("conversation", id, archive_id))
        }
        AiProjectAdded::EVENT_TYPE | AiProjectUpdated::EVENT_TYPE => {
            let (id, archive_id) = if event_type == AiProjectAdded::EVENT_TYPE {
                let payload = envelope
                    .payload_as::<AiProjectAdded>()
                    .map_err(|_| "payload")?;
                (
                    payload.project.ai_project_id.to_string(),
                    payload.import_provenance.ai_archive_id.to_string(),
                )
            } else {
                let payload = envelope
                    .payload_as::<AiProjectUpdated>()
                    .map_err(|_| "payload")?;
                (
                    payload.project.ai_project_id.to_string(),
                    payload.import_provenance.ai_archive_id.to_string(),
                )
            };
            Ok(exact("project", id, archive_id))
        }
        AiArtifactAdded::EVENT_TYPE | AiArtifactUpdated::EVENT_TYPE => {
            let (id, archive_id) = if event_type == AiArtifactAdded::EVENT_TYPE {
                let payload = envelope
                    .payload_as::<AiArtifactAdded>()
                    .map_err(|_| "payload")?;
                (
                    payload.artifact.external_artifact_id.to_string(),
                    payload.import_provenance.ai_archive_id.to_string(),
                )
            } else {
                let payload = envelope
                    .payload_as::<AiArtifactUpdated>()
                    .map_err(|_| "payload")?;
                (
                    payload.artifact.external_artifact_id.to_string(),
                    payload.import_provenance.ai_archive_id.to_string(),
                )
            };
            Ok(exact("artifact", id, archive_id))
        }
        AiArchiveTombstone::EVENT_TYPE => {
            let payload = envelope
                .payload_as::<AiArchiveTombstone>()
                .map_err(|_| "payload")?;
            let archive_key = format!("archive:{}", payload.ai_archive_id);
            let (key, scope) = match payload.subject {
                AiArchiveTombstoneSubject::Archive => (archive_key.clone(), RemovalScope::Archive),
                AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => (
                    format!("conversation:{ai_conversation_id}"),
                    RemovalScope::Exact,
                ),
                AiArchiveTombstoneSubject::Project { ai_project_id } => {
                    (format!("project:{ai_project_id}"), RemovalScope::Exact)
                }
                AiArchiveTombstoneSubject::Artifact {
                    external_artifact_id,
                } => (
                    format!("artifact:{external_artifact_id}"),
                    RemovalScope::Exact,
                ),
            };
            Ok((key, archive_key, scope))
        }
        _ => Ok((source_key.clone(), source_key, RemovalScope::Exact)),
    }
}

async fn existing_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
) -> Result<Option<(String, String, String, String, String, String)>, PrimaryAdmissionError> {
    sqlx::query_as(
        "select subject, envelope_digest_hex, producer, tenant_ref, aggregate_id, family
         from knowledge.primary_event_receipts where event_id = $1",
    )
    .bind(event_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)
    .map_err(PrimaryAdmissionError::from)
}

async fn record_rejection(
    transaction: &mut Transaction<'_, Postgres>,
    delivery_digest: &str,
    transport_subject: &str,
    code: &str,
) -> Result<(), PrimaryAdmissionError> {
    sqlx::query(
        "insert into knowledge.primary_event_rejections (
             rejection_id, delivery_digest_hex, transport_subject, rejection_code
         ) values ($1, $2, $3, $4)
         on conflict (delivery_digest_hex, transport_subject, rejection_code) do update set
             last_seen_at = now(), occurrence_count = knowledge.primary_event_rejections.occurrence_count + 1",
    )
    .bind(Uuid::now_v7())
    .bind(delivery_digest)
    .bind(transport_subject)
    .bind(code)
    .execute(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(())
}

async fn advance_source_head(
    transaction: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    metadata: &EventMetadata,
) -> Result<bool, PrimaryAdmissionError> {
    if metadata.family == "ai_archive" && metadata.source_key != metadata.parent_source_key {
        let parent_removed: bool = sqlx::query_scalar(
            "select exists (
                 select 1 from knowledge.primary_source_heads
                 where family = 'ai_archive' and tenant_ref = $1 and source_key = $2
                   and lifecycle = 'removed'
             )",
        )
        .bind(&metadata.tenant_ref)
        .bind(&metadata.parent_source_key)
        .fetch_one(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if parent_removed {
            return Ok(false);
        }
    }
    let lifecycle = match metadata.lifecycle {
        LifecycleFact::Active => "active",
        LifecycleFact::Removed => "removed",
    };
    let changed = sqlx::query(
        "insert into knowledge.primary_source_heads (
             family, tenant_ref, source_key, revision, observed_at, lifecycle, event_id
         ) values ($1, $2, $3, $4, $5::timestamptz, $6, $7)
         on conflict (family, tenant_ref, source_key) do update set
             revision = excluded.revision, observed_at = excluded.observed_at,
             lifecycle = excluded.lifecycle, event_id = excluded.event_id
         where (excluded.revision <> knowledge.primary_source_heads.revision
                or excluded.lifecycle <> knowledge.primary_source_heads.lifecycle)
           and (excluded.observed_at > knowledge.primary_source_heads.observed_at
                or (excluded.observed_at = knowledge.primary_source_heads.observed_at
                    and excluded.lifecycle = 'removed'
                    and knowledge.primary_source_heads.lifecycle = 'active'))
         returning revision",
    )
    .bind(metadata.family)
    .bind(&metadata.tenant_ref)
    .bind(&metadata.source_key)
    .bind(&metadata.revision)
    .bind(&metadata.observed_at)
    .bind(lifecycle)
    .bind(event_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(changed.is_some())
}

async fn suppress_source_work(
    transaction: &mut Transaction<'_, Postgres>,
    metadata: &EventMetadata,
) -> Result<(), PrimaryAdmissionError> {
    let (column, key) = if metadata.removal_scope == RemovalScope::Archive {
        ("parent_source_key", &metadata.parent_source_key)
    } else {
        ("source_key", &metadata.source_key)
    };
    let query = format!(
        "update knowledge.analysis_work set state = 'suppressed', terminal_code = 'source_removed',
             lease_owner = null, lease_expires_at = null, updated_at = now()
         where family = $1 and tenant_ref = $2 and {column} = $3
           and state not in ('completed', 'failed', 'suppressed')"
    );
    sqlx::query(&query)
        .bind(metadata.family)
        .bind(&metadata.tenant_ref)
        .bind(key)
        .execute(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?;
    Ok(())
}

async fn delete_source_derivatives(
    transaction: &mut Transaction<'_, Postgres>,
    metadata: &EventMetadata,
) -> Result<(), PrimaryAdmissionError> {
    let source_ids = source_ids_for_metadata(transaction, metadata).await?;
    if source_ids.is_empty() {
        return Ok(());
    }
    delete_search_rows(transaction, &source_ids).await?;
    sqlx::query(
        "delete from knowledge.analysis_outputs where run_id in (
             select run_id from knowledge.analysis_runs where source_ref_id = any($1)
         )",
    )
    .bind(&source_ids)
    .execute(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    sqlx::query(
        "delete from knowledge.analysis_attempts where run_id in (
             select run_id from knowledge.analysis_runs where source_ref_id = any($1)
         )",
    )
    .bind(&source_ids)
    .execute(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    sqlx::query("delete from knowledge.analysis_runs where source_ref_id = any($1)")
        .bind(&source_ids)
        .execute(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?;
    sqlx::query("delete from knowledge.source_refs where source_ref_id = any($1)")
        .bind(&source_ids)
        .execute(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?;
    Ok(())
}

async fn delete_search_derivatives(
    transaction: &mut Transaction<'_, Postgres>,
    metadata: &EventMetadata,
) -> Result<(), PrimaryAdmissionError> {
    let source_ids = source_ids_for_metadata(transaction, metadata).await?;
    delete_search_rows(transaction, &source_ids).await
}

async fn source_ids_for_metadata(
    transaction: &mut Transaction<'_, Postgres>,
    metadata: &EventMetadata,
) -> Result<Vec<Uuid>, PrimaryAdmissionError> {
    if metadata.removal_scope == RemovalScope::Archive {
        let archive_id = metadata
            .parent_source_key
            .strip_prefix("archive:")
            .ok_or(PersistenceError::InvalidSource)?;
        Ok(sqlx::query_scalar(
            "select source_ref_id from knowledge.source_refs
             where tenant_ref = $1 and ai_archive_id = $2",
        )
        .bind(&metadata.tenant_ref)
        .bind(archive_id)
        .fetch_all(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?)
    } else {
        let source_id = metadata
            .source_key
            .split_once(':')
            .map_or(metadata.source_key.as_str(), |(_, id)| id);
        let contract_prefix = match metadata.family {
            "social" => "social_%",
            "ai_archive" => "archive_%",
            "repository" => "repository_%",
            "document" => "article_%",
            _ => return Err(PersistenceError::InvalidSource.into()),
        };
        Ok(sqlx::query_scalar(
            "select distinct s.source_ref_id from knowledge.source_refs s
             join knowledge.analysis_runs r on r.source_ref_id = s.source_ref_id
             where s.tenant_ref = $1 and s.source_document_id = $2
               and r.contract_version like $3",
        )
        .bind(&metadata.tenant_ref)
        .bind(source_id)
        .bind(contract_prefix)
        .fetch_all(&mut **transaction)
        .await
        .map_err(PersistenceError::Query)?)
    }
}

async fn delete_search_rows(
    transaction: &mut Transaction<'_, Postgres>,
    source_ids: &[Uuid],
) -> Result<(), PrimaryAdmissionError> {
    if source_ids.is_empty() {
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
            .bind(source_ids)
            .execute(&mut **transaction)
            .await
            .map_err(PersistenceError::Query)?;
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
