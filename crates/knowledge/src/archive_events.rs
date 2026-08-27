//! Typed AI-archive lifecycle event intake and authoritative deletion propagation.

use ratatoskr_ai_archive_contracts::{
    AiArchiveImport, AiArchiveTombstone, AiArchiveTombstoneSubject, AiArtifactAdded,
    AiArtifactUpdated, AiConversationAdded, AiConversationUpdated, AiProjectAdded,
    AiProjectUpdated,
};
use ratatoskr_event_envelope::{EnvelopeError, EventEnvelope, EventPayload};

use crate::{
    BlobStore, Database, DeletionError, SourceInbox, SourceInboxAdmission, SourceInboxError,
    delete_source,
};

/// Result of consuming one at-least-once archive event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveEventAdmission {
    /// A conversation source was claimed for analysis.
    Conversation(SourceInboxAdmission),
    /// A project or Artifact state receipt was retained without analysis scheduling.
    ObjectRecorded,
    /// The object receipt was already retained.
    ObjectDuplicate,
    /// An explicit tombstone removed (or had already removed) derived data.
    Tombstone,
    /// The explicit tombstone was already consumed.
    TombstoneDuplicate,
}

/// Archive event intake failure without source or event contents.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ArchiveEventError {
    /// A typed event envelope could not be decoded.
    #[error("the archive event could not be decoded")]
    Envelope(#[from] EnvelopeError),
    /// A conversation inbox receipt could not be stored.
    #[error("the archive conversation receipt could not be stored")]
    Inbox(#[from] SourceInboxError),
    /// A generic archive object receipt could not be stored.
    #[error("the archive object receipt could not be stored")]
    Persistence(#[from] crate::PersistenceError),
    /// Derived archive state could not be deleted.
    #[error("the archive tombstone could not remove derived data")]
    Deletion(#[from] DeletionError),
    /// The event type is not part of the supported AI-archive lifecycle family.
    #[error("the archive event type is not supported")]
    Unsupported,
}

/// Consumer for published AI-archive normalized lifecycle facts.
#[derive(Debug)]
pub struct ArchiveEventConsumer<'a> {
    database: &'a Database,
    blobs: &'a BlobStore,
}

impl<'a> ArchiveEventConsumer<'a> {
    /// Creates an archive consumer over Knowledge-owned storage and blobs.
    #[must_use]
    pub const fn new(database: &'a Database, blobs: &'a BlobStore) -> Self {
        Self { database, blobs }
    }

    /// Consumes one typed published archive lifecycle envelope.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveEventError`] when the envelope is unsupported, invalid, or its durable
    /// consequence cannot be committed.
    pub async fn accept(
        &self,
        envelope: &EventEnvelope,
    ) -> Result<ArchiveEventAdmission, ArchiveEventError> {
        let event_type = envelope.event_type.to_wire();
        match event_type.as_str() {
            AiArchiveImport::EVENT_TYPE => {
                self.record_import(envelope, &envelope.payload_as::<AiArchiveImport>()?)
                    .await
            }
            AiConversationAdded::EVENT_TYPE | AiConversationUpdated::EVENT_TYPE => {
                Ok(ArchiveEventAdmission::Conversation(
                    SourceInbox::new(self.database)
                        .accept_ai_envelope(envelope)
                        .await?,
                ))
            }
            AiProjectAdded::EVENT_TYPE => {
                self.record_project(envelope, &envelope.payload_as::<AiProjectAdded>()?)
                    .await
            }
            AiProjectUpdated::EVENT_TYPE => {
                self.record_project(envelope, &envelope.payload_as::<AiProjectUpdated>()?)
                    .await
            }
            AiArtifactAdded::EVENT_TYPE => {
                self.record_artifact(envelope, &envelope.payload_as::<AiArtifactAdded>()?)
                    .await
            }
            AiArtifactUpdated::EVENT_TYPE => {
                self.record_artifact(envelope, &envelope.payload_as::<AiArtifactUpdated>()?)
                    .await
            }
            AiArchiveTombstone::EVENT_TYPE => {
                self.apply_tombstone(envelope, &envelope.payload_as::<AiArchiveTombstone>()?)
                    .await
            }
            _ => Err(ArchiveEventError::Unsupported),
        }
    }

    async fn record_import(
        &self,
        envelope: &EventEnvelope,
        payload: &AiArchiveImport,
    ) -> Result<ArchiveEventAdmission, ArchiveEventError> {
        self.record_object(
            envelope,
            payload.owner.to_string(),
            payload.ai_archive_id.to_string(),
            "archive",
            payload.ai_archive_id.to_string(),
            payload,
        )
        .await
    }

    async fn record_project<P: serde::Serialize + ProjectPayload>(
        &self,
        envelope: &EventEnvelope,
        payload: &P,
    ) -> Result<ArchiveEventAdmission, ArchiveEventError> {
        self.record_object(
            envelope,
            payload.owner().to_string(),
            payload.archive_id().to_string(),
            "project",
            payload.project_id().to_string(),
            payload,
        )
        .await
    }

    async fn record_artifact<P: serde::Serialize + ArtifactPayload>(
        &self,
        envelope: &EventEnvelope,
        payload: &P,
    ) -> Result<ArchiveEventAdmission, ArchiveEventError> {
        self.record_object(
            envelope,
            payload.owner().to_string(),
            payload.archive_id().to_string(),
            "artifact",
            payload.artifact_id().to_string(),
            payload,
        )
        .await
    }

    async fn record_object<P: serde::Serialize>(
        &self,
        envelope: &EventEnvelope,
        tenant_ref: String,
        archive_id: String,
        object_kind: &str,
        object_id: String,
        payload: &P,
    ) -> Result<ArchiveEventAdmission, ArchiveEventError> {
        let inserted = sqlx::query_scalar::<_, uuid::Uuid>(
            "insert into knowledge.ai_archive_object_inbox
                 (event_id, subject, tenant_ref, archive_id, object_kind, object_id, observed_at, payload)
             values ($1, $2, $3, $4, $5, $6, $7::timestamptz, $8)
             on conflict (event_id) do nothing returning event_id",
        )
        .bind(envelope.event_id.0)
        .bind(envelope.event_type.to_wire())
        .bind(tenant_ref)
        .bind(archive_id)
        .bind(object_kind)
        .bind(object_id)
        .bind(envelope.occurred_at.to_string())
        .bind(serde_json::to_value(payload).map_err(crate::PersistenceError::Encode)?)
        .fetch_optional(self.database.pool())
        .await
        .map_err(crate::PersistenceError::Query)?;
        Ok(if inserted.is_some() {
            ArchiveEventAdmission::ObjectRecorded
        } else {
            ArchiveEventAdmission::ObjectDuplicate
        })
    }

    async fn apply_tombstone(
        &self,
        envelope: &EventEnvelope,
        tombstone: &AiArchiveTombstone,
    ) -> Result<ArchiveEventAdmission, ArchiveEventError> {
        let (kind, subject_id) = tombstone_subject(&tombstone.subject);
        let inserted = sqlx::query_scalar::<_, uuid::Uuid>(
            "insert into knowledge.ai_archive_tombstones
                 (event_id, tenant_ref, archive_id, subject_kind, subject_id, observed_at)
             values ($1, $2, $3, $4, $5, $6::timestamptz)
             on conflict (event_id) do nothing returning event_id",
        )
        .bind(envelope.event_id.0)
        .bind(tombstone.owner.to_string())
        .bind(tombstone.ai_archive_id.to_string())
        .bind(kind)
        .bind(&subject_id)
        .bind(tombstone.observed_at.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(crate::PersistenceError::Query)?;
        if inserted.is_none() {
            return Ok(ArchiveEventAdmission::TombstoneDuplicate);
        }

        let source_ids = self
            .tombstoned_source_ids(tombstone, kind, subject_id.as_deref())
            .await?;
        for source_id in &source_ids {
            delete_source(
                self.database,
                self.blobs,
                &tombstone.owner.to_string(),
                "ratatoskr-knowledge",
                source_id,
            )
            .await?;
        }
        self.remove_inbox_sources(&tombstone.owner.to_string(), &source_ids)
            .await?;
        Ok(ArchiveEventAdmission::Tombstone)
    }

    async fn tombstoned_source_ids(
        &self,
        tombstone: &AiArchiveTombstone,
        kind: &str,
        subject_id: Option<&str>,
    ) -> Result<Vec<String>, ArchiveEventError> {
        let rows: Vec<(String,)> = match kind {
            "conversation" => {
                sqlx::query_as(
                    "select distinct source_id from knowledge.source_analysis_inbox
                 where family = 'ai_archive' and tenant_ref = $1 and source_id = $2",
                )
                .bind(tombstone.owner.to_string())
                .bind(subject_id.ok_or(ArchiveEventError::Unsupported)?)
                .fetch_all(self.database.pool())
                .await
            }
            "archive" => {
                sqlx::query_as(
                    "select distinct source_id from knowledge.source_analysis_inbox
                 where family = 'ai_archive' and tenant_ref = $1 and archive_id = $2",
                )
                .bind(tombstone.owner.to_string())
                .bind(tombstone.ai_archive_id.to_string())
                .fetch_all(self.database.pool())
                .await
            }
            _ => Ok(Vec::new()),
        }
        .map_err(crate::PersistenceError::Query)?;
        Ok(rows.into_iter().map(|(source_id,)| source_id).collect())
    }

    async fn remove_inbox_sources(
        &self,
        tenant_ref: &str,
        source_ids: &[String],
    ) -> Result<(), ArchiveEventError> {
        if source_ids.is_empty() {
            return Ok(());
        }
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(crate::PersistenceError::Query)?;
        sqlx::query(
            "delete from knowledge.source_analysis_heads
             where family = 'ai_archive' and tenant_ref = $1 and source_id = any($2)",
        )
        .bind(tenant_ref)
        .bind(source_ids)
        .execute(&mut *transaction)
        .await
        .map_err(crate::PersistenceError::Query)?;
        sqlx::query(
            "delete from knowledge.source_analysis_inbox
             where family = 'ai_archive' and tenant_ref = $1 and source_id = any($2)",
        )
        .bind(tenant_ref)
        .bind(source_ids)
        .execute(&mut *transaction)
        .await
        .map_err(crate::PersistenceError::Query)?;
        transaction
            .commit()
            .await
            .map_err(crate::PersistenceError::Query)?;
        Ok(())
    }
}

fn tombstone_subject(subject: &AiArchiveTombstoneSubject) -> (&'static str, Option<String>) {
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

trait ProjectPayload {
    fn archive_id(&self) -> ratatoskr_identifiers::AiArchiveId;
    fn owner(&self) -> ratatoskr_identifiers::TenantRef;
    fn project_id(&self) -> ratatoskr_identifiers::AiProjectId;
}

macro_rules! project_payload {
    ($type:ty) => {
        impl ProjectPayload for $type {
            fn archive_id(&self) -> ratatoskr_identifiers::AiArchiveId {
                self.import_provenance.ai_archive_id
            }
            fn owner(&self) -> ratatoskr_identifiers::TenantRef {
                self.import_provenance.owner
            }
            fn project_id(&self) -> ratatoskr_identifiers::AiProjectId {
                self.project.ai_project_id
            }
        }
    };
}
project_payload!(AiProjectAdded);
project_payload!(AiProjectUpdated);

trait ArtifactPayload {
    fn archive_id(&self) -> ratatoskr_identifiers::AiArchiveId;
    fn owner(&self) -> ratatoskr_identifiers::TenantRef;
    fn artifact_id(&self) -> &ratatoskr_identifiers::EntityLocalId;
}

macro_rules! artifact_payload {
    ($type:ty) => {
        impl ArtifactPayload for $type {
            fn archive_id(&self) -> ratatoskr_identifiers::AiArchiveId {
                self.import_provenance.ai_archive_id
            }
            fn owner(&self) -> ratatoskr_identifiers::TenantRef {
                self.artifact.owner
            }
            fn artifact_id(&self) -> &ratatoskr_identifiers::EntityLocalId {
                &self.artifact.external_artifact_id
            }
        }
    };
}
artifact_payload!(AiArtifactAdded);
artifact_payload!(AiArtifactUpdated);
