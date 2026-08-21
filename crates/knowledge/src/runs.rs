use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DocumentId, TenantRef,
};
use uuid::Uuid;

use crate::{Database, PersistenceError};

/// Immutable source evidence presented to Knowledge.
#[derive(Debug, Clone)]
pub struct SourceReference {
    /// Authorization owner of the source.
    pub tenant: TenantRef,
    /// Bounded context that owns the source document.
    pub owner_context: String,
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

impl Database {
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
                source_ref_id, tenant_ref, owner_context, source_document_id,
                content_digest_algorithm, content_digest_hex, source_blob
             ) values ($1, $2, $3, $4, $5, $6, $7)
             on conflict (
                tenant_ref, owner_context, source_document_id,
                content_digest_algorithm, content_digest_hex
             ) do update set source_ref_id = knowledge.source_refs.source_ref_id
             returning source_ref_id",
        )
        .bind(id)
        .bind(source.tenant.to_string())
        .bind(owner.as_str())
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
