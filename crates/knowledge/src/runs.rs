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

/// Complete immutable identity of one analysis run.
#[derive(Debug, Clone)]
pub struct AnalysisIdentity {
    /// Stored source revision.
    pub source_revision_id: Uuid,
    /// Typed result contract version.
    pub contract_version: String,
    /// Fixed prompt version.
    pub prompt_version: String,
    /// Deterministic context-builder version.
    pub context_builder_version: String,
    /// Provider-neutral model policy identity.
    pub model_policy: String,
}

/// Stored analysis run identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalysisRun {
    /// Knowledge-owned run identity.
    pub id: Uuid,
}

impl Database {
    /// Returns the existing run for a complete identity or creates it once.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when identity validation or persistence fails.
    pub async fn create_run(
        &self,
        identity: &AnalysisIdentity,
    ) -> Result<AnalysisRun, PersistenceError> {
        for value in [
            &identity.contract_version,
            &identity.prompt_version,
            &identity.context_builder_version,
            &identity.model_policy,
        ] {
            validate_version(value)?;
        }
        let id = Uuid::now_v7();
        let stored_id = sqlx::query_scalar(
            "insert into knowledge.analysis_runs (
                run_id, source_ref_id, contract_version, prompt_version,
                context_builder_version, model_policy
             ) values ($1, $2, $3, $4, $5, $6)
             on conflict (
                source_ref_id, contract_version, prompt_version,
                context_builder_version, model_policy
             ) do update set run_id = knowledge.analysis_runs.run_id
             returning run_id",
        )
        .bind(id)
        .bind(identity.source_revision_id)
        .bind(&identity.contract_version)
        .bind(&identity.prompt_version)
        .bind(&identity.context_builder_version)
        .bind(&identity.model_policy)
        .fetch_one(self.pool())
        .await
        .map_err(PersistenceError::Query)?;
        Ok(AnalysisRun { id: stored_id })
    }

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

fn validate_version(value: &str) -> Result<(), PersistenceError> {
    let mut characters = value.chars();
    let starts_correctly = characters
        .next()
        .is_some_and(|character| character.is_ascii_lowercase());
    let rest_is_valid = characters.all(|character| {
        character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '_' | '-')
    });
    if starts_correctly && rest_is_valid && value.len() <= 64 {
        Ok(())
    } else {
        Err(PersistenceError::InvalidAnalysisIdentity)
    }
}
