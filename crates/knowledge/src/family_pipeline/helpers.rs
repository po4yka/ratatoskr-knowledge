use ratatoskr_github_contracts::RepositoryAnalysisRequested;
use ratatoskr_identifiers::{BlobRef, ContentDigest, DigestAlgorithm, DigestHex};
use sha2::{Digest as _, Sha256};

use super::{FamilyPipelineError, RepositoryReadmeError};

pub(super) fn repository_digest(
    request: &RepositoryAnalysisRequested,
) -> Result<ContentDigest, FamilyPipelineError> {
    let bytes = serde_json::to_vec(&(
        request.source_revision.clone(),
        request.repository_attributes.clone(),
    ))?;
    let hex = format!("{:x}", Sha256::digest(bytes));
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&hex).map_err(|_| FamilyPipelineError::Source)?,
    })
}

pub(super) fn verify_readme(
    reference: &BlobRef,
    bytes: &[u8],
) -> Result<(), RepositoryReadmeError> {
    if reference.owner_service.as_str() != "ratatoskr-github"
        || reference.media_type.as_str() != "text/markdown"
        || reference.length_bytes
            != u64::try_from(bytes.len()).map_err(|_| RepositoryReadmeError::Integrity)?
        || !matches!(reference.digest.algorithm, DigestAlgorithm::Sha256)
        || format!("{:x}", Sha256::digest(bytes)) != reference.digest.hex.as_str()
    {
        return Err(RepositoryReadmeError::Integrity);
    }
    Ok(())
}
