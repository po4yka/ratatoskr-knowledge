use std::fmt::Write as _;
use std::path::PathBuf;

use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, MediaType,
};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;
use uuid::Uuid;

/// Knowledge-owned blob persistence failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlobError {
    /// Blob storage is not available yet.
    #[error("the owned blob store is unavailable")]
    Unavailable,
    /// Raw response exceeds the configured byte limit.
    #[error("the raw response exceeds the configured byte limit")]
    TooLarge,
    /// Owned filesystem operation failed.
    #[error("the owned blob operation failed")]
    Io(#[source] std::io::Error),
    /// A generated contract value is invalid.
    #[error("the owned blob reference is invalid")]
    InvalidReference,
}

/// Finite Knowledge-owned content-addressed blob root.
#[derive(Debug, Clone)]
pub struct BlobStore {
    root: PathBuf,
    max_bytes: u64,
}

impl BlobStore {
    /// Creates a bounded store rooted on its owner's durable device.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            root: root.into(),
            max_bytes,
        }
    }

    /// Stores raw provider bytes before validation.
    ///
    /// # Errors
    ///
    /// Returns [`BlobError`] for a size, filesystem, or contract failure.
    pub async fn store_raw(&self, bytes: &[u8]) -> Result<BlobRef, BlobError> {
        let length = u64::try_from(bytes.len()).map_err(|_| BlobError::TooLarge)?;
        if length > self.max_bytes {
            return Err(BlobError::TooLarge);
        }
        let hex = digest_hex(bytes)?;
        let path = self.path(&hex)?;
        if !tokio::fs::try_exists(&path).await.map_err(BlobError::Io)? {
            let parent = path.parent().ok_or(BlobError::InvalidReference)?;
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(BlobError::Io)?;
            let temporary = parent.join(format!(".{hex}.{}.tmp", Uuid::now_v7()));
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await
                .map_err(BlobError::Io)?;
            file.write_all(bytes).await.map_err(BlobError::Io)?;
            file.sync_all().await.map_err(BlobError::Io)?;
            drop(file);
            if let Err(error) = tokio::fs::rename(&temporary, &path).await {
                let _ignored = tokio::fs::remove_file(&temporary).await;
                if !tokio::fs::try_exists(&path).await.map_err(BlobError::Io)? {
                    return Err(BlobError::Io(error));
                }
            }
        }
        Ok(BlobRef {
            owner_service: BlobOwner::parse("ratatoskr-knowledge")
                .map_err(|_| BlobError::InvalidReference)?,
            digest: ContentDigest {
                algorithm: DigestAlgorithm::Sha256,
                hex: DigestHex::parse(&hex).map_err(|_| BlobError::InvalidReference)?,
            },
            media_type: MediaType::parse("application/json")
                .map_err(|_| BlobError::InvalidReference)?,
            length_bytes: length,
        })
    }

    /// Reads owned bytes by reference for authorized internal use.
    ///
    /// # Errors
    ///
    /// Returns [`BlobError`] when ownership, integrity, or I/O validation fails.
    pub async fn read(&self, reference: &BlobRef) -> Result<Vec<u8>, BlobError> {
        if reference.owner_service.as_str() != "ratatoskr-knowledge"
            || reference.media_type.as_str() != "application/json"
            || reference.length_bytes > self.max_bytes
            || !matches!(reference.digest.algorithm, DigestAlgorithm::Sha256)
        {
            return Err(BlobError::InvalidReference);
        }
        let bytes = tokio::fs::read(self.path(reference.digest.hex.as_str())?)
            .await
            .map_err(BlobError::Io)?;
        let length = u64::try_from(bytes.len()).map_err(|_| BlobError::TooLarge)?;
        if length != reference.length_bytes || digest_hex(&bytes)? != reference.digest.hex.as_str()
        {
            return Err(BlobError::InvalidReference);
        }
        Ok(bytes)
    }

    fn path(&self, hex: &str) -> Result<PathBuf, BlobError> {
        let prefix = hex.get(..2).ok_or(BlobError::InvalidReference)?;
        Ok(self.root.join("sha256").join(prefix).join(hex))
    }
}

fn digest_hex(bytes: &[u8]) -> Result<String, BlobError> {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        write!(&mut hex, "{byte:02x}").map_err(|_| BlobError::InvalidReference)?;
    }
    Ok(hex)
}
