//! One lock protocol for registering and erasing Knowledge-owned blob references (XR-021
//! CONTRACTS.md R2-07).
//!
//! The blob store is content-addressed and shared by every tenant, so two actors can race on one
//! digest: an eraser that re-checks "is this digest still referenced" and then erases, and a
//! registrar that stored identical bytes (a no-op because they exist) and registers a reference
//! afterwards. Both take the same per-digest advisory lock, inside their own transaction:
//!
//! - the eraser takes it, re-checks every reference column, then erases and drops its queue row
//!   and commits while still holding it (`deletion_blobs`);
//! - the registrar takes it before the statement that first references the digest, verifies the
//!   blob is present, stores the bytes again when an eraser won and it still holds them, and
//!   otherwise fails without registering anything.
//!
//! The invariant is that no committed row references a missing blob.
//!
//! Acquisition order, so there is one order and no cycle: the tenant lock (where a flow takes one)
//! first, then digest locks in ascending digest order when a transaction needs several, then row
//! locks. A transaction here never holds more than one digest lock.

use ratatoskr_identifiers::BlobRef;
use sqlx::{Postgres, Transaction};

use crate::{BlobError, BlobStore, PersistenceError};

/// Takes the digest lock for `digest_hex` until the transaction ends.
pub(crate) async fn blob_lock(
    transaction: &mut Transaction<'_, Postgres>,
    digest_hex: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("select pg_advisory_xact_lock(hashtextextended('blob:' || $1, 0))")
        .bind(digest_hex)
        .execute(&mut **transaction)
        .await
        .map(|_| ())
}

/// The registrar's half of the protocol: lock the digest, then make sure the blob exists.
///
/// `held` is the bytes the caller still has in memory. When the blob is missing because an eraser
/// won the race they are stored again; without them, or when storing fails, the registration
/// fails with a retryable [`BlobError`] and the caller's transaction must roll back.
///
/// # Errors
///
/// Returns [`PersistenceError`] when the lock cannot be taken or the blob cannot be made present.
pub(crate) async fn lock_and_verify(
    transaction: &mut Transaction<'_, Postgres>,
    blobs: &BlobStore,
    reference: &BlobRef,
    held: Option<&[u8]>,
) -> Result<(), PersistenceError> {
    let digest = reference.digest.hex.as_str();
    blob_lock(transaction, digest)
        .await
        .map_err(PersistenceError::Query)?;
    if blobs.exists(digest).await.map_err(PersistenceError::Blob)? {
        return Ok(());
    }
    let bytes = held.ok_or(PersistenceError::Blob(BlobError::Unavailable))?;
    let stored = blobs
        .store_raw(bytes)
        .await
        .map_err(PersistenceError::Blob)?;
    if stored.digest.hex.as_str() == digest {
        Ok(())
    } else {
        Err(PersistenceError::Blob(BlobError::InvalidReference))
    }
}
