//! Durable collection of Knowledge-owned blobs whose last reference was deleted (XR-021
//! CONTRACTS.md S12).
//!
//! A deletion never walks the blob root. It writes the Knowledge-owned digests of the rows it
//! deletes into `knowledge.blob_erasure_queue` inside the same transaction as the row deletions,
//! and a drain removes a queued digest only when no row of any blob-bearing column references it
//! any more. A crash between the commit and the drain therefore leaves a queue row, never an
//! orphaned or wrongly deleted file, and bytes that were stored before their row was registered
//! are not on the queue and are never touched.
//!
//! The re-check and the erase run under the per-digest lock of `blob_registry`, which every
//! registrar of a Knowledge-owned blob reference takes too, so a reference registered while a
//! digest is being collected either commits first (and the digest is kept) or waits, finds the
//! bytes gone and stores them again.

use std::collections::HashSet;

use uuid::Uuid;

use crate::blob_registry::blob_lock;
use crate::{BlobStore, Database, DeletionError};

const KNOWLEDGE_OWNER: &str = "ratatoskr-knowledge";

/// Digests an in-flight deletion reports for its receipt.
#[derive(Debug, Default)]
pub(crate) struct QueuedBlobs {
    /// Digests addressed by `source_blob` and owned by another service; never queued.
    pub(crate) external: Vec<String>,
}

/// Queues the Knowledge-owned digests of `source_ref_ids` inside the deletion's transaction.
///
/// Raw responses of the scope's attempts and outputs and Knowledge-owned source snapshots are
/// queued; a `source_blob` owned by another service is only reported.
pub(crate) async fn queue_scope_blobs(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source_ref_ids: &[Uuid],
    deletion_id: Uuid,
) -> Result<QueuedBlobs, DeletionError> {
    let owned: Vec<String> = sqlx::query_scalar(
        "select digest_hex from (
             select a.raw_response -> 'digest' ->> 'hex' as digest_hex
             from knowledge.analysis_attempts a
             join knowledge.analysis_runs r on r.run_id = a.run_id
             where r.source_ref_id = any($1) and a.raw_response ->> 'owner_service' = $2
             union
             select o.raw_response -> 'digest' ->> 'hex'
             from knowledge.analysis_outputs o
             join knowledge.analysis_runs r on r.run_id = o.run_id
             where r.source_ref_id = any($1) and o.raw_response ->> 'owner_service' = $2
             union
             select s.source_blob -> 'digest' ->> 'hex'
             from knowledge.source_refs s
             where s.source_ref_id = any($1) and s.source_blob ->> 'owner_service' = $2
         ) owned where digest_hex is not null order by digest_hex",
    )
    .bind(source_ref_ids)
    .bind(KNOWLEDGE_OWNER)
    .fetch_all(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?;
    if !owned.is_empty() {
        sqlx::query(
            "insert into knowledge.blob_erasure_queue (digest_hex, deletion_id)
             select digest_hex, $2::uuid from unnest($1::text[]) as queued(digest_hex)
             on conflict (digest_hex) do update set deletion_id = excluded.deletion_id",
        )
        .bind(&owned)
        .bind(deletion_id)
        .execute(&mut **transaction)
        .await
        .map_err(DeletionError::Query)?;
    }
    let external: Vec<String> = sqlx::query_scalar(
        "select distinct source_blob -> 'digest' ->> 'hex'
         from knowledge.source_refs
         where source_ref_id = any($1) and source_blob ->> 'owner_service' <> $2
         order by 1",
    )
    .bind(source_ref_ids)
    .bind(KNOWLEDGE_OWNER)
    .fetch_all(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?;
    Ok(QueuedBlobs { external })
}

/// The subset of `digests` that a row of any blob-bearing column still references.
///
/// This is the single reference check of the collector: attempt, output and channel-recap raw
/// responses, and Knowledge-owned source snapshots.
pub(crate) async fn referenced_owned_digests(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    digests: &[String],
) -> Result<HashSet<String>, DeletionError> {
    let referenced: Vec<String> = sqlx::query_scalar(
        "select digest_hex from (
             select raw_response -> 'digest' ->> 'hex' as digest_hex
             from knowledge.analysis_attempts where raw_response is not null
             union
             select raw_response -> 'digest' ->> 'hex' from knowledge.analysis_outputs
             union
             select raw_response -> 'digest' ->> 'hex'
             from knowledge.channel_recap_attempts where raw_response is not null
             union
             select source_blob -> 'digest' ->> 'hex'
             from knowledge.source_refs where source_blob ->> 'owner_service' = $2
         ) referenced where digest_hex = any($1)",
    )
    .bind(digests)
    .bind(KNOWLEDGE_OWNER)
    .fetch_all(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?;
    Ok(referenced.into_iter().collect())
}

/// Removes every queued digest that no remaining row references.
///
/// Each digest is handled in its own transaction: the digest lock is taken, the queue row is
/// locked, a digest a surviving row still references is only dropped from the queue, and any other
/// digest is removed from the blob root (idempotently) and then from the queue, all before the
/// commit releases the digest lock. Safe to run at any time and from several processes. Returns
/// the removed digests.
///
/// # Errors
///
/// Returns [`DeletionError`] when the queue cannot be read or a blob cannot be removed.
pub async fn collect_pending_blobs(
    database: &Database,
    blobs: &BlobStore,
) -> Result<Vec<String>, DeletionError> {
    Ok(drain_queue(database, blobs)
        .await?
        .into_iter()
        .map(|(digest, _)| digest)
        .collect())
}

/// Drains the queue and reports which deletion queued each removed digest.
pub(crate) async fn drain_queue(
    database: &Database,
    blobs: &BlobStore,
) -> Result<Vec<(String, Uuid)>, DeletionError> {
    let queued: Vec<String> = sqlx::query_scalar(
        "select digest_hex from knowledge.blob_erasure_queue order by enqueued_at, digest_hex",
    )
    .fetch_all(database.pool())
    .await
    .map_err(DeletionError::Query)?;
    let mut removed = Vec::new();
    for digest in queued {
        let mut transaction = database
            .pool()
            .begin()
            .await
            .map_err(DeletionError::Query)?;
        blob_lock(&mut transaction, &digest)
            .await
            .map_err(DeletionError::Query)?;
        let locked: Option<Uuid> = sqlx::query_scalar(
            "select deletion_id from knowledge.blob_erasure_queue
             where digest_hex = $1 for update",
        )
        .bind(&digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(DeletionError::Query)?;
        let Some(deletion_id) = locked else {
            transaction.commit().await.map_err(DeletionError::Query)?;
            continue;
        };
        let still_referenced =
            referenced_owned_digests(&mut transaction, std::slice::from_ref(&digest))
                .await?
                .contains(&digest);
        let collected = if still_referenced {
            false
        } else {
            blobs.remove(&digest).await?
        };
        sqlx::query("delete from knowledge.blob_erasure_queue where digest_hex = $1")
            .bind(&digest)
            .execute(&mut *transaction)
            .await
            .map_err(DeletionError::Query)?;
        transaction.commit().await.map_err(DeletionError::Query)?;
        if collected {
            removed.push((digest, deletion_id));
        }
    }
    Ok(removed)
}
