//! Auditable privacy deletion over Knowledge-owned rows and bytes.
//!
//! Deletion removes every derived trace of one tenant or one logical
//! source - analyses, attempts, outputs, search documents, embedding
//! chunks, failure records, and owned raw-response bytes - child-first in
//! one transaction with its audit row, then collects blob files by
//! reference. Externally owned provenance bytes are never removed.

use std::collections::HashSet;

use ratatoskr_identifiers::BlobRef;
use uuid::Uuid;

use crate::blob_store::{BlobError, BlobStore};
use crate::database::Database;

/// Scope selector for one deletion operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeletionScope {
    /// Every revision of every document owned by one tenant.
    Tenant {
        /// Owning tenant reference.
        tenant_ref: String,
    },
    /// Every revision of one logical source document.
    Source {
        /// Owning tenant reference.
        tenant_ref: String,
        /// Bounded context that owns the document.
        owner_context: String,
        /// Stable normalized document identity.
        source_document_id: String,
    },
    /// Every derived revision tied to one immutable AI archive snapshot.
    Archive {
        /// Owning tenant reference.
        tenant_ref: String,
        /// Stable archive identity from the producer provenance.
        ai_archive_id: String,
    },
}

impl DeletionScope {
    /// Returns the stable database spelling of the scope kind.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Tenant { .. } => "tenant",
            Self::Source { .. } => "source",
            Self::Archive { .. } => "archive",
        }
    }

    /// Returns the owning tenant reference.
    #[must_use]
    pub fn tenant_ref(&self) -> &str {
        match self {
            Self::Tenant { tenant_ref }
            | Self::Source { tenant_ref, .. }
            | Self::Archive { tenant_ref, .. } => tenant_ref,
        }
    }
}

/// Per-table deleted row counts recorded by one deletion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeletionCounts {
    /// Deleted `knowledge.tags` rows.
    pub tags: u64,
    /// Deleted `knowledge.analysis_taggings` rows.
    pub taggings: u64,
    /// Deleted `knowledge.collections` rows.
    pub collections: u64,
    /// Deleted `knowledge.collection_items` rows.
    pub collection_items: u64,
    /// Deleted `knowledge.analysis_user_states` rows.
    pub analysis_user_states: u64,
    /// Deleted `knowledge.highlights` rows.
    pub highlights: u64,
    /// Deleted `knowledge.analysis_feedback` rows.
    pub analysis_feedback: u64,
    /// Deleted `knowledge.source_refs` revisions.
    pub source_refs: u64,
    /// Deleted `knowledge.analysis_runs` rows.
    pub analysis_runs: u64,
    /// Deleted `knowledge.analysis_attempts` rows.
    pub analysis_attempts: u64,
    /// Deleted `knowledge.analysis_outputs` rows.
    pub analysis_outputs: u64,
    /// Deleted `knowledge.search_documents` rows.
    pub search_documents: u64,
    /// Deleted `knowledge.search_projection_inputs` rows.
    pub search_projection_inputs: u64,
    /// Deleted `knowledge.embedding_chunks` rows.
    pub embedding_chunks: u64,
    /// Deleted `knowledge.embedding_failures` rows.
    pub embedding_failures: u64,
}

/// Machine-readable confirmation returned by one deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionReceipt {
    /// Scope the operation executed for.
    pub scope: DeletionScope,
    /// Per-table deleted row counts.
    pub counts: DeletionCounts,
    /// Owned digests removed because no remaining row references them.
    pub blob_digests_removed: Vec<String>,
    /// Unreferenced digests reclaimed by the sweep phase; accounted
    /// separately from the operation's own scope removals.
    pub orphan_digests_removed: Vec<String>,
    /// Externally owned provenance digests reported out of scope.
    pub external_source_blob_digests: Vec<String>,
}

/// Knowledge deletion failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DeletionError {
    /// A deletion query failed.
    #[error("a deletion database query failed")]
    Query(#[source] sqlx::Error),
    /// A stored blob reference could not be parsed.
    #[error("a stored blob reference is invalid")]
    InvalidBlobReference,
    /// The owned blob store refused an operation.
    #[error("the owned blob collection failed")]
    Blob(#[from] crate::blob_store::BlobError),
    /// A deletion count exceeds its database representation.
    #[error("a deletion count exceeds its database representation")]
    ValueOverflow,
    /// Rows survived a committed deletion.
    #[error("rows survived the committed deletion")]
    VerificationFailed,
}

/// Runs the child-first row deletions and the audit insert inside a
/// caller-supplied transaction.
///
/// A future event consumer performs deletion within its own delivery
/// transaction through this published unit; the committing entry points
/// wrap it.
///
/// # Errors
///
/// Returns [`DeletionError`] when persistence fails.
pub async fn execute_deletion(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
) -> Result<DeletionCounts, DeletionError> {
    run_child_first_deletion(transaction, scope)
        .await
        .map(|(counts, _)| counts)
}

/// Deletes every revision of every document owned by one tenant.
///
/// # Errors
///
/// Returns [`DeletionError`] when persistence, blob collection, or
/// verification fails.
pub async fn delete_tenant(
    database: &Database,
    blobs: &BlobStore,
    tenant_ref: &str,
) -> Result<DeletionReceipt, DeletionError> {
    delete_scope(
        database,
        blobs,
        DeletionScope::Tenant {
            tenant_ref: tenant_ref.to_owned(),
        },
    )
    .await
}

/// Deletes every revision of one logical source document.
///
/// # Errors
///
/// Returns [`DeletionError`] when persistence, blob collection, or
/// verification fails.
pub async fn delete_source(
    database: &Database,
    blobs: &BlobStore,
    tenant_ref: &str,
    owner_context: &str,
    source_document_id: &str,
) -> Result<DeletionReceipt, DeletionError> {
    let scope = DeletionScope::Source {
        tenant_ref: tenant_ref.to_owned(),
        owner_context: owner_context.to_owned(),
        source_document_id: source_document_id.to_owned(),
    };
    delete_scope(database, blobs, scope).await
}

/// Executes one deletion for an explicit scope.
///
/// The row deletions and the audit row commit together; owned response
/// bytes are collected afterwards by reference so surviving rows keep
/// their evidence.
///
/// # Errors
///
/// Returns [`DeletionError`] when persistence, blob collection, or
/// verification fails.
async fn delete_scope(
    database: &Database,
    blobs: &BlobStore,
    scope: DeletionScope,
) -> Result<DeletionReceipt, DeletionError> {
    // The sweep phase runs before the scope is processed so a crash window
    // between an earlier commit and its file collection self-heals here.
    let orphan_digests_removed = sweep_orphaned_blobs(database.pool(), blobs).await?;
    let scope_digests = collect_scope_blob_references(database.pool(), &scope).await?;
    let mut transaction = database
        .pool()
        .begin()
        .await
        .map_err(DeletionError::Query)?;
    let (counts, deletion_id) = run_child_first_deletion(&mut transaction, &scope).await?;
    transaction.commit().await.map_err(DeletionError::Query)?;

    let referenced = collect_referenced_digests(database.pool()).await?;
    let mut blob_digests_removed = Vec::new();
    for digest in &scope_digests.owned {
        if referenced.contains(digest) {
            continue;
        }
        if blobs.remove(digest).await? {
            blob_digests_removed.push(digest.clone());
        }
    }

    // The audit row committed with the row deletions; its blob count is
    // finalized here because collection is only safe after commit.
    finalize_audit_blob_count(database.pool(), deletion_id, blob_digests_removed.len()).await?;

    verify_scope_empty(database.pool(), &scope).await?;
    Ok(DeletionReceipt {
        scope,
        counts,
        blob_digests_removed,
        orphan_digests_removed,
        external_source_blob_digests: scope_digests.external,
    })
}

/// Removes every stored digest that no remaining row references.
///
/// The sweep lists the whole content-addressed root, so a process stop
/// between an earlier commit and its file collection self-heals on the
/// next deletion instead of leaking files permanently. Orphan files are
/// dead weight - never evidence - because the rows referencing them are
/// provably gone.
async fn sweep_orphaned_blobs(
    pool: &sqlx::PgPool,
    blobs: &BlobStore,
) -> Result<Vec<String>, DeletionError> {
    let referenced = collect_referenced_digests(pool).await?;
    let sha_root = blobs.root().join("sha256");
    let mut orphans = Vec::new();
    let Ok(prefix_entries) = tokio::fs::read_dir(&sha_root).await else {
        // A missing blob root simply holds no orphans.
        return Ok(orphans);
    };
    let mut prefixes = prefix_entries;
    while let Some(prefix) = prefixes.next_entry().await.map_err(BlobError::Io)? {
        if !prefix.file_type().await.map_err(BlobError::Io)?.is_dir() {
            continue;
        }
        let mut files = tokio::fs::read_dir(prefix.path())
            .await
            .map_err(BlobError::Io)?;
        while let Some(file) = files.next_entry().await.map_err(BlobError::Io)? {
            let name = file.file_name().to_string_lossy().into_owned();
            if !crate::blob_store::is_digest_hex(&name) || referenced.contains(&name) {
                continue;
            }
            if blobs.remove(&name).await? {
                orphans.push(name);
            }
        }
    }
    Ok(orphans)
}

/// Records the exact removed-digest count on the committed audit row.
async fn finalize_audit_blob_count(
    pool: &sqlx::PgPool,
    deletion_id: Uuid,
    removed_count: usize,
) -> Result<(), DeletionError> {
    sqlx::query(
        "update knowledge.deletion_records set blob_digests_removed = $2 where deletion_id = $1",
    )
    .bind(deletion_id)
    .bind(i32::try_from(removed_count).map_err(|_| DeletionError::ValueOverflow)?)
    .execute(pool)
    .await
    .map_err(DeletionError::Query)?;
    Ok(())
}

/// Externally owned and Knowledge-owned digest references of one scope.
#[derive(Debug, Default)]
struct ScopeBlobReferences {
    /// Digests of raw responses stored in the Knowledge-owned root.
    owned: Vec<String>,
    /// Digests addressed by `source_refs.source_blob` and owned by the
    /// source-owning service; never removed by this module.
    external: Vec<String>,
}

/// Captures the blob digests the scope's rows reference today.
async fn collect_scope_blob_references(
    pool: &sqlx::PgPool,
    scope: &DeletionScope,
) -> Result<ScopeBlobReferences, DeletionError> {
    let mut references = ScopeBlobReferences::default();
    let response_rows: Vec<(Option<String>,)> = sqlx::query_as(
        "select a.raw_response::text as reference
         from knowledge.analysis_attempts a
         join knowledge.analysis_runs r on r.run_id = a.run_id
         join knowledge.source_refs s on s.source_ref_id = r.source_ref_id
         where a.raw_response is not null and (s.source_ref_id = any($1))
         union all
         select o.raw_response::text
         from knowledge.analysis_outputs o
         join knowledge.analysis_runs r on r.run_id = o.run_id
         join knowledge.source_refs s on s.source_ref_id = r.source_ref_id
         where s.source_ref_id = any($1)",
    )
    .bind(scope_source_ids(pool, scope).await?)
    .fetch_all(pool)
    .await
    .map_err(DeletionError::Query)?;
    for (value,) in response_rows {
        let Some(value) = value else {
            continue;
        };
        references.owned.push(parse_reference_digest(&value)?);
    }

    let source_blobs: Vec<(String,)> = sqlx::query_as(
        "select source_blob::text from knowledge.source_refs where source_ref_id = any($1)",
    )
    .bind(scope_source_ids(pool, scope).await?)
    .fetch_all(pool)
    .await
    .map_err(DeletionError::Query)?;
    for (value,) in source_blobs {
        references.external.push(parse_reference_digest(&value)?);
    }
    Ok(references)
}

/// Resolves the stored source revisions of one scope.
async fn scope_source_ids<'e, E>(
    executor: E,
    scope: &DeletionScope,
) -> Result<Vec<Uuid>, DeletionError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let ids: Vec<(Uuid,)> = match scope {
        DeletionScope::Tenant { tenant_ref } => {
            sqlx::query_as("select source_ref_id from knowledge.source_refs where tenant_ref = $1")
                .bind(tenant_ref)
                .fetch_all(executor)
        }
        DeletionScope::Source {
            tenant_ref,
            owner_context,
            source_document_id,
        } => sqlx::query_as(
            "select source_ref_id from knowledge.source_refs
             where tenant_ref = $1 and owner_context = $2 and source_document_id = $3",
        )
        .bind(tenant_ref)
        .bind(owner_context)
        .bind(source_document_id)
        .fetch_all(executor),
        DeletionScope::Archive {
            tenant_ref,
            ai_archive_id,
        } => sqlx::query_as(
            "select source_ref_id from knowledge.source_refs
             where tenant_ref = $1 and ai_archive_id = $2",
        )
        .bind(tenant_ref)
        .bind(ai_archive_id)
        .fetch_all(executor),
    }
    .await
    .map_err(DeletionError::Query)?;
    Ok(ids.into_iter().map(|(id,)| id).collect())
}

/// Parses one stored `BlobRef` jsonb text into its digest hex.
fn parse_reference_digest(text: &str) -> Result<String, DeletionError> {
    let reference: BlobRef =
        serde_json::from_str(text).map_err(|_| DeletionError::InvalidBlobReference)?;
    Ok(reference.digest.hex.as_str().to_owned())
}

/// Extracts every digest still referenced by any attempt or output row.
async fn collect_referenced_digests(pool: &sqlx::PgPool) -> Result<HashSet<String>, DeletionError> {
    let rows: Vec<(Option<String>,)> = sqlx::query_as(
        "select raw_response::text from knowledge.analysis_attempts
         where raw_response is not null
         union all
         select raw_response::text from knowledge.analysis_outputs",
    )
    .fetch_all(pool)
    .await
    .map_err(DeletionError::Query)?;
    let mut digests = HashSet::new();
    for (value,) in rows {
        let Some(value) = value else {
            continue;
        };
        digests.insert(parse_reference_digest(&value)?);
    }
    Ok(digests)
}

/// Runs every child-first deletion statement plus the audit insert inside
/// the caller's transaction and reports each statement's affected rows
/// together with the audit row's identity.
async fn run_child_first_deletion(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
) -> Result<(DeletionCounts, Uuid), DeletionError> {
    let source_ref_ids = scope_source_ids(&mut **transaction, scope).await?;
    let user_content = delete_user_content(&mut *transaction, scope, &source_ref_ids).await?;

    // Child-first order respects the NO ACTION foreign keys:
    // embedding_failures -> embedding_chunks -> search_documents ->
    // search_projection_inputs ->
    // analysis_attempts -> analysis_outputs -> analysis_runs -> source_refs.
    let embedding_failures = delete_by_scope(
        &mut *transaction,
        scope,
        &source_ref_ids,
        "embedding_failures",
    )
    .await?;
    let embedding_chunks = delete_by_scope(
        &mut *transaction,
        scope,
        &source_ref_ids,
        "embedding_chunks",
    )
    .await?;
    let search_documents = delete_by_scope(
        &mut *transaction,
        scope,
        &source_ref_ids,
        "search_documents",
    )
    .await?;
    let search_projection_inputs = delete_by_scope(
        &mut *transaction,
        scope,
        &source_ref_ids,
        "search_projection_inputs",
    )
    .await?;
    let analysis_attempts = sqlx::query(
        "delete from knowledge.analysis_attempts
         where run_id in (
             select run_id from knowledge.analysis_runs
             where source_ref_id = any($1)
         )",
    )
    .bind(&source_ref_ids)
    .execute(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?
    .rows_affected();
    let analysis_outputs = sqlx::query(
        "delete from knowledge.analysis_outputs
         where run_id in (
             select run_id from knowledge.analysis_runs
             where source_ref_id = any($1)
         )",
    )
    .bind(&source_ref_ids)
    .execute(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?
    .rows_affected();
    let analysis_runs =
        sqlx::query("delete from knowledge.analysis_runs where source_ref_id = any($1)")
            .bind(&source_ref_ids)
            .execute(&mut **transaction)
            .await
            .map_err(DeletionError::Query)?
            .rows_affected();
    let source_refs =
        sqlx::query("delete from knowledge.source_refs where source_ref_id = any($1)")
            .bind(&source_ref_ids)
            .execute(&mut **transaction)
            .await
            .map_err(DeletionError::Query)?
            .rows_affected();

    let counts = DeletionCounts {
        tags: user_content.tags,
        taggings: user_content.taggings,
        collections: user_content.collections,
        collection_items: user_content.collection_items,
        analysis_user_states: user_content.analysis_user_states,
        highlights: user_content.highlights,
        analysis_feedback: user_content.analysis_feedback,
        source_refs,
        analysis_runs,
        analysis_attempts,
        analysis_outputs,
        search_documents,
        search_projection_inputs,
        embedding_chunks,
        embedding_failures,
    };
    let deletion_id = insert_audit_row(&mut *transaction, scope, &counts).await?;
    Ok((counts, deletion_id))
}

#[derive(Default)]
struct UserContentDeletion {
    tags: u64,
    taggings: u64,
    collections: u64,
    collection_items: u64,
    analysis_user_states: u64,
    highlights: u64,
    analysis_feedback: u64,
}

async fn delete_user_content(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
    source_ref_ids: &[Uuid],
) -> Result<UserContentDeletion, DeletionError> {
    let output_ids: Vec<Uuid> = sqlx::query_scalar(
        "select o.output_id from knowledge.analysis_outputs o
         join knowledge.analysis_runs r on r.run_id = o.run_id
         where r.source_ref_id = any($1)",
    )
    .bind(source_ref_ids)
    .fetch_all(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?;
    Ok(UserContentDeletion {
        taggings: delete_output_children(transaction, scope, &output_ids, "analysis_taggings")
            .await?,
        analysis_user_states: delete_output_children(
            transaction,
            scope,
            &output_ids,
            "analysis_user_states",
        )
        .await?,
        highlights: delete_output_children(transaction, scope, &output_ids, "highlights").await?,
        analysis_feedback: delete_output_children(
            transaction,
            scope,
            &output_ids,
            "analysis_feedback",
        )
        .await?,
        collection_items: delete_collection_items(transaction, scope, source_ref_ids, &output_ids)
            .await?,
        collections: delete_tenant_only(transaction, scope, "collections").await?,
        tags: delete_tenant_only(transaction, scope, "tags").await?,
    })
}

async fn delete_output_children(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
    output_ids: &[Uuid],
    table: &str,
) -> Result<u64, DeletionError> {
    let result = match scope {
        DeletionScope::Tenant { tenant_ref } => {
            let sql = format!("delete from knowledge.{table} where tenant_ref = $1");
            sqlx::query(&sql)
                .bind(tenant_ref)
                .execute(&mut **transaction)
                .await
        }
        DeletionScope::Source { .. } | DeletionScope::Archive { .. } => {
            let sql = format!("delete from knowledge.{table} where output_id = any($1)");
            sqlx::query(&sql)
                .bind(output_ids)
                .execute(&mut **transaction)
                .await
        }
    };
    result
        .map_err(DeletionError::Query)
        .map(|result| result.rows_affected())
}

async fn delete_collection_items(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
    source_ref_ids: &[Uuid],
    output_ids: &[Uuid],
) -> Result<u64, DeletionError> {
    let result = match scope {
        DeletionScope::Tenant { tenant_ref } => {
            sqlx::query("delete from knowledge.collection_items where tenant_ref = $1")
                .bind(tenant_ref)
                .execute(&mut **transaction)
                .await
        }
        DeletionScope::Source { .. } | DeletionScope::Archive { .. } => {
            sqlx::query(
                "delete from knowledge.collection_items
             where source_ref_id = any($1) or output_id = any($2)",
            )
            .bind(source_ref_ids)
            .bind(output_ids)
            .execute(&mut **transaction)
            .await
        }
    };
    result
        .map_err(DeletionError::Query)
        .map(|result| result.rows_affected())
}

async fn delete_tenant_only(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
    table: &str,
) -> Result<u64, DeletionError> {
    let DeletionScope::Tenant { tenant_ref } = scope else {
        return Ok(0);
    };
    let sql = format!("delete from knowledge.{table} where tenant_ref = $1");
    sqlx::query(&sql)
        .bind(tenant_ref)
        .execute(&mut **transaction)
        .await
        .map_err(DeletionError::Query)
        .map(|result| result.rows_affected())
}

/// Deletes one tenant-and-source keyed table for the scope.
async fn delete_by_scope(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
    source_ref_ids: &[Uuid],
    table: &str,
) -> Result<u64, DeletionError> {
    // A tenant scope removes by direct tenant attribution; a source scope
    // must not touch sibling documents of the same tenant, so it removes
    // strictly by captured revision identifiers.
    let result = match scope {
        DeletionScope::Tenant { tenant_ref } => {
            let sql = format!("delete from knowledge.{table} where tenant_ref = $1");
            sqlx::query(&sql)
                .bind(tenant_ref)
                .execute(&mut **transaction)
                .await
        }
        DeletionScope::Source { .. } | DeletionScope::Archive { .. } => {
            let sql = format!("delete from knowledge.{table} where source_ref_id = any($1)");
            sqlx::query(&sql)
                .bind(source_ref_ids)
                .execute(&mut **transaction)
                .await
        }
    };
    result
        .map_err(DeletionError::Query)
        .map(|query_result| query_result.rows_affected())
}

/// Inserts the audit record inside the deletion's own transaction and
/// returns its identity.
async fn insert_audit_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
    counts: &DeletionCounts,
) -> Result<Uuid, DeletionError> {
    let deletion_id: (Uuid,) = sqlx::query_as(
        "insert into knowledge.deletion_records (
             deletion_id, tenant_ref, scope, owner_context, ai_archive_id, source_document_id,
             source_refs_deleted, analysis_runs_deleted, analysis_attempts_deleted,
             analysis_outputs_deleted, search_projection_inputs_deleted,
             search_documents_deleted,
             embedding_chunks_deleted, embedding_failures_deleted,
             tags_deleted, taggings_deleted, collections_deleted, collection_items_deleted,
             analysis_user_states_deleted, highlights_deleted, analysis_feedback_deleted,
             blob_digests_removed
         ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                   $17, $18, $19, $20, 0)
         returning deletion_id",
    )
    .bind(Uuid::now_v7())
    .bind(scope.tenant_ref())
    .bind(scope.as_str())
    .bind(scope_owner_context(scope))
    .bind(scope_ai_archive_id(scope))
    .bind(scope_source_document_id(scope))
    .bind(i32_count(counts.source_refs)?)
    .bind(i32_count(counts.analysis_runs)?)
    .bind(i32_count(counts.analysis_attempts)?)
    .bind(i32_count(counts.analysis_outputs)?)
    .bind(i32_count(counts.search_projection_inputs)?)
    .bind(i32_count(counts.search_documents)?)
    .bind(i32_count(counts.embedding_chunks)?)
    .bind(i32_count(counts.embedding_failures)?)
    .bind(i32_count(counts.tags)?)
    .bind(i32_count(counts.taggings)?)
    .bind(i32_count(counts.collections)?)
    .bind(i32_count(counts.collection_items)?)
    .bind(i32_count(counts.analysis_user_states)?)
    .bind(i32_count(counts.highlights)?)
    .bind(i32_count(counts.analysis_feedback)?)
    .fetch_one(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?;
    Ok(deletion_id.0)
}

fn scope_owner_context(scope: &DeletionScope) -> Option<&str> {
    match scope {
        DeletionScope::Tenant { .. } | DeletionScope::Archive { .. } => None,
        DeletionScope::Source { owner_context, .. } => Some(owner_context),
    }
}

fn scope_ai_archive_id(scope: &DeletionScope) -> Option<&str> {
    match scope {
        DeletionScope::Archive { ai_archive_id, .. } => Some(ai_archive_id),
        DeletionScope::Tenant { .. } | DeletionScope::Source { .. } => None,
    }
}

fn scope_source_document_id(scope: &DeletionScope) -> Option<&str> {
    match scope {
        DeletionScope::Tenant { .. } | DeletionScope::Archive { .. } => None,
        DeletionScope::Source {
            source_document_id, ..
        } => Some(source_document_id),
    }
}

fn i32_count(count: u64) -> Result<i32, DeletionError> {
    i32::try_from(count).map_err(|_| DeletionError::ValueOverflow)
}

/// Fails loudly when any row of the scope survived the committed deletion.
async fn verify_scope_empty(
    pool: &sqlx::PgPool,
    scope: &DeletionScope,
) -> Result<(), DeletionError> {
    let survivors = scope_source_ids(pool, scope).await?;
    if !survivors.is_empty() {
        return Err(DeletionError::VerificationFailed);
    }
    let DeletionScope::Tenant { tenant_ref } = scope else {
        return Ok(());
    };
    let (stray,): (i64,) = sqlx::query_as(
        "select count(*) from (
             select 1 from knowledge.embedding_failures where tenant_ref = $1
             union all
             select 1 from knowledge.embedding_chunks where tenant_ref = $1
             union all
             select 1 from knowledge.search_projection_inputs where tenant_ref = $1
             union all
             select 1 from knowledge.search_documents where tenant_ref = $1
             union all
             select 1 from knowledge.tags where tenant_ref = $1
             union all
             select 1 from knowledge.collections where tenant_ref = $1
             union all
             select 1 from knowledge.collection_items where tenant_ref = $1
             union all
             select 1 from knowledge.analysis_user_states where tenant_ref = $1
             union all
             select 1 from knowledge.highlights where tenant_ref = $1
             union all
             select 1 from knowledge.analysis_feedback where tenant_ref = $1
         ) stray",
    )
    .bind(tenant_ref)
    .fetch_one(pool)
    .await
    .map_err(DeletionError::Query)?;
    if stray > 0 {
        return Err(DeletionError::VerificationFailed);
    }
    Ok(())
}
