//! Auditable privacy deletion over Knowledge-owned rows and bytes.
//!
//! Deletion removes every derived trace of one tenant or one logical
//! source - analyses, attempts, outputs, search documents, embedding
//! chunks, failure records, inbox and tombstone state, and owned
//! raw-response bytes - child-first in one transaction with its audit row.
//! The Knowledge-owned digests of the deleted rows are queued in that same
//! transaction and collected afterwards by reference. Externally owned
//! provenance bytes are never removed.

use uuid::Uuid;

use crate::blob_store::BlobStore;
use crate::database::Database;
use crate::deletion_audit::insert_audit_row;
use crate::deletion_blobs::{drain_queue, queue_scope_blobs};
use crate::deletion_recap::{delete_recap_state, recap_state_survivors};
use crate::deletion_state::{delete_inbox_state, inbox_state_survivors};

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
    /// Deleted `knowledge.source_analysis_inbox` rows.
    pub source_analysis_inbox: u64,
    /// Deleted `knowledge.source_analysis_heads` rows.
    pub source_analysis_heads: u64,
    /// Deleted `knowledge.ai_archive_object_inbox` rows.
    pub ai_archive_object_inbox: u64,
    /// Deleted `knowledge.repository_analysis_requests` rows.
    pub repository_analysis_requests: u64,
    /// Deleted `knowledge.ai_archive_tombstones` rows.
    pub ai_archive_tombstones: u64,
    /// Deleted `knowledge.social_tombstones` rows.
    pub social_tombstones: u64,
    /// Deleted `knowledge.notes_files` rows, the ordering guards of note files.
    pub notes_files: u64,
    /// Deleted `knowledge.notes_disconnections` rows, the guards of removed vault connections.
    pub notes_disconnections: u64,
    /// Deleted rows of the six `knowledge.channel_recap_*` tables.
    pub channel_recap_rows: u64,
    /// Deleted `knowledge.event_outbox` rows.
    pub event_outbox: u64,
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
    /// Digests queued by earlier deletions whose collection had not finished, reclaimed by
    /// this operation's drain and accounted separately from its own scope removals.
    pub recovered_digests_removed: Vec<String>,
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

impl DeletionError {
    /// Whether the same deletion can never succeed, however often it is retried.
    ///
    /// A count the database cannot represent is a property of the scope. A failed query, a blob
    /// store failure and rows that survived a commit may recover on the next attempt.
    #[must_use]
    pub const fn is_permanent(&self) -> bool {
        matches!(self, Self::ValueOverflow)
    }
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
        .map(|committed| committed.counts)
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
/// The row deletions, the queued blob digests and the audit row commit together; the queue is
/// drained afterwards, so a crash in between leaves queue rows that the next drain finishes
/// instead of a file nothing accounts for.
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
    let mut transaction = database
        .pool()
        .begin()
        .await
        .map_err(DeletionError::Query)?;
    let committed = run_child_first_deletion(&mut transaction, &scope).await?;
    transaction.commit().await.map_err(DeletionError::Query)?;

    let mut blob_digests_removed = Vec::new();
    let mut recovered_digests_removed = Vec::new();
    for (digest, queued_by) in drain_queue(database, blobs).await? {
        if queued_by == committed.deletion_id {
            blob_digests_removed.push(digest);
        } else {
            recovered_digests_removed.push(digest);
        }
    }

    // The audit row committed with the row deletions; its blob count is
    // finalized here because collection is only safe after commit.
    finalize_audit_blob_count(
        database.pool(),
        committed.deletion_id,
        blob_digests_removed.len(),
    )
    .await?;

    verify_scope_empty(database, &scope).await?;
    Ok(DeletionReceipt {
        scope,
        counts: committed.counts,
        blob_digests_removed,
        recovered_digests_removed,
        external_source_blob_digests: committed.external_source_blob_digests,
    })
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

/// Everything one committed deletion reports.
struct CommittedDeletion {
    counts: DeletionCounts,
    deletion_id: Uuid,
    external_source_blob_digests: Vec<String>,
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

/// Runs every child-first deletion statement, the blob-queue insert and the audit insert inside
/// the caller's transaction and reports each statement's affected rows together with the audit
/// row's identity.
async fn run_child_first_deletion(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
) -> Result<CommittedDeletion, DeletionError> {
    let deletion_id = Uuid::now_v7();
    let source_ref_ids = scope_source_ids(&mut **transaction, scope).await?;
    // The digests are read from the rows about to be deleted, so they are queued first.
    let queued = queue_scope_blobs(&mut *transaction, &source_ref_ids, deletion_id).await?;
    let user_content = delete_user_content(&mut *transaction, scope, &source_ref_ids).await?;
    let inbox_state = delete_inbox_state(&mut *transaction, scope).await?;
    let channel_recap_rows = delete_recap_state(&mut *transaction, scope, deletion_id).await?;

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
    let analysis = delete_analysis_rows(&mut *transaction, &source_ref_ids).await?;

    let counts = DeletionCounts {
        tags: user_content.tags,
        taggings: user_content.taggings,
        collections: user_content.collections,
        collection_items: user_content.collection_items,
        analysis_user_states: user_content.analysis_user_states,
        highlights: user_content.highlights,
        analysis_feedback: user_content.analysis_feedback,
        source_refs: analysis.source_refs,
        analysis_runs: analysis.analysis_runs,
        analysis_attempts: analysis.analysis_attempts,
        analysis_outputs: analysis.analysis_outputs,
        search_documents,
        search_projection_inputs,
        embedding_chunks,
        embedding_failures,
        source_analysis_inbox: inbox_state.source_analysis_inbox,
        source_analysis_heads: inbox_state.source_analysis_heads,
        ai_archive_object_inbox: inbox_state.ai_archive_object_inbox,
        repository_analysis_requests: inbox_state.repository_analysis_requests,
        ai_archive_tombstones: inbox_state.ai_archive_tombstones,
        social_tombstones: inbox_state.social_tombstones,
        notes_files: 0,
        notes_disconnections: 0,
        event_outbox: inbox_state.event_outbox,
        channel_recap_rows,
    };
    insert_audit_row(&mut *transaction, deletion_id, scope, &counts).await?;
    Ok(CommittedDeletion {
        counts,
        deletion_id,
        external_source_blob_digests: queued.external,
    })
}

/// Rows removed from the analysis tables themselves.
struct AnalysisRowCounts {
    analysis_attempts: u64,
    analysis_outputs: u64,
    analysis_runs: u64,
    source_refs: u64,
}

/// Deletes attempts, outputs, runs and finally the source revisions, in foreign-key order.
async fn delete_analysis_rows(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source_ref_ids: &[Uuid],
) -> Result<AnalysisRowCounts, DeletionError> {
    let analysis_attempts = sqlx::query(
        "delete from knowledge.analysis_attempts
         where run_id in (
             select run_id from knowledge.analysis_runs
             where source_ref_id = any($1)
         )",
    )
    .bind(source_ref_ids)
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
    .bind(source_ref_ids)
    .execute(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?
    .rows_affected();
    let analysis_runs =
        sqlx::query("delete from knowledge.analysis_runs where source_ref_id = any($1)")
            .bind(source_ref_ids)
            .execute(&mut **transaction)
            .await
            .map_err(DeletionError::Query)?
            .rows_affected();
    let source_refs =
        sqlx::query("delete from knowledge.source_refs where source_ref_id = any($1)")
            .bind(source_ref_ids)
            .execute(&mut **transaction)
            .await
            .map_err(DeletionError::Query)?
            .rows_affected();
    Ok(AnalysisRowCounts {
        analysis_attempts,
        analysis_outputs,
        analysis_runs,
        source_refs,
    })
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

/// Fails loudly when any row of the scope survived the committed deletion.
///
/// # Errors
///
/// Returns [`DeletionError::VerificationFailed`] when any row of the scope survives and a query
/// error when the database cannot be read.
pub async fn verify_scope_empty(
    database: &Database,
    scope: &DeletionScope,
) -> Result<(), DeletionError> {
    let pool = database.pool();
    let survivors = scope_source_ids(pool, scope).await?;
    if !survivors.is_empty()
        || inbox_state_survivors(pool, scope).await? > 0
        || recap_state_survivors(pool, scope).await? > 0
    {
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
