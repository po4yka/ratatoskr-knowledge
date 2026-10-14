//! The audit row of one deletion, written inside the deletion's own transaction.

use uuid::Uuid;

use crate::deletion::{DeletionCounts, DeletionError, DeletionScope};

/// Inserts the audit record inside the deletion's own transaction.
pub(crate) async fn insert_audit_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deletion_id: Uuid,
    scope: &DeletionScope,
    counts: &DeletionCounts,
) -> Result<(), DeletionError> {
    sqlx::query(
        "insert into knowledge.deletion_records (
             deletion_id, tenant_ref, scope, owner_context, ai_archive_id, source_document_id,
             source_refs_deleted, analysis_runs_deleted, analysis_attempts_deleted,
             analysis_outputs_deleted, search_projection_inputs_deleted,
             search_documents_deleted,
             embedding_chunks_deleted, embedding_failures_deleted,
             tags_deleted, taggings_deleted, collections_deleted, collection_items_deleted,
             analysis_user_states_deleted, highlights_deleted, analysis_feedback_deleted,
             source_analysis_inbox_deleted, source_analysis_heads_deleted,
             ai_archive_object_inbox_deleted, repository_analysis_requests_deleted,
             ai_archive_tombstones_deleted, social_tombstones_deleted, event_outbox_deleted,
             channel_recap_rows_deleted, notes_files_deleted, notes_disconnections_deleted,
             blob_digests_removed
         ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                   $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31,
                   0)",
    )
    .bind(deletion_id)
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
    .bind(i32_count(counts.source_analysis_inbox)?)
    .bind(i32_count(counts.source_analysis_heads)?)
    .bind(i32_count(counts.ai_archive_object_inbox)?)
    .bind(i32_count(counts.repository_analysis_requests)?)
    .bind(i32_count(counts.ai_archive_tombstones)?)
    .bind(i32_count(counts.social_tombstones)?)
    .bind(i32_count(counts.event_outbox)?)
    .bind(i32_count(counts.channel_recap_rows)?)
    .bind(i32_count(counts.notes_files)?)
    .bind(i32_count(counts.notes_disconnections)?)
    .execute(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?;
    Ok(())
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
