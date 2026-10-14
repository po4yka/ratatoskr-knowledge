//! Deletion of the content-bearing inbox, tombstone, request and outbox state of a scope (XR-021
//! CONTRACTS.md S12).
//!
//! A tenant scope removes everything that names the tenant, including the ordering guards. A
//! source or archive scope removes the deliveries and requests keyed by the scope and keeps what
//! guards against resurrection: the tombstones and the tombstone deliveries' own dedup receipts.

use crate::{DeletionError, DeletionScope};

const TOMBSTONE_SUBJECT: &str = "ai_archive.subject.tombstoned.v1";

/// Rows deleted from the inbox-side tables by one deletion.
#[derive(Debug, Default)]
pub(crate) struct InboxStateCounts {
    pub(crate) source_analysis_inbox: u64,
    pub(crate) source_analysis_heads: u64,
    pub(crate) ai_archive_object_inbox: u64,
    pub(crate) repository_analysis_requests: u64,
    pub(crate) ai_archive_tombstones: u64,
    pub(crate) social_tombstones: u64,
    pub(crate) event_outbox: u64,
    pub(crate) notes_files: u64,
    pub(crate) notes_disconnections: u64,
}

async fn delete_one(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &str,
    first: &str,
) -> Result<u64, DeletionError> {
    sqlx::query(sql)
        .bind(first)
        .execute(&mut **transaction)
        .await
        .map_err(DeletionError::Query)
        .map(|result| result.rows_affected())
}

async fn delete_two(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &str,
    first: &str,
    second: &str,
) -> Result<u64, DeletionError> {
    sqlx::query(sql)
        .bind(first)
        .bind(second)
        .execute(&mut **transaction)
        .await
        .map_err(DeletionError::Query)
        .map(|result| result.rows_affected())
}

async fn delete_tenant_rows(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    tenant_ref: &str,
) -> Result<u64, DeletionError> {
    delete_one(
        transaction,
        &format!("delete from knowledge.{table} where tenant_ref = $1"),
        tenant_ref,
    )
    .await
}

/// Deletes the scope's inbox-side state inside the deletion's transaction.
///
/// Heads go before the inbox rows they reference.
pub(crate) async fn delete_inbox_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
) -> Result<InboxStateCounts, DeletionError> {
    match scope {
        DeletionScope::Tenant { tenant_ref } => delete_tenant_state(transaction, tenant_ref).await,
        DeletionScope::Source {
            tenant_ref,
            source_document_id,
            ..
        } => delete_source_state(transaction, tenant_ref, source_document_id).await,
        DeletionScope::Archive {
            tenant_ref,
            ai_archive_id,
        } => delete_archive_state(transaction, tenant_ref, ai_archive_id).await,
    }
}

/// Everything that names the tenant, including the ordering guards.
async fn delete_tenant_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_ref: &str,
) -> Result<InboxStateCounts, DeletionError> {
    let source_analysis_heads =
        delete_tenant_rows(transaction, "source_analysis_heads", tenant_ref).await?;
    let source_analysis_inbox =
        delete_tenant_rows(transaction, "source_analysis_inbox", tenant_ref).await?;
    let ai_archive_object_inbox =
        delete_tenant_rows(transaction, "ai_archive_object_inbox", tenant_ref).await?;
    let repository_analysis_requests =
        delete_tenant_rows(transaction, "repository_analysis_requests", tenant_ref).await?;
    let ai_archive_tombstones =
        delete_tenant_rows(transaction, "ai_archive_tombstones", tenant_ref).await?;
    let social_tombstones =
        delete_tenant_rows(transaction, "social_tombstones", tenant_ref).await?;
    let event_outbox = delete_tenant_rows(transaction, "event_outbox", tenant_ref).await?;
    let notes_files = delete_tenant_rows(transaction, "notes_files", tenant_ref).await?;
    let notes_disconnections =
        delete_tenant_rows(transaction, "notes_disconnections", tenant_ref).await?;
    Ok(InboxStateCounts {
        source_analysis_inbox,
        source_analysis_heads,
        ai_archive_object_inbox,
        repository_analysis_requests,
        ai_archive_tombstones,
        social_tombstones,
        event_outbox,
        notes_files,
        notes_disconnections,
    })
}

/// The deliveries and requests of one source; the tombstones stay.
async fn delete_source_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_ref: &str,
    source_document_id: &str,
) -> Result<InboxStateCounts, DeletionError> {
    let source_analysis_heads = delete_two(
        transaction,
        "delete from knowledge.source_analysis_heads
         where tenant_ref = $1 and source_id = $2",
        tenant_ref,
        source_document_id,
    )
    .await?;
    let source_analysis_inbox = delete_two(
        transaction,
        &format!(
            "delete from knowledge.source_analysis_inbox
             where tenant_ref = $1 and source_id = $2
               and subject <> '{TOMBSTONE_SUBJECT}'"
        ),
        tenant_ref,
        source_document_id,
    )
    .await?;
    let ai_archive_object_inbox = delete_two(
        transaction,
        "delete from knowledge.ai_archive_object_inbox
         where tenant_ref = $1 and object_id = $2",
        tenant_ref,
        source_document_id,
    )
    .await?;
    let repository_analysis_requests = delete_two(
        transaction,
        "delete from knowledge.repository_analysis_requests
         where tenant_ref = $1 and repository_id::text = $2",
        tenant_ref,
        source_document_id,
    )
    .await?;
    Ok(InboxStateCounts {
        source_analysis_inbox,
        source_analysis_heads,
        ai_archive_object_inbox,
        repository_analysis_requests,
        ..InboxStateCounts::default()
    })
}

/// The deliveries keyed by one archive; the tombstones and their dedup receipts stay.
async fn delete_archive_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_ref: &str,
    ai_archive_id: &str,
) -> Result<InboxStateCounts, DeletionError> {
    let source_analysis_heads = delete_two(
        transaction,
        &format!(
            "delete from knowledge.source_analysis_heads heads
             where heads.tenant_ref = $1 and exists (
                 select 1 from knowledge.source_analysis_inbox inbox
                 where inbox.event_id = heads.inbox_event_id
                   and inbox.archive_id = $2
                   and inbox.subject <> '{TOMBSTONE_SUBJECT}'
             )"
        ),
        tenant_ref,
        ai_archive_id,
    )
    .await?;
    let source_analysis_inbox = delete_two(
        transaction,
        &format!(
            "delete from knowledge.source_analysis_inbox
             where tenant_ref = $1 and archive_id = $2
               and subject <> '{TOMBSTONE_SUBJECT}'"
        ),
        tenant_ref,
        ai_archive_id,
    )
    .await?;
    let ai_archive_object_inbox = delete_two(
        transaction,
        "delete from knowledge.ai_archive_object_inbox
         where tenant_ref = $1 and archive_id = $2",
        tenant_ref,
        ai_archive_id,
    )
    .await?;
    Ok(InboxStateCounts {
        source_analysis_inbox,
        source_analysis_heads,
        ai_archive_object_inbox,
        ..InboxStateCounts::default()
    })
}

/// Counts the inbox-side rows a committed deletion of `scope` should have removed.
pub(crate) async fn inbox_state_survivors(
    pool: &sqlx::PgPool,
    scope: &DeletionScope,
) -> Result<i64, DeletionError> {
    let survivors: i64 = match scope {
        DeletionScope::Tenant { tenant_ref } => {
            sqlx::query_scalar(
                "select
                   (select count(*) from knowledge.source_analysis_heads where tenant_ref = $1)
                 + (select count(*) from knowledge.source_analysis_inbox where tenant_ref = $1)
                 + (select count(*) from knowledge.ai_archive_object_inbox where tenant_ref = $1)
                 + (select count(*) from knowledge.repository_analysis_requests
                    where tenant_ref = $1)
                 + (select count(*) from knowledge.ai_archive_tombstones where tenant_ref = $1)
                 + (select count(*) from knowledge.social_tombstones where tenant_ref = $1)
                 + (select count(*) from knowledge.event_outbox where tenant_ref = $1)
                 + (select count(*) from knowledge.notes_files where tenant_ref = $1)
                 + (select count(*) from knowledge.notes_disconnections where tenant_ref = $1)",
            )
            .bind(tenant_ref)
            .fetch_one(pool)
            .await
        }
        DeletionScope::Source {
            tenant_ref,
            source_document_id,
            ..
        } => {
            sqlx::query_scalar(&format!(
                "select
                   (select count(*) from knowledge.source_analysis_heads
                    where tenant_ref = $1 and source_id = $2)
                 + (select count(*) from knowledge.source_analysis_inbox
                    where tenant_ref = $1 and source_id = $2
                      and subject <> '{TOMBSTONE_SUBJECT}')
                 + (select count(*) from knowledge.ai_archive_object_inbox
                    where tenant_ref = $1 and object_id = $2)
                 + (select count(*) from knowledge.repository_analysis_requests
                    where tenant_ref = $1 and repository_id::text = $2)"
            ))
            .bind(tenant_ref)
            .bind(source_document_id)
            .fetch_one(pool)
            .await
        }
        DeletionScope::Archive {
            tenant_ref,
            ai_archive_id,
        } => {
            sqlx::query_scalar(&format!(
                "select
                   (select count(*) from knowledge.source_analysis_inbox
                    where tenant_ref = $1 and archive_id = $2
                      and subject <> '{TOMBSTONE_SUBJECT}')
                 + (select count(*) from knowledge.ai_archive_object_inbox
                    where tenant_ref = $1 and archive_id = $2)"
            ))
            .bind(tenant_ref)
            .bind(ai_archive_id)
            .fetch_one(pool)
            .await
        }
    }
    .map_err(DeletionError::Query)?;
    Ok(survivors)
}
