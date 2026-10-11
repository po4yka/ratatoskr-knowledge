//! Deletion of the channel recap state of a tenant (XR-021 CONTRACTS.md R2-06).
//!
//! The six `channel_recap_*` tables carry the tenant as `owner_ref` and hold user-derived content:
//! the verified manifest of post bodies, the raw model responses by blob reference and the recap
//! result. A tenant scope deletes every row of the tenant in foreign-key order inside the deletion's
//! transaction and queues the raw-response digests of the deleted attempts for erasure. A source or
//! archive scope never touches these tables, because a recap is not tied to one source document.

use uuid::Uuid;

use crate::{DeletionError, DeletionScope};

const KNOWLEDGE_OWNER: &str = "ratatoskr-knowledge";

/// Tables in the order their rows must go: each references the one after it.
const DELETION_ORDER: [&str; 6] = [
    "channel_recap_outbox",
    "channel_recap_results",
    "channel_recap_attempts",
    "channel_recap_manifests",
    "channel_recap_runs",
    "channel_recap_inbox",
];

/// Queues the tenant's recap raw-response digests, then deletes its recap rows.
///
/// Returns the number of deleted rows over all six tables. A source or archive scope deletes
/// nothing.
pub(crate) async fn delete_recap_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &DeletionScope,
    deletion_id: Uuid,
) -> Result<u64, DeletionError> {
    let DeletionScope::Tenant { tenant_ref } = scope else {
        return Ok(0);
    };
    // The digests are read from the attempts about to be deleted, so they are queued first.
    sqlx::query(
        "insert into knowledge.blob_erasure_queue (digest_hex, deletion_id)
         select distinct a.raw_response -> 'digest' ->> 'hex', $2::uuid
         from knowledge.channel_recap_attempts a
         join knowledge.channel_recap_runs r on r.recap_run_id = a.recap_run_id
         where r.owner_ref = $1 and a.raw_response ->> 'owner_service' = $3
         on conflict (digest_hex) do update set deletion_id = excluded.deletion_id",
    )
    .bind(tenant_ref)
    .bind(deletion_id)
    .bind(KNOWLEDGE_OWNER)
    .execute(&mut **transaction)
    .await
    .map_err(DeletionError::Query)?;
    let mut deleted = 0_u64;
    for table in DELETION_ORDER {
        deleted += delete_table(transaction, table, tenant_ref).await?;
    }
    Ok(deleted)
}

async fn delete_table(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    tenant_ref: &str,
) -> Result<u64, DeletionError> {
    let sql = match table {
        "channel_recap_runs" | "channel_recap_inbox" => {
            format!("delete from knowledge.{table} where owner_ref = $1")
        }
        _ => format!(
            "delete from knowledge.{table}
             where recap_run_id in (
                 select recap_run_id from knowledge.channel_recap_runs where owner_ref = $1
             )"
        ),
    };
    sqlx::query(&sql)
        .bind(tenant_ref)
        .execute(&mut **transaction)
        .await
        .map_err(DeletionError::Query)
        .map(|result| result.rows_affected())
}

/// Rows of the tenant's recap state that survive; zero for a source or archive scope.
pub(crate) async fn recap_state_survivors(
    pool: &sqlx::PgPool,
    scope: &DeletionScope,
) -> Result<i64, DeletionError> {
    let DeletionScope::Tenant { tenant_ref } = scope else {
        return Ok(0);
    };
    sqlx::query_scalar(
        "select (select count(*) from knowledge.channel_recap_inbox where owner_ref = $1)
              + (select count(*) from knowledge.channel_recap_runs where owner_ref = $1)
              + (select count(*) from knowledge.channel_recap_manifests where owner_ref = $1)",
    )
    .bind(tenant_ref)
    .fetch_one(pool)
    .await
    .map_err(DeletionError::Query)
}
