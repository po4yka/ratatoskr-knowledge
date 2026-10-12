//! A source is searchable as its current head, not as every revision it ever had.

use sqlx::{Postgres, Transaction};
use uuid::Uuid;

/// Removes the search projection of every revision of the source of `source_ref_id` whose content
/// is not the source's current head, and returns how many search documents went.
///
/// A conversation, a project or a social post that Knowledge analysed more than once has one
/// source revision per content digest. Its analyses and raw responses stay as history, but only
/// the head revision may be found by search: a survivor re-announcement after a privacy deletion
/// replaces the head with the text that is left, and the text the deleted export wrote (the
/// previous head) must stop being served (XR-021 CONTRACTS.md R5-08 rule 2). The projection input
/// goes with it so that a rebuild of the search documents cannot bring the old text back.
///
/// The rule is stated from the revision that was just persisted and covers both orders: a newer
/// head retires the older revisions, and an older revision that is persisted late (its analysis
/// was in flight when the head moved) retires itself. A source with no head row, such as a
/// repository or an extracted document, is left alone.
///
/// # Errors
///
/// Returns the underlying [`sqlx::Error`] when a write fails.
pub(crate) async fn retire_superseded_projections(
    transaction: &mut Transaction<'_, Postgres>,
    source_ref_id: Uuid,
) -> Result<u64, sqlx::Error> {
    sqlx::query(
        "delete from knowledge.search_projection_inputs i
         using knowledge.source_refs persisted, knowledge.source_refs revision,
               knowledge.source_analysis_heads head
         where persisted.source_ref_id = $1
           and head.tenant_ref = persisted.tenant_ref
           and head.source_id = persisted.source_document_id
           and revision.tenant_ref = persisted.tenant_ref
           and revision.owner_context = persisted.owner_context
           and revision.source_document_id = persisted.source_document_id
           and revision.content_digest_hex <> head.content_digest_hex
           and i.source_ref_id = revision.source_ref_id",
    )
    .bind(source_ref_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "delete from knowledge.search_documents d
         using knowledge.source_refs persisted, knowledge.source_refs revision,
               knowledge.source_analysis_heads head
         where persisted.source_ref_id = $1
           and head.tenant_ref = persisted.tenant_ref
           and head.source_id = persisted.source_document_id
           and revision.tenant_ref = persisted.tenant_ref
           and revision.owner_context = persisted.owner_context
           and revision.source_document_id = persisted.source_document_id
           and revision.content_digest_hex <> head.content_digest_hex
           and d.source_ref_id = revision.source_ref_id",
    )
    .bind(source_ref_id)
    .execute(&mut **transaction)
    .await
    .map(|result| result.rows_affected())
}
