//! Tenant-scoped user-authored organisation and interpretation state.

use ratatoskr_document_contracts::{Document, DocumentBlock};
use ratatoskr_identifiers::{BlockId, DigestAlgorithm};
use sqlx::PgPool;
use uuid::Uuid;

/// A validated tag name and its canonical comparison key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagName {
    /// Display spelling retained for callers.
    pub display: String,
    /// Tenant-local case-folded key.
    pub normalized: String,
}

/// Read state for one accepted analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadState {
    /// Not yet read.
    Unread,
    /// Read.
    Read,
}

/// Durable read and favorite state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AnalysisState {
    /// Read state.
    pub read_state: ReadState,
    /// Favorite marker.
    pub favorite: bool,
}

/// Stored highlight appearance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HighlightStyle {
    /// Yellow.
    Yellow,
    /// Green.
    Green,
    /// Blue.
    Blue,
    /// Pink.
    Pink,
    /// Purple.
    Purple,
    /// Underline.
    Underline,
}

/// Typed feedback category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackCategory {
    /// Incorrect.
    Incorrect,
    /// Missing context.
    MissingContext,
    /// Unsupported claim.
    UnsupportedClaim,
    /// Poor quality.
    PoorQuality,
    /// Other.
    Other,
}

/// A target allowed in a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CollectionTarget {
    /// Accepted analysis output.
    Analysis(Uuid),
    /// Immutable source revision.
    Source(Uuid),
}

/// One ordered collection entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct CollectionItem {
    /// Zero-based position.
    pub position: u32,
    /// Immutable target.
    pub target: CollectionTarget,
}

/// A text highlight to validate and persist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
pub struct HighlightAnchor {
    /// Accepted analysis output.
    pub output_id: Uuid,
    /// Source revision of the output.
    pub source_ref_id: Uuid,
    /// Stable block identity.
    pub block_id: BlockId,
    /// Inclusive Unicode-scalar start.
    pub start_offset: u32,
    /// Exclusive Unicode-scalar end.
    pub end_offset: u32,
    /// Stored appearance.
    pub style: HighlightStyle,
}

/// User-content validation or persistence failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum UserContentError {
    /// Invalid bounded value or anchor.
    #[error("user content is invalid")]
    Invalid,
    /// Scoped target is absent.
    #[error("user content target was not found")]
    NotFound,
    /// Mutation conflicts with existing state.
    #[error("user content conflicts with existing state")]
    Conflict,
    /// Database stored a value outside this module's enum.
    #[error("user content contains an invalid stored value")]
    DatabaseValue,
    /// Owned query failed.
    #[error("user content persistence failed")]
    Database(#[source] sqlx::Error),
}

/// Validates and canonicalizes a tag name.
///
/// # Errors
/// Returns [`UserContentError::Invalid`] for an empty or oversized name.
pub fn tag_name(value: &str) -> Result<TagName, UserContentError> {
    let display = bounded(value, 128)?;
    let normalized = display.to_lowercase();
    if normalized.chars().count() > 128 {
        return Err(UserContentError::Invalid);
    }
    Ok(TagName {
        display,
        normalized,
    })
}

/// Creates a tenant-local tag.
///
/// # Errors
/// Returns a conflict for a duplicate name or a persistence error.
pub async fn create_tag(
    pool: &PgPool,
    tenant_ref: &str,
    name: TagName,
) -> Result<Uuid, UserContentError> {
    let id = Uuid::now_v7();
    match sqlx::query("insert into knowledge.tags (tag_id, tenant_ref, normalized_name, display_name) values ($1,$2,$3,$4)")
        .bind(id).bind(tenant_ref).bind(name.normalized).bind(name.display).execute(pool).await {
        Ok(_) => Ok(id), Err(error) if unique(&error) => Err(UserContentError::Conflict), Err(error) => Err(UserContentError::Database(error)),
    }
}

/// Tags one tenant-owned accepted output.
///
/// # Errors
/// Returns a scoped absence for a foreign or unaccepted target.
pub async fn tag_analysis(
    pool: &PgPool,
    tenant: &str,
    tag: Uuid,
    output: Uuid,
) -> Result<(), UserContentError> {
    let result = sqlx::query("insert into knowledge.analysis_taggings (tag_id, output_id, tenant_ref)
        select t.tag_id,$3,$1 from knowledge.tags t where t.tag_id=$2 and t.tenant_ref=$1 and exists (
          select 1 from knowledge.analysis_outputs o join knowledge.analysis_runs r on r.run_id=o.run_id join knowledge.source_refs s on s.source_ref_id=r.source_ref_id
          where o.output_id=$3 and o.accepted and s.tenant_ref=$1) on conflict do nothing")
        .bind(tenant).bind(tag).bind(output).execute(pool).await.map_err(UserContentError::Database)?;
    if result.rows_affected() != 0 {
        return Ok(());
    }
    let exists: bool = sqlx::query_scalar("select exists(select 1 from knowledge.analysis_taggings where tenant_ref=$1 and tag_id=$2 and output_id=$3)")
        .bind(tenant).bind(tag).bind(output).fetch_one(pool).await.map_err(UserContentError::Database)?;
    if exists {
        Ok(())
    } else {
        Err(UserContentError::NotFound)
    }
}

/// Atomically merges a source tag into a same-tenant destination tag.
///
/// # Errors
/// Returns a scoped absence unless both tags belong to the tenant.
pub async fn merge_tags(
    pool: &PgPool,
    tenant: &str,
    source: Uuid,
    destination: Uuid,
) -> Result<(), UserContentError> {
    if source == destination {
        return Err(UserContentError::Invalid);
    }
    let mut tx = pool.begin().await.map_err(UserContentError::Database)?;
    let tags: Vec<Uuid> = sqlx::query_scalar("select tag_id from knowledge.tags where tenant_ref=$1 and tag_id=any($2) order by tag_id for update")
        .bind(tenant).bind(vec![source,destination]).fetch_all(&mut *tx).await.map_err(UserContentError::Database)?;
    if tags.len() != 2 {
        return Err(UserContentError::NotFound);
    }
    sqlx::query("insert into knowledge.analysis_taggings (tag_id,output_id,tenant_ref) select $1,output_id,tenant_ref from knowledge.analysis_taggings where tag_id=$2 and tenant_ref=$3 on conflict do nothing")
        .bind(destination).bind(source).bind(tenant).execute(&mut *tx).await.map_err(UserContentError::Database)?;
    sqlx::query("delete from knowledge.tags where tag_id=$1 and tenant_ref=$2")
        .bind(source)
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .map_err(UserContentError::Database)?;
    tx.commit().await.map_err(UserContentError::Database)
}

/// Creates an empty tenant-local collection.
///
/// # Errors
/// Returns invalid for an empty or oversized name.
pub async fn create_collection(
    pool: &PgPool,
    tenant: &str,
    name: &str,
) -> Result<Uuid, UserContentError> {
    let id = Uuid::now_v7();
    let name = bounded(name, 256)?;
    sqlx::query(
        "insert into knowledge.collections (collection_id,tenant_ref,name) values ($1,$2,$3)",
    )
    .bind(id)
    .bind(tenant)
    .bind(name)
    .execute(pool)
    .await
    .map_err(UserContentError::Database)?;
    Ok(id)
}

/// Inserts a target at a dense position; `None` appends.
///
/// # Errors
/// Returns invalid for an out-of-bounds position and scoped absence for foreign targets.
pub async fn add_collection_item(
    pool: &PgPool,
    tenant: &str,
    collection: Uuid,
    target: CollectionTarget,
    position: Option<u32>,
) -> Result<CollectionItem, UserContentError> {
    let mut tx = pool.begin().await.map_err(UserContentError::Database)?;
    lock_collection(&mut tx, tenant, collection).await?;
    ensure_target(&mut tx, tenant, target).await?;
    defer_positions(&mut tx).await?;
    let count: u32 = count_items(&mut tx, collection).await?;
    let position = position.unwrap_or(count);
    if position > count {
        return Err(UserContentError::Invalid);
    }
    sqlx::query("update knowledge.collection_items set position=position+1 where collection_id=$1 and position >= $2").bind(collection).bind(as_i32(position)?).execute(&mut *tx).await.map_err(UserContentError::Database)?;
    insert_item(&mut tx, tenant, collection, target, position).await?;
    touch(&mut tx, collection).await?;
    tx.commit().await.map_err(UserContentError::Database)?;
    Ok(CollectionItem { position, target })
}

/// Moves an existing target while preserving unaffected relative order.
///
/// # Errors
/// Returns invalid for an out-of-bounds destination and scoped absence for missing targets.
pub async fn move_collection_item(
    pool: &PgPool,
    tenant: &str,
    collection: Uuid,
    target: CollectionTarget,
    destination: u32,
) -> Result<(), UserContentError> {
    let mut tx = pool.begin().await.map_err(UserContentError::Database)?;
    lock_collection(&mut tx, tenant, collection).await?;
    defer_positions(&mut tx).await?;
    let current = item_position(&mut tx, collection, target).await?;
    let count = count_items(&mut tx, collection).await?;
    let last = count.checked_sub(1).ok_or(UserContentError::NotFound)?;
    if destination > last {
        return Err(UserContentError::Invalid);
    }
    if current != destination {
        sqlx::query("update knowledge.collection_items set position=case when $3>$2 and position>$2 and position<=$3 then position-1 when $3<$2 and position>=$3 and position<$2 then position+1 else position end where collection_id=$1")
        .bind(collection).bind(as_i32(current)?).bind(as_i32(destination)?).execute(&mut *tx).await.map_err(UserContentError::Database)?;
        set_item_position(&mut tx, collection, target, destination).await?;
        touch(&mut tx, collection).await?;
    }
    tx.commit().await.map_err(UserContentError::Database)
}

/// Lists a collection in durable explicit order.
///
/// # Errors
/// Returns a scoped absence for a foreign collection.
pub async fn list_collection_items(
    pool: &PgPool,
    tenant: &str,
    collection: Uuid,
) -> Result<Vec<CollectionItem>, UserContentError> {
    let exists: Option<i32> = sqlx::query_scalar(
        "select 1 from knowledge.collections where collection_id=$1 and tenant_ref=$2",
    )
    .bind(collection)
    .bind(tenant)
    .fetch_optional(pool)
    .await
    .map_err(UserContentError::Database)?;
    if exists.is_none() {
        return Err(UserContentError::NotFound);
    }
    let rows:Vec<(i32,Option<Uuid>,Option<Uuid>)>=sqlx::query_as("select position,output_id,source_ref_id from knowledge.collection_items where collection_id=$1 and tenant_ref=$2 order by position").bind(collection).bind(tenant).fetch_all(pool).await.map_err(UserContentError::Database)?;
    rows.into_iter()
        .map(|(position, output, source)| {
            Ok(CollectionItem {
                position: u32::try_from(position).map_err(|_| UserContentError::DatabaseValue)?,
                target: match (output, source) {
                    (Some(id), None) => CollectionTarget::Analysis(id),
                    (None, Some(id)) => CollectionTarget::Source(id),
                    _ => return Err(UserContentError::DatabaseValue),
                },
            })
        })
        .collect()
}

/// Sets one accepted analysis's complete read/favorite state idempotently.
///
/// # Errors
/// Returns a scoped absence for a foreign or unaccepted analysis.
pub async fn set_analysis_state(
    pool: &PgPool,
    tenant: &str,
    output: Uuid,
    state: AnalysisState,
) -> Result<AnalysisState, UserContentError> {
    let row:Option<(String,bool)>=sqlx::query_as("insert into knowledge.analysis_user_states (tenant_ref,output_id,read_state,favorite) select $1,$2,$3,$4 where exists (select 1 from knowledge.analysis_outputs o join knowledge.analysis_runs r on r.run_id=o.run_id join knowledge.source_refs s on s.source_ref_id=r.source_ref_id where o.output_id=$2 and o.accepted and s.tenant_ref=$1) on conflict (tenant_ref,output_id) do update set read_state=excluded.read_state,favorite=excluded.favorite,updated_at=now() returning read_state,favorite")
        .bind(tenant).bind(output).bind(read_state_name(state.read_state)).bind(state.favorite).fetch_optional(pool).await.map_err(UserContentError::Database)?;
    let Some((read_state, favorite)) = row else {
        return Err(UserContentError::NotFound);
    };
    Ok(AnalysisState {
        read_state: parse_read_state(&read_state)?,
        favorite,
    })
}

/// Persists bounded typed feedback without mutating the analysis.
///
/// # Errors
/// Returns invalid for oversized detail and scoped absence for foreign outputs.
pub async fn record_feedback(
    pool: &PgPool,
    tenant: &str,
    output: Uuid,
    category: FeedbackCategory,
    detail: Option<&str>,
) -> Result<Uuid, UserContentError> {
    let detail = detail.map(|value| bounded(value, 2000)).transpose()?;
    let id = Uuid::now_v7();
    sqlx::query_scalar("insert into knowledge.analysis_feedback (feedback_id,tenant_ref,output_id,issue_category,detail) select $1,$2,$3,$4,$5 where exists (select 1 from knowledge.analysis_outputs o join knowledge.analysis_runs r on r.run_id=o.run_id join knowledge.source_refs s on s.source_ref_id=r.source_ref_id where o.output_id=$3 and o.accepted and s.tenant_ref=$2) returning feedback_id")
      .bind(id).bind(tenant).bind(output).bind(feedback_name(category)).bind(detail).fetch_optional(pool).await.map_err(UserContentError::Database)?.ok_or(UserContentError::NotFound)
}

/// Validates an anchor against Unicode-scalar offsets in one supplied Document IR revision.
///
/// # Errors
/// Returns scoped absence for an unknown block and invalid for an empty or out-of-range range.
pub fn validate_highlight_anchor(
    document: &Document,
    block_id: BlockId,
    start: u32,
    end: u32,
) -> Result<(), UserContentError> {
    let text = document
        .blocks
        .iter()
        .find_map(|block| match block {
            DocumentBlock::Heading {
                block_id: candidate,
                text,
                ..
            }
            | DocumentBlock::Paragraph {
                block_id: candidate,
                text,
                ..
            } if *candidate == block_id => Some(text.as_str()),
            _ => None,
        })
        .ok_or(UserContentError::NotFound)?;
    let length = u32::try_from(text.chars().count()).map_err(|_| UserContentError::Invalid)?;
    if start >= end || end > length {
        Err(UserContentError::Invalid)
    } else {
        Ok(())
    }
}

/// Persists a validated anchor without retaining the supplied source text.
///
/// # Errors
/// Returns a scoped absence when the document identity does not match the accepted provenance.
pub async fn create_highlight(
    pool: &PgPool,
    tenant: &str,
    document: &Document,
    anchor: HighlightAnchor,
) -> Result<Uuid, UserContentError> {
    validate_highlight_anchor(
        document,
        anchor.block_id,
        anchor.start_offset,
        anchor.end_offset,
    )?;
    let id = Uuid::now_v7();
    let algorithm = match document.content_digest.algorithm {
        DigestAlgorithm::Sha256 => "sha256",
        _ => return Err(UserContentError::Invalid),
    };
    let stored=sqlx::query_scalar("insert into knowledge.highlights (highlight_id,tenant_ref,output_id,source_ref_id,block_id,start_offset,end_offset,style) select $1,$2,$3,$4,$5,$6,$7,$8 where exists (select 1 from knowledge.analysis_outputs o join knowledge.analysis_runs r on r.run_id=o.run_id join knowledge.source_refs s on s.source_ref_id=r.source_ref_id where o.output_id=$3 and o.accepted and r.source_ref_id=$4 and s.tenant_ref=$2 and s.source_document_id=$9 and s.content_digest_algorithm=$10 and s.content_digest_hex=$11) returning highlight_id")
      .bind(id).bind(tenant).bind(anchor.output_id).bind(anchor.source_ref_id).bind(anchor.block_id.0).bind(as_i32(anchor.start_offset)?).bind(as_i32(anchor.end_offset)?).bind(style_name(anchor.style)).bind(document.document_id.0).bind(algorithm).bind(document.content_digest.hex.as_str()).fetch_optional(pool).await;
    match stored {
        Ok(Some(id)) => Ok(id),
        Ok(None) => Err(UserContentError::NotFound),
        Err(error) if unique(&error) => Err(UserContentError::Conflict),
        Err(error) => Err(UserContentError::Database(error)),
    }
}

fn bounded(value: &str, max: usize) -> Result<String, UserContentError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > max {
        Err(UserContentError::Invalid)
    } else {
        Ok(value.to_owned())
    }
}
fn as_i32(value: u32) -> Result<i32, UserContentError> {
    i32::try_from(value).map_err(|_| UserContentError::Invalid)
}
fn unique(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| code == "23505")
}
fn read_state_name(value: ReadState) -> &'static str {
    match value {
        ReadState::Unread => "unread",
        ReadState::Read => "read",
    }
}
fn parse_read_state(value: &str) -> Result<ReadState, UserContentError> {
    match value {
        "unread" => Ok(ReadState::Unread),
        "read" => Ok(ReadState::Read),
        _ => Err(UserContentError::DatabaseValue),
    }
}
fn style_name(value: HighlightStyle) -> &'static str {
    match value {
        HighlightStyle::Yellow => "yellow",
        HighlightStyle::Green => "green",
        HighlightStyle::Blue => "blue",
        HighlightStyle::Pink => "pink",
        HighlightStyle::Purple => "purple",
        HighlightStyle::Underline => "underline",
    }
}
fn feedback_name(value: FeedbackCategory) -> &'static str {
    match value {
        FeedbackCategory::Incorrect => "incorrect",
        FeedbackCategory::MissingContext => "missing_context",
        FeedbackCategory::UnsupportedClaim => "unsupported_claim",
        FeedbackCategory::PoorQuality => "poor_quality",
        FeedbackCategory::Other => "other",
    }
}

async fn lock_collection(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: &str,
    collection: Uuid,
) -> Result<(), UserContentError> {
    let row: Option<i32> = sqlx::query_scalar(
        "select 1 from knowledge.collections where collection_id=$1 and tenant_ref=$2 for update",
    )
    .bind(collection)
    .bind(tenant)
    .fetch_optional(&mut **tx)
    .await
    .map_err(UserContentError::Database)?;
    row.ok_or(UserContentError::NotFound).map(|_| ())
}
async fn defer_positions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), UserContentError> {
    sqlx::query("set constraints collection_items_collection_position_key deferred")
        .execute(&mut **tx)
        .await
        .map_err(UserContentError::Database)?;
    Ok(())
}
async fn count_items(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    collection: Uuid,
) -> Result<u32, UserContentError> {
    let count: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.collection_items where collection_id=$1",
    )
    .bind(collection)
    .fetch_one(&mut **tx)
    .await
    .map_err(UserContentError::Database)?;
    u32::try_from(count).map_err(|_| UserContentError::DatabaseValue)
}
async fn ensure_target(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: &str,
    target: CollectionTarget,
) -> Result<(), UserContentError> {
    let exists:bool=match target{CollectionTarget::Analysis(id)=>sqlx::query_scalar("select exists(select 1 from knowledge.analysis_outputs o join knowledge.analysis_runs r on r.run_id=o.run_id join knowledge.source_refs s on s.source_ref_id=r.source_ref_id where o.output_id=$1 and o.accepted and s.tenant_ref=$2)").bind(id).bind(tenant).fetch_one(&mut **tx).await,CollectionTarget::Source(id)=>sqlx::query_scalar("select exists(select 1 from knowledge.source_refs where source_ref_id=$1 and tenant_ref=$2)").bind(id).bind(tenant).fetch_one(&mut **tx).await}.map_err(UserContentError::Database)?;
    if exists {
        Ok(())
    } else {
        Err(UserContentError::NotFound)
    }
}
async fn insert_item(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: &str,
    collection: Uuid,
    target: CollectionTarget,
    position: u32,
) -> Result<(), UserContentError> {
    let query=match target{CollectionTarget::Analysis(id)=>sqlx::query("insert into knowledge.collection_items (collection_id,position,tenant_ref,output_id) values ($1,$2,$3,$4)").bind(collection).bind(as_i32(position)?).bind(tenant).bind(id).execute(&mut **tx).await,CollectionTarget::Source(id)=>sqlx::query("insert into knowledge.collection_items (collection_id,position,tenant_ref,source_ref_id) values ($1,$2,$3,$4)").bind(collection).bind(as_i32(position)?).bind(tenant).bind(id).execute(&mut **tx).await};
    match query {
        Ok(_) => Ok(()),
        Err(error) if unique(&error) => Err(UserContentError::Conflict),
        Err(error) => Err(UserContentError::Database(error)),
    }
}
async fn item_position(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    collection: Uuid,
    target: CollectionTarget,
) -> Result<u32, UserContentError> {
    let row:Option<i32>=match target{CollectionTarget::Analysis(id)=>sqlx::query_scalar("select position from knowledge.collection_items where collection_id=$1 and output_id=$2").bind(collection).bind(id).fetch_optional(&mut **tx).await,CollectionTarget::Source(id)=>sqlx::query_scalar("select position from knowledge.collection_items where collection_id=$1 and source_ref_id=$2").bind(collection).bind(id).fetch_optional(&mut **tx).await}.map_err(UserContentError::Database)?;
    row.ok_or(UserContentError::NotFound)
        .and_then(|value| u32::try_from(value).map_err(|_| UserContentError::DatabaseValue))
}
async fn set_item_position(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    collection: Uuid,
    target: CollectionTarget,
    position: u32,
) -> Result<(), UserContentError> {
    let changed=match target{CollectionTarget::Analysis(id)=>sqlx::query("update knowledge.collection_items set position=$3 where collection_id=$1 and output_id=$2").bind(collection).bind(id).bind(as_i32(position)?).execute(&mut **tx).await,CollectionTarget::Source(id)=>sqlx::query("update knowledge.collection_items set position=$3 where collection_id=$1 and source_ref_id=$2").bind(collection).bind(id).bind(as_i32(position)?).execute(&mut **tx).await}.map_err(UserContentError::Database)?;
    if changed.rows_affected() == 1 {
        Ok(())
    } else {
        Err(UserContentError::NotFound)
    }
}
async fn touch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    collection: Uuid,
) -> Result<(), UserContentError> {
    sqlx::query("update knowledge.collections set updated_at=now() where collection_id=$1")
        .bind(collection)
        .execute(&mut **tx)
        .await
        .map_err(UserContentError::Database)?;
    Ok(())
}
