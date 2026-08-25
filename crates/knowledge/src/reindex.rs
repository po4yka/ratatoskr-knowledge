//! Explicit idempotent regeneration of embedding vectors.
//!
//! The reindex resolves the active identity from the provider, enumerates
//! projected sources whose vectors lack complete active-identity coverage or
//! carry rows under any other identity, and regenerates them one source at a
//! time with the same chunk-embed-persist transaction as the background
//! worker. Superseded-identity rows and failure entries are deleted only
//! inside the successful per-source transaction, `analysis_outputs` and run
//! history are never touched, and a fully converged database yields an empty
//! plan with zero provider calls.

use pgvector::Vector;
use uuid::Uuid;

use crate::chunking::{CHUNKING_VERSION, ChunkPolicy, chunk_article};
use crate::database::PersistenceError;
use crate::embeddings::EmbeddingProvider;
use crate::indexer::{
    EMBEDDING_STORAGE_DIMENSIONS, EmbeddingWrite, IndexingIdentity, IndexingTarget, input_groups,
    record_indexing_failure, store_embeddings,
};
use crate::provider::ProviderFailureClass;

/// Counts reported by one reindex execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReindexSummary {
    /// Sources fully regenerated under the active identity.
    pub sources_processed: usize,
    /// Sources skipped because their projection or provider call failed;
    /// each failure is recorded and nothing of the source was deleted.
    pub failures: usize,
}

/// Enumerates projected sources needing regeneration under `identity`.
///
/// A source enters the plan when it lacks any chunk under the active
/// identity tuple or carries at least one row under any other identity.
/// The plan is one statement, ordered by source identity, so repeated
/// planning over unchanged data is deterministic.
///
/// # Errors
///
/// Returns [`PersistenceError`] when the database read fails.
pub async fn plan_reindex<'e, E>(
    executor: E,
    identity: &IndexingIdentity,
) -> Result<Vec<Uuid>, PersistenceError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        "select distinct s.source_ref_id
         from knowledge.search_documents s
         where not exists (
                   select 1 from knowledge.embedding_chunks c
                   where c.source_ref_id = s.source_ref_id
                     and c.chunking_version = $1 and c.provider = $2
                     and c.model = $3 and c.prompt_version = $4
               )
            or exists (
                   select 1 from knowledge.embedding_chunks f
                   where f.source_ref_id = s.source_ref_id
                     and not (f.chunking_version = $1 and f.provider = $2
                              and f.model = $3 and f.prompt_version = $4)
               )
         order by s.source_ref_id asc",
    )
    .bind(CHUNKING_VERSION)
    .bind(&identity.provider)
    .bind(&identity.model)
    .bind(&identity.prompt_version)
    .fetch_all(executor)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(rows
        .into_iter()
        .map(|(source_ref_id,)| source_ref_id)
        .collect())
}

/// One projected source's facts loaded for regeneration.
#[derive(Debug)]
struct ReindexSource {
    run_id: Uuid,
    output_id: Uuid,
    tenant_ref: String,
    owner_context: String,
    document_id: Uuid,
    title: String,
    lead: String,
    body: String,
}

/// Raw selected shape for one reindex candidate row.
#[allow(clippy::type_complexity)]
type ReindexRow = (Uuid, Uuid, String, String, Uuid, String, String, String);

/// Loads one source's projection and latest accepted output ids.
///
/// # Errors
///
/// Returns [`PersistenceError`] when the database read fails.
async fn load_reindex_source<'e, E>(
    executor: E,
    source_ref_id: Uuid,
) -> Result<Option<ReindexSource>, PersistenceError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let row: Option<ReindexRow> = sqlx::query_as(
        "select r.run_id, o.output_id, s.tenant_ref, s.owner_context,
                s.document_id, s.title, s.lead, s.body
         from knowledge.search_documents s
         join knowledge.source_refs sr on sr.source_ref_id = s.source_ref_id
         join knowledge.analysis_outputs o on o.output_id = s.latest_output_id
         join knowledge.analysis_runs r on r.run_id = o.run_id
         where s.source_ref_id = $1",
    )
    .bind(source_ref_id)
    .fetch_optional(executor)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(row.map(
        |(run_id, output_id, tenant_ref, owner_context, document_id, title, lead, body)| {
            ReindexSource {
                run_id,
                output_id,
                tenant_ref,
                owner_context,
                document_id,
                title,
                lead,
                body,
            }
        },
    ))
}

/// Regenerates every planned source under the provider's active identity.
///
/// Each source is embedded in bounded input groups and persisted inside one
/// transaction that also prunes superseded-identity rows and clears the
/// source's failure entries. A provider or validation failure records a
/// bounded failure row and leaves the source's existing vectors untouched;
/// completed work stays persisted. `analysis_outputs` and run states are
/// never modified.
///
/// # Errors
///
/// Returns [`PersistenceError`] when persistence itself fails; provider
/// failures are captured as failure records instead.
pub async fn execute_reindex<P: EmbeddingProvider>(
    database: &crate::Database,
    provider: &P,
    policy: ChunkPolicy,
    max_input_characters: usize,
) -> Result<ReindexSummary, PersistenceError> {
    let provider_identity = provider.identity();
    let identity = IndexingIdentity {
        provider: provider_identity.provider.clone(),
        model: provider_identity.model.clone(),
        prompt_version: provider_identity.prompt_version.clone(),
    };
    let planned = plan_reindex(database.pool(), &identity).await?;
    let mut summary = ReindexSummary::default();
    for source_ref_id in planned {
        let Some(source) = load_reindex_source(database.pool(), source_ref_id).await? else {
            summary.failures += 1;
            continue;
        };
        let target = IndexingTarget {
            run_id: source.run_id,
            source_ref_id,
            output_id: source.output_id,
            tenant_ref: source.tenant_ref.clone(),
            owner_context: source.owner_context.clone(),
            document_id: source.document_id,
        };
        if regenerate_source(
            database,
            provider,
            policy,
            max_input_characters,
            &identity,
            &target,
            &source,
        )
        .await?
        {
            summary.sources_processed += 1;
        } else {
            summary.failures += 1;
        }
    }
    Ok(summary)
}

/// Regenerates one source; `Ok(false)` means a recorded failure.
async fn regenerate_source<P: EmbeddingProvider>(
    database: &crate::Database,
    provider: &P,
    policy: ChunkPolicy,
    max_input_characters: usize,
    identity: &IndexingIdentity,
    target: &IndexingTarget,
    source: &ReindexSource,
) -> Result<bool, PersistenceError> {
    let chunks = chunk_article(&source.title, &source.lead, &source.body, policy);
    if chunks.is_empty() {
        return Ok(false);
    }
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(chunks.len());
    for group in input_groups(
        chunks.iter().map(|chunk| chunk.text.clone()).collect(),
        max_input_characters,
    ) {
        let response = match provider.embed(group).await {
            Ok(response) => response,
            Err(failure) => {
                tracing::warn!(
                    operation = "embedding_reindex",
                    outcome = "provider_failure",
                    failure_class = failure.class.as_str(),
                    http_status = failure.http_status,
                    "reindex embed failed"
                );
                return record_failure(database, identity, target, failure.class)
                    .await
                    .map(|()| false);
            }
        };
        vectors.extend(response.vectors);
    }
    if vectors.len() != chunks.len()
        || vectors.iter().any(|vector| {
            vector.len() != usize::try_from(EMBEDDING_STORAGE_DIMENSIONS).unwrap_or(0)
        })
    {
        return record_failure(
            database,
            identity,
            target,
            ProviderFailureClass::RequestInvalid,
        )
        .await
        .map(|()| false);
    }
    let writes: Vec<EmbeddingWrite> = chunks
        .into_iter()
        .zip(vectors)
        .map(|(chunk, vector)| EmbeddingWrite {
            ordinal: chunk.ordinal,
            chunk_text: chunk.text,
            digest_hex: chunk.digest_hex,
            vector: Vector::from(vector),
        })
        .collect();
    let mut transaction = database
        .pool()
        .begin()
        .await
        .map_err(PersistenceError::Query)?;
    store_embeddings(&mut transaction, identity, target, writes).await?;
    sqlx::query(
        "delete from knowledge.embedding_chunks
         where source_ref_id = $1
           and not (chunking_version = $2 and provider = $3
                    and model = $4 and prompt_version = $5)",
    )
    .bind(target.source_ref_id)
    .bind(CHUNKING_VERSION)
    .bind(&identity.provider)
    .bind(&identity.model)
    .bind(&identity.prompt_version)
    .execute(&mut *transaction)
    .await
    .map_err(PersistenceError::Query)?;
    sqlx::query("delete from knowledge.embedding_failures where source_ref_id = $1")
        .bind(target.source_ref_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
    transaction
        .commit()
        .await
        .map_err(PersistenceError::Query)?;
    Ok(true)
}

/// Records one bounded reindex failure without deleting anything.
async fn record_failure(
    database: &crate::Database,
    identity: &IndexingIdentity,
    target: &IndexingTarget,
    class: ProviderFailureClass,
) -> Result<(), PersistenceError> {
    let mut transaction = database
        .pool()
        .begin()
        .await
        .map_err(PersistenceError::Query)?;
    record_indexing_failure(&mut transaction, identity, target, class).await?;
    transaction
        .commit()
        .await
        .map_err(PersistenceError::Query)?;
    Ok(())
}
