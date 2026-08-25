//! Durable-state-driven embedding indexing over accepted analyses.
//!
//! The indexer selects runs resting at [`crate::RunState::Persisted`],
//! chunks their projected text under the active versioned policy,
//! embeds through any [`EmbeddingProvider`] (production wraps the
//! controlled seam), persists vectors with full model identity, and
//! performs the single guarded `persisted -> indexed` transition
//! inside one transaction per source. Failures are recorded as bounded
//! per-identity rows and never erase an accepted result.

use pgvector::Vector;
use uuid::Uuid;

use crate::chunking::{CHUNKING_VERSION, ChunkPolicy, chunk_article};
use crate::database::PersistenceError;
use crate::embeddings::EmbeddingProvider;
use crate::provider::ProviderFailureClass;

/// Storage dimensionality mirrored from the fixed vector column typmod.
pub const EMBEDDING_STORAGE_DIMENSIONS: i32 = 1536;

/// Model-version identity bound to every written vector row.
#[derive(Debug, Clone)]
pub struct IndexingIdentity {
    /// Stable provider identifier recorded on each row.
    pub provider: String,
    /// Model identifier recorded on each row.
    pub model: String,
    /// Prompt-version label recorded on each row.
    pub prompt_version: String,
}

/// The source-side facts one indexing transaction needs.
#[derive(Debug, Clone)]
pub struct IndexingTarget {
    /// Run receiving the `persisted -> indexed` transition.
    pub run_id: Uuid,
    /// Owning source revision.
    pub source_ref_id: Uuid,
    /// Accepted output the vectors derive from.
    pub output_id: Uuid,
    /// Canonical tenant text copied onto every row.
    pub tenant_ref: String,
    /// Owner context copied onto the projection.
    pub owner_context: String,
    /// Document identifier copied onto the projection.
    pub document_id: Uuid,
}

/// One chunk ready for persistence.
#[derive(Debug, Clone)]
pub struct EmbeddingWrite {
    /// Stable position within the chunk sequence.
    pub ordinal: usize,
    /// Exact stored chunk text including title prefix.
    pub chunk_text: String,
    /// Lowercase SHA-256 of the chunk text.
    pub digest_hex: String,
    /// Vector matching [`EMBEDDING_STORAGE_DIMENSIONS`].
    pub vector: Vector,
}

/// One candidate selected by durable state.
#[derive(Debug, Clone)]
pub struct PendingSource {
    /// Run currently resting at `persisted`.
    pub run_id: Uuid,
    /// Owning source revision.
    pub source_ref_id: Uuid,
    /// Latest accepted output for the source.
    pub output_id: Uuid,
    /// Document identifier of the projection.
    pub document_id: Uuid,
    /// Owner context of the projection.
    pub owner_context: String,
    /// Canonical tenant text of the projection.
    pub tenant_ref: String,
    /// Projected title, empty when unprojected.
    pub title: String,
    /// Projected lead, empty when unprojected.
    pub lead: String,
    /// Projected body, empty when unprojected.
    pub body: String,
    /// Whether a searchable projection exists at all.
    pub has_projection: bool,
}

/// Bounds configuring one indexer instance.
#[derive(Debug, Clone, Copy)]
pub struct IndexerLimits {
    /// Sources processed per pass.
    pub batch_sources: usize,
    /// Maximum characters sent to the provider in one request.
    pub max_input_characters: usize,
    /// Failure attempts allowed before a source waits for explicit reset.
    pub max_failure_attempts: i32,
}

/// Counters reported by one indexing pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexingOutcome {
    /// Sources embedded and transitioned to `indexed`.
    pub indexed: usize,
    /// Sources skipped for lacking a searchable projection.
    pub skipped_without_projection: usize,
    /// Sources whose provider call or validation failed and was recorded.
    pub failed: usize,
    /// Sources skipped because their failure attempts reached the bound.
    pub bound_skipped: usize,
}

/// Raw selected shape for one pending candidate row.
#[allow(clippy::type_complexity)]
type PendingRow = (
    Uuid,
    Uuid,
    Uuid,
    Option<Uuid>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    bool,
);

/// Selects the oldest persisted runs up to `limit`.
///
/// # Errors
///
/// Returns [`PersistenceError`] when the database read fails.
pub async fn pending_indexing_batch<'e, E>(
    executor: E,
    limit: i64,
) -> Result<Vec<PendingSource>, PersistenceError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rows: Vec<PendingRow> = sqlx::query_as(
        "select r.run_id, r.source_ref_id, o.output_id, s.document_id,
                s.owner_context, s.tenant_ref,
                s.title, s.lead, s.body,
                (s.search_document_id is not null)
         from knowledge.analysis_runs r
         join knowledge.analysis_outputs o on o.run_id = r.run_id
         left join knowledge.search_documents s on s.latest_output_id = o.output_id
         where r.state = 'persisted'
         order by r.updated_at asc, r.run_id asc
         limit $1",
    )
    .bind(limit)
    .fetch_all(executor)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(rows
        .into_iter()
        .map(
            |(
                run_id,
                source_ref_id,
                output_id,
                document_id,
                owner_context,
                tenant_ref,
                title,
                lead,
                body,
                has_projection,
            )| {
                PendingSource {
                    run_id,
                    source_ref_id,
                    output_id,
                    document_id: document_id.unwrap_or_default(),
                    owner_context: owner_context.unwrap_or_default(),
                    tenant_ref: tenant_ref.unwrap_or_default(),
                    title: title.unwrap_or_default(),
                    lead: lead.unwrap_or_default(),
                    body: body.unwrap_or_default(),
                    has_projection,
                }
            },
        )
        .collect())
}

/// Persists one source's vectors under an exact model identity.
///
/// Upserts every chunk keyed by source, chunking version, provider,
/// model, prompt version, and ordinal; prunes stale higher ordinals;
/// and clears any prior failure entry for the same identity. Callers
/// commit this alongside the guarded run transition so a source moves
/// atomically or not at all.
///
/// # Errors
///
/// Returns [`PersistenceError`] when persistence fails; the caller's
/// transaction rolls back, leaving the previous state intact.
pub async fn store_embeddings(
    executor: &mut sqlx::PgConnection,
    identity: &IndexingIdentity,
    target: &IndexingTarget,
    writes: Vec<EmbeddingWrite>,
) -> Result<(), PersistenceError> {
    for write in &writes {
        sqlx::query(
            "insert into knowledge.embedding_chunks (
                 embedding_chunk_id, source_ref_id, output_id, tenant_ref,
                 owner_context, document_id, ordinal, chunk_text,
                 chunk_digest_hex, chunking_version, provider, model,
                 dimensions, prompt_version, embedding
             )
             values ($1, $6, $7, $8, $9, $10, $11, $12, $13,
                     $2, $3, $4, $14, $5, $15)
             on conflict (source_ref_id, chunking_version, provider,
                          model, prompt_version, ordinal)
             do update set output_id = excluded.output_id,
                           tenant_ref = excluded.tenant_ref,
                           owner_context = excluded.owner_context,
                           document_id = excluded.document_id,
                           chunk_text = excluded.chunk_text,
                           chunk_digest_hex = excluded.chunk_digest_hex,
                           dimensions = excluded.dimensions,
                           embedding = excluded.embedding,
                           created_at = now()",
        )
        .bind(Uuid::now_v7())
        .bind(CHUNKING_VERSION)
        .bind(&identity.provider)
        .bind(&identity.model)
        .bind(&identity.prompt_version)
        .bind(target.source_ref_id)
        .bind(target.output_id)
        .bind(&target.tenant_ref)
        .bind(&target.owner_context)
        .bind(target.document_id)
        .bind(i32::try_from(write.ordinal).map_err(|error| {
            PersistenceError::Query(sqlx::Error::ColumnDecode {
                index: "ordinal".to_owned(),
                source: Box::new(error),
            })
        })?)
        .bind(&write.chunk_text)
        .bind(&write.digest_hex)
        .bind(EMBEDDING_STORAGE_DIMENSIONS)
        .bind(write.vector.clone())
        .execute(&mut *executor)
        .await
        .map_err(PersistenceError::Query)?;
    }
    let ordinal_bound = i32::try_from(writes.len()).unwrap_or(i32::MAX);
    sqlx::query(
        "delete from knowledge.embedding_chunks c
         where c.source_ref_id = $1
           and c.chunking_version = $2 and c.provider = $3
           and c.model = $4 and c.prompt_version = $5
           and c.ordinal >= $6",
    )
    .bind(target.source_ref_id)
    .bind(CHUNKING_VERSION)
    .bind(&identity.provider)
    .bind(&identity.model)
    .bind(&identity.prompt_version)
    .bind(ordinal_bound)
    .execute(&mut *executor)
    .await
    .map_err(PersistenceError::Query)?;
    sqlx::query(
        "delete from knowledge.embedding_failures f
         where f.source_ref_id = $1
           and f.chunking_version = $2 and f.provider = $3
           and f.model = $4 and f.prompt_version = $5",
    )
    .bind(target.source_ref_id)
    .bind(CHUNKING_VERSION)
    .bind(&identity.provider)
    .bind(&identity.model)
    .bind(&identity.prompt_version)
    .execute(&mut *executor)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(())
}

/// Returns the recorded failure attempt count for one source identity.
///
/// # Errors
///
/// Returns [`PersistenceError`] when the database read fails.
pub async fn failure_attempt_count<'e, E>(
    executor: E,
    identity: &IndexingIdentity,
    source_ref_id: Uuid,
) -> Result<i32, PersistenceError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let (attempt,): (Option<i32>,) = sqlx::query_as(
        "select f.attempt from knowledge.embedding_failures f
         where f.source_ref_id = $1
           and f.chunking_version = $2 and f.provider = $3
           and f.model = $4 and f.prompt_version = $5",
    )
    .bind(source_ref_id)
    .bind(CHUNKING_VERSION)
    .bind(&identity.provider)
    .bind(&identity.model)
    .bind(&identity.prompt_version)
    .fetch_optional(executor)
    .await
    .map_err(PersistenceError::Query)?
    .unwrap_or((None,));
    Ok(attempt.unwrap_or(0))
}

/// Records one indexing failure, upserting the per-identity row in place.
///
/// Returns the new attempt count. Storage stays bounded: one row per
/// source and model identity regardless of how many attempts occur.
///
/// # Errors
///
/// Returns [`PersistenceError`] when persistence fails.
pub async fn record_indexing_failure(
    executor: &mut sqlx::PgConnection,
    identity: &IndexingIdentity,
    target: &IndexingTarget,
    class: ProviderFailureClass,
) -> Result<i32, PersistenceError> {
    let (attempt,): (i32,) = sqlx::query_as(
        "insert into knowledge.embedding_failures (
             failure_id, source_ref_id, output_id, tenant_ref,
             chunking_version, provider, model, prompt_version,
             error_class, attempt
         )
         values ($1, $6, $7, $8, $2, $3, $4, $5, $9, 1)
         on conflict (source_ref_id, chunking_version, provider,
                      model, prompt_version)
         do update set error_class = excluded.error_class,
                       output_id = excluded.output_id,
                       updated_at = now(),
                       attempt = knowledge.embedding_failures.attempt + 1
         returning attempt",
    )
    .bind(Uuid::now_v7())
    .bind(CHUNKING_VERSION)
    .bind(&identity.provider)
    .bind(&identity.model)
    .bind(&identity.prompt_version)
    .bind(target.source_ref_id)
    .bind(target.output_id)
    .bind(&target.tenant_ref)
    .bind(class.as_str())
    .fetch_one(executor)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(attempt)
}

/// One pass of the bounded background embedding step.
#[derive(Debug)]
pub struct Indexer<P> {
    database: crate::Database,
    provider: P,
    policy: ChunkPolicy,
    limits: IndexerLimits,
}

impl<P: EmbeddingProvider> Indexer<P> {
    /// Builds an indexer over one provider under explicit bounds.
    ///
    /// Production passes a controlled embeddings wrapper so budget,
    /// rate, deadline, and size caps apply to every call; tests pass
    /// the scripted fake directly.
    pub fn new(
        database: &crate::Database,
        provider: P,
        policy: ChunkPolicy,
        limits: IndexerLimits,
    ) -> Self {
        Self {
            database: database.clone(),
            provider,
            policy,
            limits,
        }
    }

    /// Processes at most one batch of pending sources.
    ///
    /// Selection reads only durable state, so interruption anywhere
    /// converges on the next pass. Provider failures are recorded as
    /// bounded failure rows and never erase an accepted result.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when persistence itself fails;
    /// provider failures are captured as failure records instead.
    pub async fn process_pending(&self) -> Result<IndexingOutcome, PersistenceError> {
        let mut outcome = IndexingOutcome::default();
        let batch = pending_indexing_batch(
            self.database.pool(),
            i64::try_from(self.limits.batch_sources).unwrap_or(i64::MAX),
        )
        .await?;
        let provider_identity = self.provider.identity();
        let identity = IndexingIdentity {
            provider: provider_identity.provider.clone(),
            model: provider_identity.model.clone(),
            prompt_version: provider_identity.prompt_version.clone(),
        };
        for source in batch {
            if !source.has_projection {
                outcome.skipped_without_projection += 1;
                continue;
            }
            let target = IndexingTarget {
                run_id: source.run_id,
                source_ref_id: source.source_ref_id,
                output_id: source.output_id,
                tenant_ref: source.tenant_ref.clone(),
                owner_context: source.owner_context.clone(),
                document_id: source.document_id,
            };
            let attempts =
                failure_attempt_count(self.database.pool(), &identity, source.source_ref_id)
                    .await?;
            if attempts >= self.limits.max_failure_attempts {
                outcome.bound_skipped += 1;
                continue;
            }
            if self.index_source(&source, &target, &identity).await? {
                outcome.indexed += 1;
            } else {
                outcome.failed += 1;
            }
        }
        Ok(outcome)
    }

    /// Embeds and persists one source; `Ok(false)` means a recorded failure.
    async fn index_source(
        &self,
        source: &PendingSource,
        target: &IndexingTarget,
        identity: &IndexingIdentity,
    ) -> Result<bool, PersistenceError> {
        let chunks = chunk_article(&source.title, &source.lead, &source.body, self.policy);
        if chunks.is_empty() {
            return Ok(false);
        }
        let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(chunks.len());
        for group in input_groups(
            chunks.iter().map(|chunk| chunk.text.clone()).collect(),
            self.limits.max_input_characters,
        ) {
            let response = match self.provider.embed(group).await {
                Ok(response) => response,
                Err(failure) => {
                    tracing::warn!(
                        operation = "embedding_indexing",
                        outcome = "provider_failure",
                        failure_class = failure.class.as_str(),
                        http_status = failure.http_status,
                        "indexing embed failed"
                    );
                    let mut transaction = self
                        .database
                        .pool()
                        .begin()
                        .await
                        .map_err(PersistenceError::Query)?;
                    record_indexing_failure(&mut transaction, identity, target, failure.class)
                        .await?;
                    transaction
                        .commit()
                        .await
                        .map_err(PersistenceError::Query)?;
                    return Ok(false);
                }
            };
            vectors.extend(response.vectors);
        }
        if vectors.len() != chunks.len()
            || vectors.iter().any(|vector| {
                vector.len() != usize::try_from(EMBEDDING_STORAGE_DIMENSIONS).unwrap_or(0)
            })
        {
            let mut transaction = self
                .database
                .pool()
                .begin()
                .await
                .map_err(PersistenceError::Query)?;
            record_indexing_failure(
                &mut transaction,
                identity,
                target,
                ProviderFailureClass::RequestInvalid,
            )
            .await?;
            transaction
                .commit()
                .await
                .map_err(PersistenceError::Query)?;
            return Ok(false);
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
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        store_embeddings(&mut transaction, identity, target, writes).await?;
        let transitioned = sqlx::query(
            "update knowledge.analysis_runs set state = 'indexed', updated_at = now()
             where run_id = $1 and state = 'persisted'",
        )
        .bind(target.run_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if transitioned.rows_affected() != 1 {
            return Err(PersistenceError::Query(sqlx::Error::RowNotFound));
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(true)
    }
}

/// Splits inputs into groups whose total character count stays within
/// `max_characters`; single oversized inputs travel alone.
pub(crate) fn input_groups(inputs: Vec<String>, max_characters: usize) -> Vec<Vec<String>> {
    let mut groups: Vec<Vec<String>> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut current_chars = 0_usize;
    for input in inputs {
        let chars = input.chars().count();
        if !current.is_empty() && current_chars + chars > max_characters {
            groups.push(std::mem::take(&mut current));
            current_chars = 0;
        }
        current_chars += chars;
        current.push(input);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}
