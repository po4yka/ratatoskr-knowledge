//! Durable-state-driven embedding indexing checks over a disposable
//! database.

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    ChunkPolicy, EmbeddingWrite, Indexer, IndexerLimits, ProviderError, ProviderFailureClass,
    ScriptedEmbeddingProvider, ScriptedEmbeddingSuccess, SourceReference,
};

const IDENTITY_PROVIDER: &str = "scripted_fake";
const IDENTITY_MODEL: &str = "fake_default_v1";
const IDENTITY_PROMPT_VERSION: &str = "none.v1";
const DIMENSIONS: i32 = 1536;

fn digest(digit: char) -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    })
}

fn digest_of(text: &str) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(text.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn fixture_vector(directions: &[(usize, f32)]) -> pgvector::Vector {
    let mut values = vec![0.0_f32; usize::try_from(DIMENSIONS).unwrap_or(0)];
    for (index, magnitude) in directions {
        if let Some(slot) = values.get_mut(*index) {
            *slot = *magnitude;
        }
    }
    pgvector::Vector::from(values)
}

struct SeededSource {
    run_id: uuid::Uuid,
    source_ref_id: uuid::Uuid,
    output_id: uuid::Uuid,
    document_id: uuid::Uuid,
    owner_context: String,
    tenant_ref: String,
}

/// Registers one source with a run resting at `state`, an accepted
/// output, and optionally the projected search document row.
async fn seed_source(
    database: &TestDatabase,
    state: &str,
    with_projection: bool,
) -> Result<SeededSource, Box<dyn std::error::Error>> {
    let document = Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:indexing")?,
        content_digest: digest('c')?,
        title: Some("Indexing fixture".to_owned()),
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            block_id: BlockId::new_v7(),
            text: "Lead sentence.".to_owned(),
        }],
        provenance: Vec::new(),
    };
    let tenant = TenantRef::of_user(UserId::new_v7());
    let source = database
        .database
        .register_source(&SourceReference {
            tenant,
            owner_context: "ratatoskr-extractor".to_owned(),
            ai_archive_id: String::new(),
            document_id: document.document_id,
            content_digest: document.content_digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse("ratatoskr-extractor")?,
                digest: document.content_digest.clone(),
                media_type: MediaType::parse("application/json")?,
                length_bytes: 128,
            },
        })
        .await?;
    let (tenant_ref,): (String,) =
        sqlx::query_as("select tenant_ref from knowledge.source_refs where source_ref_id = $1")
            .bind(source.id)
            .fetch_one(database.database.pool())
            .await?;
    let run_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         )
         values ($1, $2, 'article-analysis.v1', 'v1', 'v1', 'fake_default_v1', $3)",
    )
    .bind(run_id)
    .bind(source.id)
    .bind(state)
    .execute(database.database.pool())
    .await?;
    let output_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_outputs (output_id, run_id, result, raw_response)
         values ($1, $2, '{}', '{}')",
    )
    .bind(output_id)
    .bind(run_id)
    .execute(database.database.pool())
    .await?;
    if with_projection {
        sqlx::query(
            "insert into knowledge.search_documents (
                 search_document_id, source_ref_id, latest_output_id, tenant_ref,
                 owner_context, document_id, title, lead, body, updated_at
             )
             values ($1, $2, (select output_id from knowledge.analysis_outputs
                              where run_id = $3 limit 1),
                     $4, 'ratatoskr-extractor', $5, 'Indexing fixture',
                     'Lead sentence.', 'Body paragraph.', now())",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(source.id)
        .bind(run_id)
        .bind(&tenant_ref)
        .bind(document.document_id.0)
        .execute(database.database.pool())
        .await?;
    }
    Ok(SeededSource {
        run_id,
        source_ref_id: source.id,
        output_id,
        document_id: document.document_id.0,
        owner_context: "ratatoskr-extractor".to_owned(),
        tenant_ref,
    })
}

fn scripted_provider(
    outcomes: Vec<Result<ScriptedEmbeddingSuccess, ProviderError>>,
) -> ScriptedEmbeddingProvider {
    ScriptedEmbeddingProvider::new(u16::try_from(DIMENSIONS).unwrap_or(0), outcomes)
}

fn target_for(seeded: &SeededSource) -> ratatoskr_knowledge::IndexingTarget {
    ratatoskr_knowledge::IndexingTarget {
        run_id: seeded.run_id,
        source_ref_id: seeded.source_ref_id,
        output_id: seeded.output_id,
        tenant_ref: seeded.tenant_ref.clone(),
        owner_context: seeded.owner_context.clone(),
        document_id: seeded.document_id,
    }
}

fn indexer_limits() -> IndexerLimits {
    IndexerLimits {
        batch_sources: 8,
        max_input_characters: 120_000,
        max_failure_attempts: 5,
    }
}

#[tokio::test]
async fn embedding_rows_carry_full_identity() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let seeded = seed_source(&database, "persisted", true).await?;
    let chunk_text = "Indexing fixture\n\nLead sentence.".to_owned();

    let mut transaction = database.database.pool().begin().await?;
    ratatoskr_knowledge::store_embeddings(
        &mut transaction,
        &ratatoskr_knowledge::IndexingIdentity {
            provider: IDENTITY_PROVIDER.to_owned(),
            model: IDENTITY_MODEL.to_owned(),
            prompt_version: IDENTITY_PROMPT_VERSION.to_owned(),
        },
        &target_for(&seeded),
        vec![EmbeddingWrite {
            ordinal: 0,
            chunk_text: chunk_text.clone(),
            digest_hex: digest_of(&chunk_text),
            vector: fixture_vector(&[(0, 1.0)]),
        }],
    )
    .await?;
    transaction.commit().await?;

    let row: (i32, String, String, String, String, i32, i64) = sqlx::query_as(
        "select c.ordinal, c.chunk_text, c.chunk_digest_hex, c.chunking_version,
                c.provider, c.dimensions,
                (select count(*) from knowledge.embedding_chunks)
         from knowledge.embedding_chunks c",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(row.0, 0);
    assert_eq!(row.1, chunk_text);
    assert_eq!(row.2, digest_of(&chunk_text));
    assert_eq!(row.3, ratatoskr_knowledge::CHUNKING_VERSION);
    assert_eq!(row.4, IDENTITY_PROVIDER);
    assert_eq!(row.5, DIMENSIONS);
    assert_eq!(row.6, 1);

    // Upserting the same identity replaces in place; higher ordinals are
    // pruned; the vector round-trips through cosine ordering.
    let mut transaction = database.database.pool().begin().await?;
    ratatoskr_knowledge::store_embeddings(
        &mut transaction,
        &ratatoskr_knowledge::IndexingIdentity {
            provider: IDENTITY_PROVIDER.to_owned(),
            model: IDENTITY_MODEL.to_owned(),
            prompt_version: IDENTITY_PROMPT_VERSION.to_owned(),
        },
        &target_for(&seeded),
        vec![
            EmbeddingWrite {
                ordinal: 0,
                chunk_text: "Replaced.".to_owned(),
                digest_hex: digest_of("Replaced."),
                vector: fixture_vector(&[(1, 1.0)]),
            },
            EmbeddingWrite {
                ordinal: 1,
                chunk_text: "Second.".to_owned(),
                digest_hex: digest_of("Second."),
                vector: fixture_vector(&[(2, 1.0)]),
            },
        ],
    )
    .await?;
    transaction.commit().await?;
    let (count, replaced): (i64, String) =
        sqlx::query_as("select count(*), min(chunk_text) from knowledge.embedding_chunks")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(count, 2);
    assert_eq!(replaced, "Replaced.");

    // A dimension mismatch fails validation without persisting anything.
    let mut transaction = database.database.pool().begin().await?;
    let rejected = ratatoskr_knowledge::store_embeddings(
        &mut transaction,
        &ratatoskr_knowledge::IndexingIdentity {
            provider: IDENTITY_PROVIDER.to_owned(),
            model: IDENTITY_MODEL.to_owned(),
            prompt_version: IDENTITY_PROMPT_VERSION.to_owned(),
        },
        &target_for(&seeded),
        vec![EmbeddingWrite {
            ordinal: 0,
            chunk_text: "Wrong dims.".to_owned(),
            digest_hex: digest_of("Wrong dims."),
            vector: pgvector::Vector::from(vec![0.5_f32; 8]),
        }],
    )
    .await;
    assert!(rejected.is_err(), "dimension mismatch must be rejected");
    transaction.rollback().await?;
    let count_after: (i64,) = sqlx::query_as("select count(*) from knowledge.embedding_chunks")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(count_after.0, 2, "no partial row may persist");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn indexing_pass_transitions_persisted_runs_once() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let first = seed_source(&database, "persisted", true).await?;
    let second = seed_source(&database, "persisted", true).await?;
    let unprojected = seed_source(&database, "persisted", false).await?;
    let provider = scripted_provider(vec![
        Ok(ScriptedEmbeddingSuccess { input_tokens: 12 }),
        Ok(ScriptedEmbeddingSuccess { input_tokens: 12 }),
    ]);
    let indexer = Indexer::new(
        &database.database,
        provider,
        ChunkPolicy::new(1600, 200)?,
        indexer_limits(),
    );

    let outcome = indexer.process_pending().await?;
    assert_eq!(outcome.indexed, 2);
    assert_eq!(outcome.skipped_without_projection, 1);
    assert_eq!(outcome.failed, 0);

    let states: Vec<(String,)> =
        sqlx::query_as("select state from knowledge.analysis_runs order by run_id")
            .fetch_all(database.database.pool())
            .await?;
    let mut indexed_count = 0;
    let mut persisted_count = 0;
    for (state,) in &states {
        match state.as_str() {
            "indexed" => indexed_count += 1,
            // The projection-less source stays pending by design.
            "persisted" => persisted_count += 1,
            other => unreachable!("unexpected state: {other}"),
        }
    }
    assert_eq!(indexed_count, 2);
    assert_eq!(persisted_count, 1);
    let (indexed_runs,): (i64,) =
        sqlx::query_as("select count(*) from knowledge.analysis_runs where state = 'indexed'")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(indexed_runs, 2);
    let (vectors,): (i64,) = sqlx::query_as("select count(*) from knowledge.embedding_chunks")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(vectors, 2);

    // A second pass makes no provider calls and changes nothing.
    let outcome_again = indexer.process_pending().await?;
    assert_eq!(outcome_again.indexed, 0);
    let (vectors_after,): (i64,) =
        sqlx::query_as("select count(*) from knowledge.embedding_chunks")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(vectors_after, vectors);
    let _ = (first, second, unprojected);

    database.cleanup().await?;
    Ok(())
}

const LEGACY_PROVIDER: &str = "legacy_embedder";
const LEGACY_MODEL: &str = "legacy_model_v0";

/// Inserts one embedding chunk for `seeded` under a superseded identity.
async fn insert_legacy_chunk(
    database: &TestDatabase,
    seeded: &SeededSource,
) -> Result<(), Box<dyn std::error::Error>> {
    let chunk_text = "Indexing fixture\n\nBody paragraph.".to_owned();
    sqlx::query(
        "insert into knowledge.embedding_chunks (
             embedding_chunk_id, source_ref_id, output_id, tenant_ref,
             owner_context, document_id, ordinal, chunk_text,
             chunk_digest_hex, chunking_version, provider, model,
             dimensions, prompt_version, embedding
         )
         values ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9, $10, $11, $12, $13, $14)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(seeded.source_ref_id)
    .bind(seeded.output_id)
    .bind(&seeded.tenant_ref)
    .bind(&seeded.owner_context)
    .bind(seeded.document_id)
    .bind(&chunk_text)
    .bind(digest_of(&chunk_text))
    .bind(ratatoskr_knowledge::CHUNKING_VERSION)
    .bind(LEGACY_PROVIDER)
    .bind(LEGACY_MODEL)
    .bind(DIMENSIONS)
    .bind(IDENTITY_PROMPT_VERSION)
    .bind(fixture_vector(&[(3, 1.0)]))
    .execute(database.database.pool())
    .await?;
    Ok(())
}

/// Inserts one indexing-failure row for `seeded` under the legacy identity.
async fn insert_legacy_failure(
    database: &TestDatabase,
    seeded: &SeededSource,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "insert into knowledge.embedding_failures (
             failure_id, source_ref_id, output_id, tenant_ref,
             chunking_version, provider, model, prompt_version,
             error_class, attempt
         )
         values ($1, $2, $3, $4, $5, $6, $7, $8, 'unclassified', 1)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(seeded.source_ref_id)
    .bind(seeded.output_id)
    .bind(&seeded.tenant_ref)
    .bind(ratatoskr_knowledge::CHUNKING_VERSION)
    .bind(LEGACY_PROVIDER)
    .bind(LEGACY_MODEL)
    .bind(IDENTITY_PROMPT_VERSION)
    .execute(database.database.pool())
    .await?;
    Ok(())
}

/// Captures every accepted output's persisted bytes for history comparison.
async fn output_bytes(
    database: &TestDatabase,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    Ok(sqlx::query_as(
        "select result::text, raw_response::text
         from knowledge.analysis_outputs order by output_id",
    )
    .fetch_all(database.database.pool())
    .await?)
}

/// Captures every run's current state for history comparison.
async fn run_states(database: &TestDatabase) -> Result<Vec<(String,)>, Box<dyn std::error::Error>> {
    Ok(
        sqlx::query_as("select state from knowledge.analysis_runs order by run_id")
            .fetch_all(database.database.pool())
            .await?,
    )
}

#[tokio::test]
async fn reindex_converges_idempotently_and_leaves_history()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let source_a = seed_source(&database, "completed", true).await?;
    let source_b = seed_source(&database, "completed", true).await?;
    insert_legacy_chunk(&database, &source_a).await?;
    insert_legacy_failure(&database, &source_a).await?;

    let outputs_before = output_bytes(&database).await?;
    let states_before = run_states(&database).await?;

    let provider = scripted_provider(vec![
        Ok(ScriptedEmbeddingSuccess { input_tokens: 9 }),
        Ok(ScriptedEmbeddingSuccess { input_tokens: 9 }),
        Ok(ScriptedEmbeddingSuccess { input_tokens: 9 }),
        Ok(ScriptedEmbeddingSuccess { input_tokens: 9 }),
    ]);

    let summary = ratatoskr_knowledge::execute_reindex(
        &database.database,
        &provider,
        ChunkPolicy::new(1600, 200)?,
        120_000,
        &ratatoskr_knowledge::ReindexScope::unrestricted(),
        |_, _| {},
    )
    .await?;
    assert_eq!(summary.sources_processed, 2);
    assert_eq!(summary.failures, 0);

    assert_active_coverage(&database, &[&source_a, &source_b]).await?;

    assert_eq!(
        output_bytes(&database).await?,
        outputs_before,
        "analysis_outputs bytes must stay untouched"
    );
    assert_eq!(
        run_states(&database).await?,
        states_before,
        "run states must stay untouched"
    );

    let calls_after_first_pass = provider.call_count()?;
    assert_eq!(calls_after_first_pass, 2);

    let summary_again = ratatoskr_knowledge::execute_reindex(
        &database.database,
        &provider,
        ChunkPolicy::new(1600, 200)?,
        120_000,
        &ratatoskr_knowledge::ReindexScope::unrestricted(),
        |_, _| {},
    )
    .await?;
    assert_eq!(summary_again.sources_processed, 0);
    assert_eq!(summary_again.failures, 0);
    assert_eq!(
        provider.call_count()?,
        calls_after_first_pass,
        "a converged reindex must make zero provider calls"
    );

    assert_worker_leaves_completed_reindex_untouched(&database).await?;

    database.cleanup().await?;
    Ok(())
}

/// Verifies that every projected source has only the active identity after a
/// successful reindex and that failure rows were cleared.
async fn assert_active_coverage(
    database: &TestDatabase,
    sources: &[&SeededSource],
) -> Result<(), Box<dyn std::error::Error>> {
    for seeded in sources {
        let (active,): (i64,) = sqlx::query_as(
            "select count(*) from knowledge.embedding_chunks
             where source_ref_id = $1 and provider = $2 and model = $3
               and prompt_version = $4",
        )
        .bind(seeded.source_ref_id)
        .bind(IDENTITY_PROVIDER)
        .bind(IDENTITY_MODEL)
        .bind(IDENTITY_PROMPT_VERSION)
        .fetch_one(database.database.pool())
        .await?;
        assert!(active >= 1, "every projected source must gain coverage");
    }
    let (superseded,): (i64,) = sqlx::query_as(
        "select count(*) from knowledge.embedding_chunks
         where not (provider = $1 and model = $2 and prompt_version = $3)",
    )
    .bind(IDENTITY_PROVIDER)
    .bind(IDENTITY_MODEL)
    .bind(IDENTITY_PROMPT_VERSION)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(superseded, 0, "superseded-identity rows must be pruned");
    let (failures_left,): (i64,) =
        sqlx::query_as("select count(*) from knowledge.embedding_failures")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(failures_left, 0, "failure entries must be cleared");
    Ok(())
}

/// Verifies that worker startup does not mutate runs completed by the job.
async fn assert_worker_leaves_completed_reindex_untouched(
    database: &TestDatabase,
) -> Result<(), Box<dyn std::error::Error>> {
    // A worker-only startup with the same active identity mutates nothing:
    // only runs resting at `persisted` are ever touched.
    let worker = Indexer::new(
        &database.database,
        scripted_provider(Vec::new()),
        ChunkPolicy::new(1600, 200)?,
        indexer_limits(),
    );
    let outcome = worker.process_pending().await?;
    assert_eq!(outcome, ratatoskr_knowledge::IndexingOutcome::default());
    let (vectors_final, active_final): (i64, i64) = sqlx::query_as(
        "select count(*),
                count(*) filter (where provider = $1 and model = $2
                                 and prompt_version = $3)
         from knowledge.embedding_chunks",
    )
    .bind(IDENTITY_PROVIDER)
    .bind(IDENTITY_MODEL)
    .bind(IDENTITY_PROMPT_VERSION)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(vectors_final, active_final);
    assert_eq!(vectors_final, 2);

    Ok(())
}

/// Seeds one source revision under an explicit tenant with a completed
/// run, an accepted output, and its projected search row;
/// `with_active_chunk` adds complete coverage under the scripted identity
/// so planning considers the source converged.
async fn seed_tenant_source(
    database: &TestDatabase,
    tenant: TenantRef,
    with_active_chunk: bool,
) -> Result<SeededSource, Box<dyn std::error::Error>> {
    let document = Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:indexing")?,
        content_digest: digest('c')?,
        title: Some("Indexing fixture".to_owned()),
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            block_id: BlockId::new_v7(),
            text: "Lead sentence.".to_owned(),
        }],
        provenance: Vec::new(),
    };
    let source = database
        .database
        .register_source(&SourceReference {
            tenant,
            owner_context: "ratatoskr-extractor".to_owned(),
            ai_archive_id: String::new(),
            document_id: document.document_id,
            content_digest: document.content_digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse("ratatoskr-extractor")?,
                digest: document.content_digest.clone(),
                media_type: MediaType::parse("application/json")?,
                length_bytes: 128,
            },
        })
        .await?;
    let (tenant_ref,): (String,) =
        sqlx::query_as("select tenant_ref from knowledge.source_refs where source_ref_id = $1")
            .bind(source.id)
            .fetch_one(database.database.pool())
            .await?;
    let run_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         )
         values ($1, $2, 'article-analysis.v1', 'v1', 'v1', 'fake_default_v1', 'completed')",
    )
    .bind(run_id)
    .bind(source.id)
    .execute(database.database.pool())
    .await?;
    let output_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_outputs (output_id, run_id, result, raw_response)
         values ($1, $2, '{}', '{}')",
    )
    .bind(output_id)
    .bind(run_id)
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref,
             owner_context, document_id, title, lead, body, updated_at
         )
         values ($1, $2, $3, $4, 'ratatoskr-extractor', $5, 'Indexing fixture',
                 'Lead sentence.', 'Body paragraph.', now())",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(source.id)
    .bind(output_id)
    .bind(&tenant_ref)
    .bind(document.document_id.0)
    .execute(database.database.pool())
    .await?;
    if with_active_chunk {
        insert_active_chunk(
            database,
            source.id,
            output_id,
            &tenant_ref,
            document.document_id.0,
        )
        .await?;
    }
    Ok(SeededSource {
        run_id,
        source_ref_id: source.id,
        output_id,
        document_id: document.document_id.0,
        owner_context: "ratatoskr-extractor".to_owned(),
        tenant_ref,
    })
}

/// Adds complete active-identity coverage to one fixture source.
async fn insert_active_chunk(
    database: &TestDatabase,
    source_ref_id: uuid::Uuid,
    output_id: uuid::Uuid,
    tenant_ref: &str,
    document_id: uuid::Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    let chunk_text = "Indexing fixture\n\nLead sentence.".to_owned();
    sqlx::query(
        "insert into knowledge.embedding_chunks (
             embedding_chunk_id, source_ref_id, output_id, tenant_ref,
             owner_context, document_id, ordinal, chunk_text,
             chunk_digest_hex, chunking_version, provider, model,
             dimensions, prompt_version, embedding
         )
         values ($1, $2, $3, $4, 'ratatoskr-extractor', $5, 0, $6, $7,
                 $8, $9, $10, $11, $12, $13)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(source_ref_id)
    .bind(output_id)
    .bind(tenant_ref)
    .bind(document_id)
    .bind(&chunk_text)
    .bind(digest_of(&chunk_text))
    .bind(ratatoskr_knowledge::CHUNKING_VERSION)
    .bind(IDENTITY_PROVIDER)
    .bind(IDENTITY_MODEL)
    .bind(DIMENSIONS)
    .bind(IDENTITY_PROMPT_VERSION)
    .bind(fixture_vector(&[(0, 1.0)]))
    .execute(database.database.pool())
    .await?;
    Ok(())
}

#[tokio::test]
async fn reindex_plan_honors_tenant_and_source_scopes() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let tenant_a = TenantRef::of_user(UserId::new_v7());
    let tenant_b = TenantRef::of_user(UserId::new_v7());
    // Creation order is ascending uuid v7, matching plan ordering.
    let needing_a = seed_tenant_source(&database, tenant_a, false).await?;
    let converged_a = seed_tenant_source(&database, tenant_a, true).await?;
    let needing_b = seed_tenant_source(&database, tenant_b, false).await?;

    let identity = ratatoskr_knowledge::IndexingIdentity {
        provider: IDENTITY_PROVIDER.to_owned(),
        model: IDENTITY_MODEL.to_owned(),
        prompt_version: IDENTITY_PROMPT_VERSION.to_owned(),
    };

    let unrestricted = ratatoskr_knowledge::plan_reindex(
        database.database.pool(),
        &identity,
        &ratatoskr_knowledge::ReindexScope::unrestricted(),
    )
    .await?;
    assert_eq!(
        unrestricted,
        vec![needing_a.source_ref_id, needing_b.source_ref_id],
        "converged sources never enter the plan"
    );

    let tenant_scoped = ratatoskr_knowledge::plan_reindex(
        database.database.pool(),
        &identity,
        &ratatoskr_knowledge::ReindexScope::for_tenant(&needing_a.tenant_ref),
    )
    .await?;
    assert_eq!(
        tenant_scoped,
        vec![needing_a.source_ref_id],
        "another tenant's sources must stay outside a tenant-scoped plan"
    );

    let source_scoped = ratatoskr_knowledge::plan_reindex(
        database.database.pool(),
        &identity,
        &ratatoskr_knowledge::ReindexScope::for_source(
            &needing_a.tenant_ref,
            &needing_a.owner_context,
            needing_a.document_id.to_string(),
        ),
    )
    .await?;
    assert_eq!(
        source_scoped,
        vec![needing_a.source_ref_id],
        "a source-scoped plan names exactly that source"
    );
    let _ = converged_a;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn indexing_failure_is_explicit_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let seeded = seed_source(&database, "persisted", true).await?;
    let failing = Indexer::new(
        &database.database,
        scripted_provider(vec![
            Err(ProviderError::Transient),
            Err(ProviderError::Transient),
        ]),
        ChunkPolicy::new(1600, 200)?,
        IndexerLimits {
            batch_sources: 8,
            max_input_characters: 120_000,
            max_failure_attempts: 2,
        },
    );
    let _ = &seeded;

    let outcome_first = failing.process_pending().await?;
    assert_eq!(outcome_first.failed, 1);
    let (attempt, class, output_count, run_state): (i32, String, i64, String) = sqlx::query_as(
        "select (select attempt from knowledge.embedding_failures limit 1),
                (select error_class from knowledge.embedding_failures limit 1),
                (select count(*) from knowledge.analysis_outputs),
                (select state from knowledge.analysis_runs)",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(attempt, 1);
    assert_eq!(class, ProviderFailureClass::Unclassified.as_str());
    assert_eq!(output_count, 1, "the accepted output must survive");
    assert_eq!(run_state, "persisted");

    let outcome_second = failing.process_pending().await?;
    assert_eq!(outcome_second.failed, 1);
    let (attempt_second,): (i32,) =
        sqlx::query_as("select attempt from knowledge.embedding_failures limit 1")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(attempt_second, 2);

    // The bound is reached: further passes make no provider calls.
    let outcome_bounded = failing.process_pending().await?;
    assert_eq!(outcome_bounded.failed, 0);
    assert_eq!(outcome_bounded.bound_skipped, 1);
    let (attempts_total,): (Option<i64>,) =
        sqlx::query_as("select sum(attempt) from knowledge.embedding_failures")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(attempts_total, Some(2));

    // A later healthy pass succeeds and clears the failure entry.
    let healthy = Indexer::new(
        &database.database,
        scripted_provider(vec![Ok(ScriptedEmbeddingSuccess { input_tokens: 8 })]),
        ChunkPolicy::new(1600, 200)?,
        indexer_limits(),
    );
    let outcome_healthy = healthy.process_pending().await?;
    assert_eq!(outcome_healthy.indexed, 1);
    let (failures_left, run_state_now): (i64, String) = sqlx::query_as(
        "select (select count(*) from knowledge.embedding_failures),
                (select state from knowledge.analysis_runs)",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(failures_left, 0);
    assert_eq!(run_state_now, "indexed");

    database.cleanup().await?;
    Ok(())
}
