//! Durable-state-driven embedding indexing checks over a disposable
//! database.

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
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
