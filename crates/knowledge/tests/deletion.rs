//! Privacy deletion behavior over disposable databases and blob roots.

use std::path::{Path, PathBuf};

use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{BlobStore, DeletionCounts, DeletionScope, SourceReference};
use uuid::Uuid;

const OWNER_CONTEXT: &str = "ratatoskr-extractor";

#[tokio::test]
async fn remove_deletes_owned_bytes_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let root = TemporaryBlobRoot::create().await?;
    let store = BlobStore::new(root.path(), 4_096);
    let reference = store.store_raw(br#"{"attempt":"one"}"#).await?;
    let hex = reference.digest.hex.as_str();
    let path = blob_path(root.path(), hex);
    assert!(
        tokio::fs::try_exists(&path).await?,
        "fixture file must exist"
    );

    assert!(
        store.remove(hex).await?,
        "first removal must delete the content-addressed file"
    );
    assert!(!tokio::fs::try_exists(&path).await?, "file must be gone");

    assert!(
        !store.remove(hex).await?,
        "second removal must report absence without error"
    );
    Ok(())
}

struct SeededSource {
    tenant_ref: String,
    owner_context: String,
    source_document_id: String,
    source_ref_id: Uuid,
    run_id: Uuid,
    attempt_digest: String,
    output_digest: String,
}

async fn seed_analyzed_source(
    database: &TestDatabase,
    blobs: &BlobStore,
    marker: &str,
    run_state: &str,
) -> Result<SeededSource, Box<dyn std::error::Error>> {
    let document_id = DocumentId::new_v7();
    let content_hex = digest_hex_of(marker);
    let source = register_fixture_source(database, document_id, &content_hex).await?;
    let (tenant_ref,): (String,) =
        sqlx::query_as("select tenant_ref from knowledge.source_refs where source_ref_id = $1")
            .bind(source.id)
            .fetch_one(database.database.pool())
            .await?;

    let run_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         ) values ($1, $2, 'article-analysis.v1', 'v1', 'v1', 'fake_default_v1', $3)",
    )
    .bind(run_id)
    .bind(source.id)
    .bind(run_state)
    .execute(database.database.pool())
    .await?;

    let attempt_digest = seed_invalid_attempt(database, blobs, run_id, marker).await?;
    let output_id = seed_accepted_output(database, blobs, run_id, marker).await?;
    seed_search_document(database, source.id, output_id, &tenant_ref, document_id.0).await?;
    for identity in [
        ("scripted_fake", "fake_default_v1"),
        ("legacy_embedder", "legacy_model_v0"),
    ] {
        insert_chunk(
            database,
            source.id,
            output_id,
            &tenant_ref,
            document_id.0,
            identity.0,
            identity.1,
        )
        .await?;
    }
    seed_embedding_failure(database, source.id, output_id, &tenant_ref).await?;

    Ok(SeededSource {
        tenant_ref,
        owner_context: OWNER_CONTEXT.to_owned(),
        source_document_id: document_id.to_string(),
        source_ref_id: source.id,
        run_id,
        attempt_digest,
        output_digest: digest_hex_of(&format!("accepted-output-{marker}")),
    })
}

async fn register_fixture_source(
    database: &TestDatabase,
    document_id: DocumentId,
    content_hex: &str,
) -> Result<ratatoskr_knowledge::SourceRevision, Box<dyn std::error::Error>> {
    let source = database
        .database
        .register_source(&SourceReference {
            tenant: TenantRef::of_user(UserId::new_v7()),
            owner_context: OWNER_CONTEXT.to_owned(),
            document_id,
            content_digest: content_digest(content_hex)?,
            source_blob: BlobRef {
                owner_service: BlobOwner::parse(OWNER_CONTEXT)?,
                digest: content_digest(content_hex)?,
                media_type: MediaType::parse("application/json")?,
                length_bytes: 128,
            },
        })
        .await?;
    Ok(source)
}

async fn seed_invalid_attempt(
    database: &TestDatabase,
    blobs: &BlobStore,
    run_id: Uuid,
    marker: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let attempt_bytes = format!("invalid-response-{marker}");
    let attempt_reference = blobs.store_raw(attempt_bytes.as_bytes()).await?;
    sqlx::query(
        "insert into knowledge.analysis_attempts (
             run_id, ordinal, reason, provider, model_policy, model,
             raw_response, outcome, validation_code
         ) values ($1, 1, 'initial', 'openrouter', 'fake_default_v1',
                   'openai/gpt-oss-20b', $2, 'invalid', 'schema_mismatch')",
    )
    .bind(run_id)
    .bind(serde_json::to_value(&attempt_reference)?)
    .execute(database.database.pool())
    .await?;
    Ok(attempt_reference.digest.hex.as_str().to_owned())
}

async fn seed_accepted_output(
    database: &TestDatabase,
    blobs: &BlobStore,
    run_id: Uuid,
    marker: &str,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    let output_bytes = format!("accepted-output-{marker}");
    let output_reference = blobs.store_raw(output_bytes.as_bytes()).await?;
    let output_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_outputs (output_id, run_id, result, raw_response)
         values ($1, $2, '{}', $3)",
    )
    .bind(output_id)
    .bind(run_id)
    .bind(serde_json::to_value(&output_reference)?)
    .execute(database.database.pool())
    .await?;
    Ok(output_id)
}

async fn seed_search_document(
    database: &TestDatabase,
    source_ref_id: Uuid,
    output_id: Uuid,
    tenant_ref: &str,
    document_id: Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "insert into knowledge.search_projection_inputs (
             source_ref_id, latest_output_id, tenant_ref, owner_context,
             document_id, title, lead, body, updated_at
         ) values ($1, $2, $3, $4, $5, 'Seeded title', 'Seeded lead.',
                   'Seeded body.', now())",
    )
    .bind(source_ref_id)
    .bind(output_id)
    .bind(tenant_ref)
    .bind(OWNER_CONTEXT)
    .bind(document_id)
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref,
             owner_context, document_id, title, lead, body, updated_at
         ) values ($1, $2, $3, $4, $5, $6, 'Seeded title', 'Seeded lead.',
                   'Seeded body.', now())",
    )
    .bind(Uuid::now_v7())
    .bind(source_ref_id)
    .bind(output_id)
    .bind(tenant_ref)
    .bind(OWNER_CONTEXT)
    .bind(document_id)
    .execute(database.database.pool())
    .await?;
    Ok(())
}

async fn seed_embedding_failure(
    database: &TestDatabase,
    source_ref_id: Uuid,
    output_id: Uuid,
    tenant_ref: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "insert into knowledge.embedding_failures (
             failure_id, source_ref_id, output_id, tenant_ref,
             chunking_version, provider, model, prompt_version,
             error_class, attempt
         ) values ($1, $2, $3, $4, $5, 'legacy_embedder', 'legacy_model_v0',
                   'none.v1', 'rate_limited', 1)",
    )
    .bind(Uuid::now_v7())
    .bind(source_ref_id)
    .bind(output_id)
    .bind(tenant_ref)
    .bind(ratatoskr_knowledge::CHUNKING_VERSION)
    .execute(database.database.pool())
    .await?;
    Ok(())
}

async fn insert_chunk(
    database: &TestDatabase,
    source_ref_id: Uuid,
    output_id: Uuid,
    tenant_ref: &str,
    document_id: Uuid,
    provider: &str,
    model: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let chunk_text = format!("Chunk text for {provider}.");
    sqlx::query(
        "insert into knowledge.embedding_chunks (
             embedding_chunk_id, source_ref_id, output_id, tenant_ref,
             owner_context, document_id, ordinal, chunk_text,
             chunk_digest_hex, chunking_version, provider, model,
             dimensions, prompt_version, embedding
         ) values ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9, $10, $11, 1536,
                   'none.v1', $12)",
    )
    .bind(Uuid::now_v7())
    .bind(source_ref_id)
    .bind(output_id)
    .bind(tenant_ref)
    .bind(OWNER_CONTEXT)
    .bind(document_id)
    .bind(&chunk_text)
    .bind(digest_hex_of(&chunk_text))
    .bind(ratatoskr_knowledge::CHUNKING_VERSION)
    .bind(provider)
    .bind(model)
    .bind(fixture_vector(provider.len()))
    .execute(database.database.pool())
    .await?;
    Ok(())
}

fn fixture_vector(direction: usize) -> pgvector::Vector {
    const DIMENSIONS: usize = 1536;
    let mut values = vec![0.0_f32; DIMENSIONS];
    if let Some(slot) = values.get_mut(direction % DIMENSIONS) {
        *slot = 1.0;
    }
    pgvector::Vector::from(values)
}

async fn count_by_source_ids(
    database: &TestDatabase,
    table: &str,
    ids: &[Uuid],
) -> Result<i64, Box<dyn std::error::Error>> {
    let sql = format!("select count(*) from knowledge.{table} where source_ref_id = any($1)");
    let (count,): (i64,) = sqlx::query_as(&sql)
        .bind(ids.to_vec())
        .fetch_one(database.database.pool())
        .await?;
    Ok(count)
}

async fn count_by_run_ids(
    database: &TestDatabase,
    table: &str,
    ids: &[Uuid],
) -> Result<i64, Box<dyn std::error::Error>> {
    let sql = format!("select count(*) from knowledge.{table} where run_id = any($1)");
    let (count,): (i64,) = sqlx::query_as(&sql)
        .bind(ids.to_vec())
        .fetch_one(database.database.pool())
        .await?;
    Ok(count)
}

fn digest_hex_of(value: &str) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn content_digest(hex: &str) -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(hex)?,
    })
}

fn blob_path(root: &Path, hex: &str) -> PathBuf {
    let prefix = hex.get(..2).unwrap_or(hex);
    root.join("sha256").join(prefix).join(hex)
}

async fn assert_source_fully_deleted(
    database: &TestDatabase,
    root: &Path,
    seeded: &SeededSource,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_ids = [seeded.source_ref_id];
    let run_ids = [seeded.run_id];
    assert_eq!(
        count_by_source_ids(database, "source_refs", &source_ids).await?,
        0,
        "source_refs rows survived"
    );
    assert_eq!(
        count_by_source_ids(database, "analysis_runs", &source_ids).await?,
        0,
        "analysis_runs rows survived"
    );
    assert_eq!(
        count_by_run_ids(database, "analysis_attempts", &run_ids).await?,
        0,
        "analysis_attempts rows survived"
    );
    assert_eq!(
        count_by_run_ids(database, "analysis_outputs", &run_ids).await?,
        0,
        "analysis_outputs rows survived"
    );
    assert_eq!(
        count_by_source_ids(database, "search_documents", &source_ids).await?,
        0,
        "search_documents rows survived"
    );
    assert_eq!(
        count_by_source_ids(database, "search_projection_inputs", &source_ids).await?,
        0,
        "search_projection_inputs rows survived"
    );
    assert_eq!(
        count_by_source_ids(database, "embedding_chunks", &source_ids).await?,
        0,
        "embedding_chunks rows survived"
    );
    assert_eq!(
        count_by_source_ids(database, "embedding_failures", &source_ids).await?,
        0,
        "embedding_failures rows survived"
    );
    assert!(
        !tokio::fs::try_exists(blob_path(root, &seeded.attempt_digest)).await?,
        "attempt-referenced blob survived"
    );
    assert!(
        !tokio::fs::try_exists(blob_path(root, &seeded.output_digest)).await?,
        "output-referenced blob survived"
    );
    Ok(())
}

async fn assert_source_fully_intact(
    database: &TestDatabase,
    root: &Path,
    seeded: &SeededSource,
    expected_outputs: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_ids = [seeded.source_ref_id];
    let run_ids = [seeded.run_id];
    assert_eq!(
        count_by_source_ids(database, "source_refs", &source_ids).await?,
        1
    );
    assert_eq!(
        count_by_source_ids(database, "analysis_runs", &source_ids).await?,
        1
    );
    assert_eq!(
        count_by_run_ids(database, "analysis_attempts", &run_ids).await?,
        1
    );
    assert_eq!(
        count_by_run_ids(database, "analysis_outputs", &run_ids).await?,
        expected_outputs
    );
    assert_eq!(
        count_by_source_ids(database, "search_documents", &source_ids).await?,
        1
    );
    assert_eq!(
        count_by_source_ids(database, "search_projection_inputs", &source_ids).await?,
        1
    );
    assert_eq!(
        count_by_source_ids(database, "embedding_chunks", &source_ids).await?,
        2
    );
    assert_eq!(
        count_by_source_ids(database, "embedding_failures", &source_ids).await?,
        1
    );
    assert!(
        tokio::fs::try_exists(blob_path(root, &seeded.attempt_digest)).await?,
        "attempt-referenced blob must remain"
    );
    assert!(
        tokio::fs::try_exists(blob_path(root, &seeded.output_digest)).await?,
        "output-referenced blob must remain"
    );
    Ok(())
}

async fn seed_extra_output(
    database: &TestDatabase,
    run_id: Uuid,
    reference: &ratatoskr_identifiers::BlobRef,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "insert into knowledge.analysis_outputs (
             output_id, run_id, result, raw_response, accepted
         ) values ($1, $2, '{}', $3, false)",
    )
    .bind(Uuid::now_v7())
    .bind(run_id)
    .bind(serde_json::to_value(reference)?)
    .execute(database.database.pool())
    .await?;
    Ok(())
}

async fn provider_usage_rows(
    database: &TestDatabase,
) -> Result<Vec<(Uuid, String, i64, i64, i64)>, Box<dyn std::error::Error>> {
    Ok(sqlx::query_as(
        "select usage_id, provider, input_tokens, output_tokens,
                estimated_cost_micro_usd
         from knowledge.provider_usage order by usage_id",
    )
    .fetch_all(database.database.pool())
    .await?)
}

#[derive(Debug, PartialEq, Eq)]
struct AuditRow {
    scope: String,
    source_refs_deleted: i32,
    analysis_runs_deleted: i32,
    analysis_attempts_deleted: i32,
    analysis_outputs_deleted: i32,
    search_projection_inputs_deleted: i32,
    search_documents_deleted: i32,
    embedding_chunks_deleted: i32,
    embedding_failures_deleted: i32,
    blob_digests_removed: i32,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for AuditRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row as _;
        Ok(Self {
            scope: row.try_get("scope")?,
            source_refs_deleted: row.try_get("source_refs_deleted")?,
            analysis_runs_deleted: row.try_get("analysis_runs_deleted")?,
            analysis_attempts_deleted: row.try_get("analysis_attempts_deleted")?,
            analysis_outputs_deleted: row.try_get("analysis_outputs_deleted")?,
            search_projection_inputs_deleted: row.try_get("search_projection_inputs_deleted")?,
            search_documents_deleted: row.try_get("search_documents_deleted")?,
            embedding_chunks_deleted: row.try_get("embedding_chunks_deleted")?,
            embedding_failures_deleted: row.try_get("embedding_failures_deleted")?,
            blob_digests_removed: row.try_get("blob_digests_removed")?,
        })
    }
}

async fn assert_audit_rows(
    database: &TestDatabase,
    tenant_ref: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let audits: Vec<AuditRow> = sqlx::query_as(
        "select scope, source_refs_deleted, analysis_runs_deleted,
                analysis_attempts_deleted, analysis_outputs_deleted,
                search_projection_inputs_deleted, search_documents_deleted,
                embedding_chunks_deleted,
                embedding_failures_deleted, blob_digests_removed
         from knowledge.deletion_records where tenant_ref = $1
         order by deletion_id",
    )
    .bind(tenant_ref)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(audits.len(), 2, "exactly one audit row per deletion");
    let expected_first = AuditRow {
        scope: "tenant".to_owned(),
        source_refs_deleted: 1,
        analysis_runs_deleted: 1,
        analysis_attempts_deleted: 1,
        analysis_outputs_deleted: 2,
        search_projection_inputs_deleted: 1,
        search_documents_deleted: 1,
        embedding_chunks_deleted: 2,
        embedding_failures_deleted: 1,
        blob_digests_removed: 2,
    };
    let expected_rerun = AuditRow {
        scope: "tenant".to_owned(),
        source_refs_deleted: 0,
        analysis_runs_deleted: 0,
        analysis_attempts_deleted: 0,
        analysis_outputs_deleted: 0,
        search_projection_inputs_deleted: 0,
        search_documents_deleted: 0,
        embedding_chunks_deleted: 0,
        embedding_failures_deleted: 0,
        blob_digests_removed: 0,
    };
    assert_eq!(
        audits.first(),
        Some(&expected_first),
        "first audit row must equal the receipt facts"
    );
    assert_eq!(
        audits.get(1),
        Some(&expected_rerun),
        "rerun audit row must carry zero counts"
    );
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct RowTotals {
    source_refs: i64,
    analysis_runs: i64,
    analysis_attempts: i64,
    analysis_outputs: i64,
    search_projection_inputs: i64,
    search_documents: i64,
    embedding_chunks: i64,
    embedding_failures: i64,
    audit_rows: i64,
}

async fn scoped_row_totals(
    database: &TestDatabase,
    seeded: &SeededSource,
) -> Result<RowTotals, Box<dyn std::error::Error>> {
    let totals: (i64, i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "select
             (select count(*) from knowledge.source_refs
              where source_ref_id = $1),
             (select count(*) from knowledge.analysis_runs
              where source_ref_id = $1),
             (select count(*) from knowledge.analysis_attempts
              where run_id = $2),
             (select count(*) from knowledge.analysis_outputs
              where run_id = $2),
             (select count(*) from knowledge.search_projection_inputs
              where source_ref_id = $1),
             (select count(*) from knowledge.search_documents
              where source_ref_id = $1),
             (select count(*) from knowledge.embedding_chunks
              where source_ref_id = $1),
             (select count(*) from knowledge.embedding_failures
              where source_ref_id = $1),
             (select count(*) from knowledge.deletion_records)",
    )
    .bind(seeded.source_ref_id)
    .bind(seeded.run_id)
    .fetch_one(database.database.pool())
    .await?;
    Ok(RowTotals {
        source_refs: totals.0,
        analysis_runs: totals.1,
        analysis_attempts: totals.2,
        analysis_outputs: totals.3,
        search_projection_inputs: totals.4,
        search_documents: totals.5,
        embedding_chunks: totals.6,
        embedding_failures: totals.7,
        audit_rows: totals.8,
    })
}

#[tokio::test]
async fn row_deletion_is_atomic_with_its_audit_record() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let deleted = seed_analyzed_source(&database, &blobs, "atomic-source", "completed").await?;

    let scope = DeletionScope::Source {
        tenant_ref: deleted.tenant_ref.clone(),
        owner_context: deleted.owner_context.clone(),
        source_document_id: deleted.source_document_id.clone(),
    };
    let before = scoped_row_totals(&database, &deleted).await?;
    assert_eq!(before.audit_rows, 0, "no audit row may pre-exist");

    let mut transaction = database.database.pool().begin().await?;
    let counts = ratatoskr_knowledge::execute_deletion(&mut transaction, &scope).await?;

    let during = scoped_row_totals(&database, &deleted).await?;
    assert_eq!(during, before, "uncommitted deletion must be invisible");

    transaction.commit().await?;

    let after = scoped_row_totals(&database, &deleted).await?;
    assert_eq!(after.source_refs, 0, "source_refs rows survived");
    assert_eq!(after.analysis_runs, 0, "analysis_runs rows survived");
    assert_eq!(
        after.analysis_attempts, 0,
        "analysis_attempts rows survived"
    );
    assert_eq!(after.analysis_outputs, 0, "analysis_outputs rows survived");
    assert_eq!(
        after.search_projection_inputs, 0,
        "search_projection_inputs rows survived"
    );
    assert_eq!(after.search_documents, 0, "search_documents rows survived");
    assert_eq!(after.embedding_chunks, 0, "embedding_chunks rows survived");
    assert_eq!(
        after.embedding_failures, 0,
        "embedding_failures rows survived"
    );
    assert_eq!(after.audit_rows, 1, "exactly one audit row must exist");
    assert_eq!(
        counts,
        DeletionCounts {
            source_refs: 1,
            analysis_runs: 1,
            analysis_attempts: 1,
            analysis_outputs: 1,
            search_projection_inputs: 1,
            search_documents: 1,
            embedding_chunks: 2,
            embedding_failures: 1,
        }
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn orphan_sweep_reclaims_unreferenced_blobs_on_any_deletion()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);

    let orphan_hex = digest_hex_of("orphan-leftover-bytes");
    let orphan_path = blob_path(root.path(), &orphan_hex);
    tokio::fs::create_dir_all(orphan_path.parent().expect("orphan parent")).await?;
    tokio::fs::write(&orphan_path, b"orphan bytes").await?;
    assert!(tokio::fs::try_exists(&orphan_path).await?);

    let receipt =
        ratatoskr_knowledge::delete_tenant(&database.database, &blobs, "user:no-such-tenant")
            .await?;

    assert!(
        !tokio::fs::try_exists(&orphan_path).await?,
        "the sweep phase must reclaim the unreferenced file"
    );
    assert_eq!(
        receipt.orphan_digests_removed,
        vec![orphan_hex],
        "orphans must be reported separately from scope removals"
    );
    assert_eq!(receipt.counts, DeletionCounts::default());
    assert!(receipt.blob_digests_removed.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn deleting_a_tenant_leaves_the_survivor_and_ledger_intact()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);

    let deleted = seed_analyzed_source(&database, &blobs, "tenant-deleted", "completed").await?;
    let survivor = seed_analyzed_source(&database, &blobs, "tenant-survivor", "indexed").await?;

    // One byte-identical response shared by both tenants' rows.
    let shared_reference = blobs.store_raw(b"shared-response-bytes").await?;
    let shared_hex = shared_reference.digest.hex.as_str().to_owned();
    seed_extra_output(&database, deleted.run_id, &shared_reference).await?;
    seed_extra_output(&database, survivor.run_id, &shared_reference).await?;

    for (usage_id, provider, tokens) in [
        (Uuid::now_v7(), "openrouter", 100_i64),
        (Uuid::now_v7(), "scripted_fake", 40),
    ] {
        sqlx::query(
            "insert into knowledge.provider_usage (
                 usage_id, provider, model, input_tokens, output_tokens,
                 estimated_cost_micro_usd
             ) values ($1, $2, 'shared_model', $3, 10, 7)",
        )
        .bind(usage_id)
        .bind(provider)
        .bind(tokens)
        .execute(database.database.pool())
        .await?;
    }
    let usage_before = provider_usage_rows(&database).await?;

    let receipt =
        ratatoskr_knowledge::delete_tenant(&database.database, &blobs, &deleted.tenant_ref).await?;

    assert_source_fully_deleted(&database, root.path(), &deleted).await?;
    assert_source_fully_intact(&database, root.path(), &survivor, 2).await?;
    assert!(
        tokio::fs::try_exists(blob_path(root.path(), &shared_hex)).await?,
        "the digest shared between tenants must survive"
    );

    // Externally owned provenance bytes are reported, never removed; they
    // never lived in the Knowledge-owned root in the first place.
    let external_digest = digest_hex_of("tenant-deleted");
    assert_eq!(
        receipt.external_source_blob_digests,
        vec![external_digest.clone()]
    );
    assert!(
        !tokio::fs::try_exists(blob_path(root.path(), &external_digest)).await?,
        "externally owned digests must gain no file handling"
    );

    // The aggregate spend ledger outlives content erasure.
    assert_eq!(provider_usage_rows(&database).await?, usage_before);

    assert_eq!(
        receipt.counts,
        DeletionCounts {
            source_refs: 1,
            analysis_runs: 1,
            analysis_attempts: 1,
            analysis_outputs: 2,
            search_projection_inputs: 1,
            search_documents: 1,
            embedding_chunks: 2,
            embedding_failures: 1,
        }
    );
    let mut removed = receipt.blob_digests_removed.clone();
    removed.sort();
    let mut expected_removed = vec![
        deleted.attempt_digest.clone(),
        deleted.output_digest.clone(),
    ];
    expected_removed.sort();
    assert_eq!(removed, expected_removed, "only private digests die");

    // Rerunning is a quiet no-op that still records its audit row.
    let receipt_again =
        ratatoskr_knowledge::delete_tenant(&database.database, &blobs, &deleted.tenant_ref).await?;
    assert_eq!(receipt_again.counts, DeletionCounts::default());
    assert!(receipt_again.blob_digests_removed.is_empty());
    assert_audit_rows(&database, &deleted.tenant_ref).await?;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn deleting_a_source_removes_every_derived_row_and_owned_blob()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);

    let deleted = seed_analyzed_source(&database, &blobs, "deleted-source", "completed").await?;
    let survivor = seed_analyzed_source(&database, &blobs, "survivor-source", "indexed").await?;

    let receipt = ratatoskr_knowledge::delete_source(
        &database.database,
        &blobs,
        &deleted.tenant_ref,
        &deleted.owner_context,
        &deleted.source_document_id,
    )
    .await?;

    // Enumerate every derived location for the deleted scope.
    assert_source_fully_deleted(&database, root.path(), &deleted).await?;
    assert_source_fully_intact(&database, root.path(), &survivor, 1).await?;

    // The receipt equals the independently counted facts.
    assert_eq!(
        receipt.scope,
        DeletionScope::Source {
            tenant_ref: deleted.tenant_ref.clone(),
            owner_context: deleted.owner_context.clone(),
            source_document_id: deleted.source_document_id.clone(),
        }
    );
    assert_eq!(
        receipt.counts,
        DeletionCounts {
            source_refs: 1,
            analysis_runs: 1,
            analysis_attempts: 1,
            analysis_outputs: 1,
            search_projection_inputs: 1,
            search_documents: 1,
            embedding_chunks: 2,
            embedding_failures: 1,
        }
    );
    let mut removed = receipt.blob_digests_removed.clone();
    removed.sort();
    let mut expected_removed = vec![
        deleted.attempt_digest.clone(),
        deleted.output_digest.clone(),
    ];
    expected_removed.sort();
    assert_eq!(removed, expected_removed, "removed digests must be exact");

    database.cleanup().await?;
    Ok(())
}
