//! Current Knowledge schema integration behavior.

use ratatoskr_knowledge::test_support::TestDatabase;
use sqlx::Row as _;

#[tokio::test]
async fn owned_schema_applies_twice_without_cross_schema_objects()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    database.database.apply_schema().await?;

    let rows = sqlx::query(
        "select table_name from information_schema.tables
         where table_schema = 'knowledge' order by table_name",
    )
    .fetch_all(database.database.pool())
    .await?;
    let tables = rows
        .into_iter()
        .map(|row| row.try_get::<String, _>("table_name"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        tables,
        [
            "ai_archive_object_inbox",
            "ai_archive_tombstones",
            "analysis_attempts",
            "analysis_feedback",
            "analysis_outputs",
            "analysis_runs",
            "analysis_taggings",
            "analysis_user_states",
            "collection_items",
            "collections",
            "deletion_records",
            "embedding_chunks",
            "embedding_failures",
            "highlights",
            "provider_usage",
            "repository_analysis_requests",
            "search_documents",
            "search_projection_inputs",
            "source_analysis_heads",
            "source_analysis_inbox",
            "source_refs",
            "tags"
        ]
    );

    let cross_schema_count: i64 = sqlx::query_scalar(
        "select count(*) from information_schema.tables
         where table_schema not in ('knowledge', 'information_schema', 'pg_catalog')",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(cross_schema_count, 0);

    let constraints: Vec<String> = sqlx::query_scalar(
        "select constraint_name from information_schema.table_constraints
         where table_schema = 'knowledge' order by constraint_name",
    )
    .fetch_all(database.database.pool())
    .await?;
    for expected in [
        "analysis_attempts_reason_check",
        "analysis_outputs_result_object_check",
        "analysis_runs_state_check",
        "source_refs_digest_algorithm_check",
        "source_refs_identity_key",
    ] {
        assert!(constraints.iter().any(|name| name == expected));
    }

    let accepted_index: Option<String> = sqlx::query_scalar(
        "select indexname from pg_indexes
         where schemaname = 'knowledge' and indexname = 'one_accepted_output_per_run'",
    )
    .fetch_optional(database.database.pool())
    .await?;
    assert_eq!(
        accepted_index.as_deref(),
        Some("one_accepted_output_per_run")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn schema_creates_deletion_audit_table() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    database.database.apply_schema().await?;

    let deletion_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.deletion_records (
             deletion_id, tenant_ref, scope, source_refs_deleted,
             analysis_runs_deleted, analysis_attempts_deleted,
             analysis_outputs_deleted, search_projection_inputs_deleted,
             search_documents_deleted,
             embedding_chunks_deleted, embedding_failures_deleted,
             blob_digests_removed
         ) values ($1, 'user:seeded-tenant', 'tenant', 0, 0, 0, 0, 0, 0, 0, 0, 0)",
    )
    .bind(deletion_id)
    .execute(database.database.pool())
    .await?;
    let (persisted_scope,): (String,) =
        sqlx::query_as("select scope from knowledge.deletion_records where deletion_id = $1")
            .bind(deletion_id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(persisted_scope, "tenant");

    let rejected = sqlx::query(
        "insert into knowledge.deletion_records (
             deletion_id, tenant_ref, scope, source_refs_deleted,
             analysis_runs_deleted, analysis_attempts_deleted,
             analysis_outputs_deleted, search_projection_inputs_deleted,
             search_documents_deleted,
             embedding_chunks_deleted, embedding_failures_deleted,
             blob_digests_removed
         ) values ($1, 'user:seeded-tenant', 'bogus', 0, 0, 0, 0, 0, 0, 0, 0, 0)",
    )
    .bind(uuid::Uuid::now_v7())
    .execute(database.database.pool())
    .await;
    let error = rejected.expect_err("scope 'bogus' must violate the check constraint");
    assert!(
        error.to_string().contains("deletion_records_scope_check"),
        "unexpected error: {error}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn provider_usage_records_window_totals() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    sqlx::query(
        "insert into knowledge.provider_usage (
            usage_id, provider, model, input_tokens, output_tokens,
            estimated_cost_micro_usd, recorded_at
         ) values ($1, 'openrouter', 'openai/gpt-oss-20b', 100, 40, 9,
                   now() - interval '1 day')",
    )
    .bind(uuid::Uuid::now_v7())
    .execute(database.database.pool())
    .await?;
    let ledger = ratatoskr_knowledge::BudgetLedger::new(database.database.pool().clone());
    ledger
        .record_usage(
            "openrouter",
            "openai/gpt-oss-20b",
            ratatoskr_knowledge::ProviderUsage {
                input_tokens: 30,
                output_tokens: 10,
            },
            5,
        )
        .await?;

    let (day_tokens, day_cost) = ledger
        .window_totals("openrouter", ratatoskr_knowledge::BudgetWindow::Daily)
        .await?;
    let (month_tokens, month_cost) = ledger
        .window_totals("openrouter", ratatoskr_knowledge::BudgetWindow::Monthly)
        .await?;
    assert_eq!((day_tokens, day_cost), (40, 5));
    assert_eq!((month_tokens, month_cost), (180, 14));
    let other = ledger
        .window_totals("other-provider", ratatoskr_knowledge::BudgetWindow::Monthly)
        .await?;
    assert_eq!(other, (0, 0));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn search_documents_projection() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;

    let columns: Vec<String> = sqlx::query_scalar(
        "select column_name from information_schema.columns
         where table_schema = 'knowledge' and table_name = 'search_documents'
         order by ordinal_position",
    )
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(
        columns,
        [
            "search_document_id",
            "source_ref_id",
            "latest_output_id",
            "tenant_ref",
            "owner_context",
            "document_id",
            "title",
            "lead",
            "body",
            "search_vector",
            "updated_at"
        ]
    );

    let unique_definitions: Vec<String> = sqlx::query_scalar(
        "select pg_get_constraintdef(oid) from pg_constraint
         where conrelid = 'knowledge.search_documents'::regclass and contype = 'u'
         order by conname",
    )
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(unique_definitions.as_slice(), ["UNIQUE (source_ref_id)"]);

    let vector_index: Option<String> = sqlx::query_scalar(
        "select indexdef from pg_indexes
         where schemaname = 'knowledge' and tablename = 'search_documents'
           and indexname = 'search_documents_search_vector_idx'",
    )
    .fetch_optional(database.database.pool())
    .await?;
    let vector_index = vector_index.unwrap_or_default();
    assert!(
        vector_index.contains("USING gin"),
        "indexdef was: {vector_index}"
    );
    assert!(
        vector_index.contains("search_vector"),
        "indexdef was: {vector_index}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pgvector_embedding_schema_objects() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    database.database.apply_schema().await?;

    let extension: Option<String> =
        sqlx::query_scalar("select extname from pg_extension where extname = 'vector'")
            .fetch_optional(database.database.pool())
            .await?;
    assert_eq!(extension.as_deref(), Some("vector"));

    for table in ["embedding_chunks", "embedding_failures"] {
        let present: Option<String> =
            sqlx::query_scalar("select to_regclass('knowledge.' || $1)::text")
                .bind(table)
                .fetch_one(database.database.pool())
                .await?;
        let expected = table;
        assert_eq!(
            present.as_deref(),
            Some(expected),
            "table {table} must exist"
        );
    }

    let embedding_type: Option<String> = sqlx::query_scalar(
        "select format_type(a.atttypid, a.atttypmod)
         from pg_attribute a
         where a.attrelid = 'knowledge.embedding_chunks'::regclass
           and a.attname = 'embedding'
           and not a.attisdropped",
    )
    .fetch_optional(database.database.pool())
    .await?;
    assert_eq!(embedding_type.as_deref(), Some("vector(1536)"));

    let hnsw_index: Option<String> = sqlx::query_scalar(
        "select indexdef from pg_indexes
         where schemaname = 'knowledge' and tablename = 'embedding_chunks'
           and indexname = 'embedding_chunks_embedding_hnsw_idx'",
    )
    .fetch_optional(database.database.pool())
    .await?;
    let hnsw_index = hnsw_index.unwrap_or_default();
    assert!(
        hnsw_index.contains("USING hnsw") && hnsw_index.contains("vector_cosine_ops"),
        "indexdef was: {hnsw_index}"
    );

    let constraints: Vec<String> = sqlx::query_scalar(
        "select constraint_name from information_schema.table_constraints
         where table_schema = 'knowledge' order by constraint_name",
    )
    .fetch_all(database.database.pool())
    .await?;
    for expected in [
        "embedding_chunks_dimensions_check",
        "embedding_chunks_digest_check",
        "embedding_chunks_identity_key",
        "embedding_failures_attempt_check",
        "embedding_failures_class_check",
        "embedding_failures_identity_key",
    ] {
        assert!(
            constraints.iter().any(|name| name == expected),
            "missing constraint {expected}"
        );
    }

    database.cleanup().await?;
    Ok(())
}
