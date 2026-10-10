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
            "blob_erasure_queue",
            "channel_recap_attempts",
            "channel_recap_inbox",
            "channel_recap_manifests",
            "channel_recap_outbox",
            "channel_recap_results",
            "channel_recap_runs",
            "collection_items",
            "collections",
            "deletion_records",
            "embedding_chunks",
            "embedding_failures",
            "event_outbox",
            "highlights",
            "ingest_failures",
            "provider_usage",
            "repository_analysis_requests",
            "search_documents",
            "search_projection_inputs",
            "social_tombstones",
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

fn database_error_code(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(std::borrow::Cow::into_owned)
}

async fn insert_outbox_row(
    pool: &sqlx::PgPool,
    subject: &str,
    dedupe_key: &str,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        "insert into knowledge.event_outbox
             (outbox_id, subject, tenant_ref, aggregate_id, correlation_id, causation_id,
              payload, dedupe_key)
         values ($1, $2, 'user:018f0000-0000-7000-8000-000000000005',
                 'social_source:018f0000-0000-7000-8000-000000000201',
                 'event:018f0000-0000-7000-8000-000000000901',
                 'event:018f0000-0000-7000-8000-000000000901', '{}'::jsonb, $3)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(subject)
    .bind(dedupe_key)
    .execute(pool)
    .await
}

/// XR-021 CONTRACTS.md S09 and S12: the outbox and the social tombstones exist in the one current
/// schema definition with their closed vocabularies.
#[tokio::test]
async fn outbox_and_social_tombstone_tables_exist_with_their_constraints()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let pool = database.database.pool();

    insert_outbox_row(pool, "knowledge.analysis.completed.v1", "social:t:s:aa").await?;
    for subject in [
        "knowledge.ai_archive_analysis.completed.v1",
        "knowledge.repository_analysis.completed.v1",
        "knowledge.repository_analysis.failed.v1",
    ] {
        insert_outbox_row(pool, subject, &format!("accepted:{subject}")).await?;
    }
    let bad_subject = insert_outbox_row(pool, "knowledge.unrelated.completed.v1", "bad-subject")
        .await
        .expect_err("a subject outside the four completion facts is refused");
    assert_eq!(database_error_code(&bad_subject).as_deref(), Some("23514"));
    let duplicate = insert_outbox_row(pool, "knowledge.analysis.completed.v1", "social:t:s:aa")
        .await
        .expect_err("a repeated dedupe key is refused");
    assert_eq!(database_error_code(&duplicate).as_deref(), Some("23505"));
    let unpublished_index: Option<String> = sqlx::query_scalar(
        "select indexdef from pg_indexes
         where schemaname = 'knowledge' and tablename = 'event_outbox'
           and indexdef ilike '%published_at is null%'",
    )
    .fetch_optional(pool)
    .await?;
    assert!(
        unpublished_index.is_some(),
        "the unpublished outbox rows need a partial index"
    );

    let tombstone = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.social_tombstones
             (event_id, tenant_ref, source_id, reason, removed_at)
         values ($1, 'user:018f0000-0000-7000-8000-000000000005',
                 '018f0000-0000-7000-8000-000000000201', 'user_deleted', now())",
    )
    .bind(tombstone)
    .execute(pool)
    .await?;
    let repeated_tombstone = sqlx::query(
        "insert into knowledge.social_tombstones
             (event_id, tenant_ref, source_id, reason, removed_at)
         values ($1, 'user:018f0000-0000-7000-8000-000000000005',
                 '018f0000-0000-7000-8000-000000000201', 'user_deleted', now())",
    )
    .bind(tombstone)
    .execute(pool)
    .await
    .expect_err("a tombstone event is claimed once");
    assert_eq!(
        database_error_code(&repeated_tombstone).as_deref(),
        Some("23505")
    );

    database.cleanup().await?;
    Ok(())
}

/// XR-021 CONTRACTS.md S09 and S12: the ingest failure record and the blob erasure queue exist
/// with their closed vocabularies.
#[tokio::test]
async fn ingest_failure_and_erasure_queue_tables_exist_with_their_constraints()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let pool = database.database.pool();

    for family in ["documents", "social", "ai_archive", "repository"] {
        sqlx::query(
            "insert into knowledge.ingest_failures
                 (family, event_id, failure_class, delivered_count)
             values ($1, $2, 'transient_exhausted', 5)",
        )
        .bind(family)
        .bind(uuid::Uuid::now_v7())
        .execute(pool)
        .await?;
    }
    let bad_family = sqlx::query(
        "insert into knowledge.ingest_failures
             (family, event_id, failure_class, delivered_count)
         values ('articles', $1, 'transient_exhausted', 5)",
    )
    .bind(uuid::Uuid::now_v7())
    .execute(pool)
    .await
    .expect_err("an unknown ingest family is refused");
    assert_eq!(database_error_code(&bad_family).as_deref(), Some("23514"));

    sqlx::query(
        "insert into knowledge.blob_erasure_queue (digest_hex, deletion_id) values ($1, $2)",
    )
    .bind("a".repeat(64))
    .bind(uuid::Uuid::now_v7())
    .execute(pool)
    .await?;
    let bad_digest = sqlx::query(
        "insert into knowledge.blob_erasure_queue (digest_hex, deletion_id) values ($1, $2)",
    )
    .bind("not-a-digest")
    .bind(uuid::Uuid::now_v7())
    .execute(pool)
    .await
    .expect_err("a malformed digest is refused");
    assert_eq!(database_error_code(&bad_digest).as_deref(), Some("23514"));

    database.cleanup().await?;
    Ok(())
}

/// XR-021 CONTRACTS.md S08 and S12: a recap attempt may end `interrupted`, and the deletion
/// record carries a count column per new content-bearing table.
#[tokio::test]
async fn interrupted_recap_attempts_and_deletion_counts_exist()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let pool = database.database.pool();

    let command_id = uuid::Uuid::now_v7();
    let recap_run_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.channel_recap_inbox
             (command_id, owner_ref, operation_id, digest_run_id, manifest_ref,
              manifest_digest_hex, window_start_at, window_end_at, source_count, channel_count,
              analysis_family, analysis_contract, output_language, request_payload)
         values ($1, 'user:018f0000-0000-7000-8000-000000000005', $2, $3,
                 'channel-digest-manifest:018f0000-0000-7000-8000-000000000204', $4,
                 '2026-08-20T10:00:00Z', '2026-08-21T10:00:00Z', 1, 1,
                 'channel_digest_recap', 'channel_digest_recap.v1', 'en', '{}'::jsonb)",
    )
    .bind(command_id)
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::now_v7())
    .bind("0".repeat(64))
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into knowledge.channel_recap_runs
             (recap_run_id, inbox_command_id, owner_ref, digest_run_id, manifest_digest_hex,
              analysis_family, analysis_contract, prompt_version, context_version,
              output_language, state)
         select $1, command_id, owner_ref, digest_run_id, manifest_digest_hex, analysis_family,
                analysis_contract, 'channel_digest_recap_prompt.v1',
                'channel_digest_recap_context.v1', output_language, 'model_requested'
         from knowledge.channel_recap_inbox where command_id = $2",
    )
    .bind(recap_run_id)
    .bind(command_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into knowledge.channel_recap_attempts
             (recap_run_id, ordinal, reason, provider, model, outcome, duration_ms)
         values ($1, 1, 'initial', 'scripted', 'scripted-model', 'interrupted', 0)",
    )
    .bind(recap_run_id)
    .execute(pool)
    .await?;

    let count_columns: Vec<String> = sqlx::query_scalar(
        "select column_name from information_schema.columns
         where table_schema = 'knowledge' and table_name = 'deletion_records'
           and column_default = '0' order by column_name",
    )
    .fetch_all(pool)
    .await?;
    for expected in [
        "ai_archive_object_inbox_deleted",
        "ai_archive_tombstones_deleted",
        "event_outbox_deleted",
        "repository_analysis_requests_deleted",
        "social_tombstones_deleted",
        "source_analysis_heads_deleted",
        "source_analysis_inbox_deleted",
    ] {
        assert!(
            count_columns.iter().any(|name| name == expected),
            "missing deletion_records count column {expected}"
        );
    }

    database.cleanup().await?;
    Ok(())
}
