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
            "analysis_attempts",
            "analysis_outputs",
            "analysis_runs",
            "provider_usage",
            "search_documents",
            "source_refs"
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
