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
