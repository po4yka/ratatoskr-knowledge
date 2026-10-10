//! An article run resumes from its durable state (XR-021 CONTRACTS.md S02 rule 7 and S09).
//!
//! Each test builds the state a stopped, failed or concurrent executor leaves behind and shows
//! that the next execute finishes the same run, with the provider called only when the stored
//! state needs a call.

use ratatoskr_identifiers::BlobRef;
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    ArticlePipeline, BlobStore, PersistenceError, PipelineError, ProviderError, ProviderResponse,
    ProviderUsage, ScriptedProvider, build_generation_request,
};

mod support;

use support::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const VALID: &[u8] = br#"{"summary":"A grounded summary.","key_points":[{"text":"Evidence exists.","source_block_indexes":[0]}]}"#;
const INVALID: &[u8] = br#"{"summary":"","key_points":[]}"#;

fn pipeline<'a>(
    database: &'a TestDatabase,
    provider: &'a ScriptedProvider,
    blobs: &'a BlobStore,
) -> ArticlePipeline<'a, ScriptedProvider> {
    ArticlePipeline::new(
        &database.database,
        provider,
        blobs,
        std::time::Duration::from_secs(1),
    )
}

/// Stores `bytes` as the raw response of attempt 1 and rests the run at `state`.
async fn seed_stored_response(
    database: &TestDatabase,
    blobs: &BlobStore,
    run_id: uuid::Uuid,
    state: &str,
    outcome: &str,
    bytes: &[u8],
) -> TestResult {
    let reference: BlobRef = blobs.store_raw(bytes).await?;
    sqlx::query(
        "insert into knowledge.analysis_attempts (
            run_id, ordinal, reason, provider, model_policy, outcome, raw_response
         ) values ($1, 1, 'initial', 'scripted_fake', 'fake_default_v1', $2, $3)",
    )
    .bind(run_id)
    .bind(outcome)
    .bind(serde_json::to_value(reference)?)
    .execute(database.database.pool())
    .await?;
    set_state(database, run_id, state).await
}

async fn set_state(database: &TestDatabase, run_id: uuid::Uuid, state: &str) -> TestResult {
    sqlx::query("update knowledge.analysis_runs set state = $2 where run_id = $1")
        .bind(run_id)
        .bind(state)
        .execute(database.database.pool())
        .await?;
    Ok(())
}

async fn run_summary(
    database: &TestDatabase,
    run_id: uuid::Uuid,
) -> Result<(String, i64, i64), sqlx::Error> {
    sqlx::query_as(
        "select state,
                (select count(*) from knowledge.analysis_attempts where run_id = $1),
                (select count(*) from knowledge.analysis_outputs where run_id = $1)
         from knowledge.analysis_runs where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await
}

/// Two transient calls fail the run; the next execute reopens it and a third call completes it.
#[tokio::test]
async fn a_run_that_failed_transiently_is_reopened_by_the_next_execute() -> TestResult {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    let provider = ScriptedProvider::new([
        Err(ProviderError::Transient),
        Err(ProviderError::Transient),
        Ok(valid_response("third-call")),
    ]);
    let pipeline = pipeline(&database, &provider, &blobs);

    let first = pipeline
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await;
    assert!(matches!(
        first,
        Err(PipelineError::Provider(ProviderError::Transient))
    ));
    assert_eq!(run_summary(&database, run_id).await?.0, "failed");

    let second = pipeline
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await?;
    assert_eq!(second.summary, "A grounded summary.");
    assert_eq!(
        run_summary(&database, run_id).await?,
        ("persisted".to_owned(), 3, 1)
    );
    let reasons: Vec<String> = sqlx::query_scalar(
        "select reason from knowledge.analysis_attempts where run_id = $1 order by ordinal",
    )
    .bind(run_id)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(reasons, ["initial", "retry", "retry"]);
    database.cleanup().await?;
    Ok(())
}

/// A run whose calls keep failing transiently stops at the per-run budget instead of looping.
#[tokio::test]
async fn a_run_at_its_attempt_budget_is_not_reopened() -> TestResult {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    for ordinal in 1..=10_i16 {
        sqlx::query(
            "insert into knowledge.analysis_attempts (
                run_id, ordinal, reason, provider, model_policy, outcome
             ) values ($1, $2, 'retry', 'scripted_fake', 'fake_default_v1', 'transient_failure')",
        )
        .bind(run_id)
        .bind(ordinal)
        .execute(database.database.pool())
        .await?;
    }
    set_state(&database, run_id, "failed").await?;
    let provider = ScriptedProvider::new([Ok(valid_response("must-not-run"))]);

    let result = pipeline(&database, &provider, &blobs)
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await;
    assert!(matches!(
        result,
        Err(PipelineError::Persistence(
            PersistenceError::AttemptBudgetExhausted
        ))
    ));
    assert_eq!(provider.call_count()?, 0);
    assert_eq!(run_summary(&database, run_id).await?.0, "failed");
    database.cleanup().await?;
    Ok(())
}

/// A call that was in flight when the executor stopped is recorded as interrupted and the run
/// continues with the next call.
#[tokio::test]
async fn an_interrupted_call_is_recorded_and_the_run_continues() -> TestResult {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    sqlx::query(
        "insert into knowledge.analysis_attempts (
            run_id, ordinal, reason, provider, model_policy, outcome
         ) values ($1, 1, 'initial', 'scripted_fake', 'fake_default_v1', 'requested')",
    )
    .bind(run_id)
    .execute(database.database.pool())
    .await?;
    set_state(&database, run_id, "model_requested").await?;
    let provider = ScriptedProvider::new([Ok(valid_response("after-kill"))]);

    pipeline(&database, &provider, &blobs)
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await?;
    let outcomes: Vec<String> = sqlx::query_scalar(
        "select outcome from knowledge.analysis_attempts where run_id = $1 order by ordinal",
    )
    .bind(run_id)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(outcomes, ["interrupted", "accepted"]);
    assert_eq!(
        run_summary(&database, run_id).await?,
        ("persisted".to_owned(), 2, 1)
    );
    database.cleanup().await?;
    Ok(())
}

/// A response stored before the executor stopped is validated again; the provider is not called
/// for it, whether the run stopped before or after validation was recorded.
#[tokio::test]
async fn a_stored_valid_response_is_accepted_without_a_provider_call() -> TestResult {
    for (state, outcome) in [
        ("response_received", "response_received"),
        ("schema_validated", "accepted"),
    ] {
        let database = TestDatabase::create().await?;
        let root = TemporaryBlobRoot::create().await?;
        let blobs = BlobStore::new(root.path(), 4_096);
        let (run_id, context, document) = run_and_context(&database).await?;
        seed_stored_response(&database, &blobs, run_id, state, outcome, VALID).await?;
        let provider = ScriptedProvider::new([]);

        let article = pipeline(&database, &provider, &blobs)
            .execute(
                run_id,
                build_generation_request(&context)?,
                &context,
                &document,
            )
            .await?;
        assert_eq!(article.summary, "A grounded summary.", "from {state}");
        assert_eq!(provider.call_count()?, 0, "from {state}");
        assert_eq!(
            run_summary(&database, run_id).await?,
            ("persisted".to_owned(), 1, 1),
            "from {state}"
        );
        let attempt: String = sqlx::query_scalar(
            "select outcome from knowledge.analysis_attempts where run_id = $1 and ordinal = 1",
        )
        .bind(run_id)
        .fetch_one(database.database.pool())
        .await?;
        assert_eq!(attempt, "accepted", "from {state}");
        database.cleanup().await?;
    }
    Ok(())
}

/// A stored response that fails validation is repaired once, as an uninterrupted run would.
#[tokio::test]
async fn a_stored_invalid_response_is_repaired_once() -> TestResult {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    seed_stored_response(
        &database,
        &blobs,
        run_id,
        "response_received",
        "response_received",
        INVALID,
    )
    .await?;
    let provider = ScriptedProvider::new([Ok(ProviderResponse {
        bytes: VALID.to_vec(),
        request_id: Some("repair".to_owned()),
        usage: ProviderUsage {
            input_tokens: 20,
            output_tokens: 10,
        },
    })]);

    pipeline(&database, &provider, &blobs)
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await?;
    assert_eq!(provider.call_count()?, 1);
    assert!(
        provider.requests()?[0]
            .task_instruction
            .contains("Repair validation code: schema.")
    );
    let attempts: Vec<(String, String)> = sqlx::query_as(
        "select reason, outcome from knowledge.analysis_attempts where run_id = $1 order by ordinal",
    )
    .bind(run_id)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(
        attempts,
        [
            ("initial".to_owned(), "invalid".to_owned()),
            ("repair".to_owned(), "accepted".to_owned())
        ]
    );
    database.cleanup().await?;
    Ok(())
}

/// An executor that finds the run held by another one reports a conflict, which a later delivery
/// can overcome, and touches neither the provider nor the run.
#[tokio::test]
async fn a_run_held_by_another_executor_reports_a_conflict() -> TestResult {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    let mut holder = database.database.pool().acquire().await?.detach();
    let held: bool =
        sqlx::query_scalar("select pg_try_advisory_lock(hashtextextended($1::text, 0))")
            .bind(run_id.to_string())
            .fetch_one(&mut holder)
            .await?;
    assert!(held);
    let provider = ScriptedProvider::new([Ok(valid_response("must-not-run"))]);

    let result = pipeline(&database, &provider, &blobs)
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await;
    assert!(matches!(result, Err(PipelineError::Conflict)));
    assert_eq!(provider.call_count()?, 0);
    assert_eq!(run_summary(&database, run_id).await?.0, "queued");
    drop(holder);
    database.cleanup().await?;
    Ok(())
}
