//! Durable fake-provider pipeline tests.

use std::sync::{Mutex, Once};

use ratatoskr_identifiers::BlobRef;
use ratatoskr_knowledge::test_support::{FakeTransport, TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    ArticlePipeline, BlobStore, GenerationRequest, LlmProvider, PipelineError, ProviderError,
    ProviderFailure, ProviderIdentity, ProviderResponse, ProviderUsage, ScriptedProvider,
    build_generation_request,
};

mod support;

use support::*;

static TELEMETRY: Mutex<Vec<u8>> = Mutex::new(Vec::new());
static TELEMETRY_INIT: Once = Once::new();

#[tokio::test(flavor = "current_thread")]
async fn malformed_response_is_stored_before_json_validation()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 1_024);
    let (run_id, context, document) = run_and_context(&database).await?;
    let provider = ScriptedProvider::new([Ok(ProviderResponse {
        bytes: b"{malformed LEAKME".to_vec(),
        request_id: Some("request-malformed".to_owned()),
        usage: ProviderUsage {
            input_tokens: 10,
            output_tokens: 2,
        },
    })]);
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );
    TELEMETRY_INIT.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(SharedWriter(&TELEMETRY))
            .finish();
        let _ignored = tracing::subscriber::set_global_default(subscriber);
    });
    TELEMETRY.lock().map_err(lock_error)?.clear();
    assert!(
        pipeline
            .execute(
                run_id,
                build_generation_request(&context)?,
                &context,
                &document
            )
            .await
            .is_err()
    );
    let row: (Option<serde_json::Value>, Option<String>) = sqlx::query_as(
        "select raw_response, validation_code from knowledge.analysis_attempts
         where run_id = $1 and ordinal = 1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    let reference: BlobRef = serde_json::from_value(row.0.ok_or("missing raw response")?)?;
    assert_eq!(reference.owner_service.as_str(), "ratatoskr-knowledge");
    assert_eq!(row.1.as_deref(), Some("json_syntax"));
    assert_eq!(blobs.read(&reference).await?, b"{malformed LEAKME");
    let captured = String::from_utf8(TELEMETRY.lock().map_err(lock_error)?.clone())?;
    assert!(captured.contains("json_syntax"));
    assert!(!captured.contains("LEAKME"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn one_transient_failure_retries_once() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    let provider = ScriptedProvider::new([
        Err(ProviderError::Transient),
        Ok(valid_response("request-retry")),
    ]);
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );

    let result = pipeline
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await?;
    assert_eq!(result.summary, "A grounded summary.");
    assert_eq!(provider.call_count()?, 2);
    let attempts: Vec<(i16, String)> = sqlx::query_as(
        "select ordinal, reason from knowledge.analysis_attempts
         where run_id = $1 order by ordinal",
    )
    .bind(run_id)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(
        attempts,
        [(1, "initial".to_owned()), (2, "retry".to_owned())]
    );
    let state: String =
        sqlx::query_scalar("select state from knowledge.analysis_runs where run_id = $1")
            .bind(run_id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(state, "persisted");

    let (slow_run_id, slow_context, slow_document) = run_and_context(&database).await?;
    let slow = SlowProvider;
    let slow_pipeline = ArticlePipeline::new(
        &database.database,
        &slow,
        &blobs,
        std::time::Duration::from_millis(1),
    );
    let timed_out = slow_pipeline
        .execute(
            slow_run_id,
            build_generation_request(&slow_context)?,
            &slow_context,
            &slow_document,
        )
        .await;
    assert!(matches!(timed_out, Err(PipelineError::Timeout)));
    let timeout_attempts: i64 =
        sqlx::query_scalar("select count(*) from knowledge.analysis_attempts where run_id = $1")
            .bind(slow_run_id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(timeout_attempts, 2);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn one_invalid_response_repairs_once() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    let provider = ScriptedProvider::new([
        Ok(ProviderResponse {
            bytes: br#"{"summary":"","key_points":[]}"#.to_vec(),
            request_id: Some("request-invalid".to_owned()),
            usage: ProviderUsage {
                input_tokens: 20,
                output_tokens: 4,
            },
        }),
        Ok(valid_response("request-repair")),
    ]);
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );

    let result = pipeline
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await?;
    assert_eq!(result.summary, "A grounded summary.");
    let attempts: Vec<(i16, String, serde_json::Value, String)> = sqlx::query_as(
        "select ordinal, reason, raw_response, outcome
         from knowledge.analysis_attempts where run_id = $1 order by ordinal",
    )
    .bind(run_id)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].1, "initial");
    assert_eq!(attempts[1].1, "repair");
    assert_ne!(attempts[0].2, attempts[1].2);
    assert_eq!(attempts[0].3, "invalid");
    assert_eq!(attempts[1].3, "accepted");

    let requests = provider.requests()?;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].system_policy, requests[1].system_policy);
    assert_eq!(requests[0].source_content, requests[1].source_content);
    assert!(
        requests[1]
            .task_instruction
            .contains("Repair validation code: schema.")
    );
    let output_count: i64 =
        sqlx::query_scalar("select count(*) from knowledge.analysis_outputs where run_id = $1")
            .bind(run_id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(output_count, 1);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn second_invalid_response_fails_without_a_third_call()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    let invalid = ProviderResponse {
        bytes: br#"{"summary":"","key_points":[]}"#.to_vec(),
        request_id: Some("request-invalid".to_owned()),
        usage: ProviderUsage {
            input_tokens: 20,
            output_tokens: 4,
        },
    };
    let provider = ScriptedProvider::new([
        Ok(invalid.clone()),
        Ok(invalid),
        Ok(valid_response("must-not-run")),
    ]);
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );

    let result = pipeline
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await;
    assert!(matches!(result, Err(PipelineError::Invalid)));
    assert_eq!(provider.call_count()?, 2);
    let (state, attempt_count, output_count): (String, i64, i64) = sqlx::query_as(
        "select state,
                (select count(*) from knowledge.analysis_attempts where run_id = $1),
                (select count(*) from knowledge.analysis_outputs where run_id = $1)
         from knowledge.analysis_runs where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "failed");
    assert_eq!(attempt_count, 2);
    assert_eq!(output_count, 0);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn permanent_failures_end_without_an_extra_call() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 1);

    let (provider_run_id, provider_context, provider_document) = run_and_context(&database).await?;
    let permanent = ScriptedProvider::new([Err(ProviderError::Permanent)]);
    let provider_pipeline = ArticlePipeline::new(
        &database.database,
        &permanent,
        &blobs,
        std::time::Duration::from_secs(1),
    );
    assert!(
        provider_pipeline
            .execute(
                provider_run_id,
                build_generation_request(&provider_context)?,
                &provider_context,
                &provider_document,
            )
            .await
            .is_err()
    );
    assert_eq!(permanent.call_count()?, 1);

    let (raw_run_id, raw_context, raw_document) = run_and_context(&database).await?;
    let oversized = ScriptedProvider::new([Ok(valid_response("oversized"))]);
    let raw_pipeline = ArticlePipeline::new(
        &database.database,
        &oversized,
        &blobs,
        std::time::Duration::from_secs(1),
    );
    assert!(matches!(
        raw_pipeline
            .execute(
                raw_run_id,
                build_generation_request(&raw_context)?,
                &raw_context,
                &raw_document,
            )
            .await,
        Err(PipelineError::Blob(_))
    ));
    assert_eq!(oversized.call_count()?, 1);
    let states: Vec<String> = sqlx::query_scalar(
        "select state from knowledge.analysis_runs where run_id = any($1) order by run_id",
    )
    .bind(vec![provider_run_id, raw_run_id])
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(states, ["failed", "failed"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn completed_replay_returns_one_atomic_result_without_provider_call()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    let provider = ScriptedProvider::new([Ok(valid_response("request-replay"))]);
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );
    let request = build_generation_request(&context)?;

    let first = pipeline
        .execute(run_id, request.clone(), &context, &document)
        .await?;
    sqlx::query("update knowledge.analysis_runs set state = 'persisted' where run_id = $1")
        .bind(run_id)
        .execute(database.database.pool())
        .await?;
    let replay = pipeline
        .execute(run_id, request, &context, &document)
        .await?;

    assert_eq!(replay, first);
    assert_eq!(provider.call_count()?, 1);
    let (state, output_count): (String, i64) = sqlx::query_as(
        "select state,
                (select count(*) from knowledge.analysis_outputs where run_id = $1)
         from knowledge.analysis_runs where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "persisted");
    assert_eq!(output_count, 1);

    database.cleanup().await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct SharedWriter(&'static Mutex<Vec<u8>>);

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for SharedWriter {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

impl std::io::Write for SharedWriter {
    fn write(&mut self, buffer: &[u8]) -> Result<usize, std::io::Error> {
        let mut bytes = self.0.lock().map_err(lock_error)?;
        std::io::Write::write(&mut *bytes, buffer)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

fn lock_error<T>(_error: std::sync::PoisonError<T>) -> std::io::Error {
    std::io::Error::other("telemetry capture lock was poisoned")
}

#[derive(Debug)]
struct SlowProvider;

impl LlmProvider for SlowProvider {
    fn identity(&self) -> ProviderIdentity {
        ProviderIdentity {
            provider: "scripted_slow".to_owned(),
            model: "fake_default_v1".to_owned(),
        }
    }

    async fn generate_json(
        &self,
        _request: GenerationRequest,
    ) -> Result<ProviderResponse, ProviderFailure> {
        tokio::time::sleep(std::time::Duration::from_mins(1)).await;
        Err(ProviderError::Permanent.into())
    }
}

#[tokio::test]
async fn real_attempts_record_identity_latency_and_failure_class()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    let transport = FakeTransport::start(Vec::new()).await?;
    let provider = controlled_openrouter(&database, &transport, 3)?;
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );

    let outcome = pipeline
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await;

    assert!(outcome.is_err());
    assert_eq!(transport.request_count()?, 6);
    let attempts: Vec<AttemptFacts> = sqlx::query_as(
        "select ordinal, provider, model, duration_ms, error_class, http_status
         from knowledge.analysis_attempts where run_id = $1 order by ordinal",
    )
    .bind(run_id)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(attempts.len(), 2);
    for attempt in &attempts {
        assert_eq!(attempt.1, "openrouter");
        assert_eq!(attempt.2, "openai/gpt-oss-20b");
        assert!(attempt.3.is_some());
        assert_eq!(attempt.4.as_deref(), Some("server_error"));
        assert_eq!(attempt.5, Some(500));
    }

    database.cleanup().await?;
    Ok(())
}

/// One attempt row's structured adapter facts.
type AttemptFacts = (
    i16,
    String,
    String,
    Option<i32>,
    Option<String>,
    Option<i16>,
);
