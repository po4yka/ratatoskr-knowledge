//! Durable fake-provider pipeline tests.

use std::sync::Arc;
use std::sync::{Mutex, Once};

use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport};
use ratatoskr_knowledge::{
    BudgetLedger, BudgetLimits, ControlledProvider, OpenRouterProvider, OpenRouterSettings,
    ProviderSecret, RateLimiter, RetryPolicy, SpendControls, TokenPrices,
};

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    AnalysisIdentity, ArticlePipeline, BlobStore, GenerationRequest, LlmProvider, PipelineError,
    ProviderError, ProviderFailure, ProviderIdentity, ProviderResponse, ProviderUsage,
    ScriptedProvider, SourceReference, build_generation_request, prepare_context,
};

static TELEMETRY: Mutex<Vec<u8>> = Mutex::new(Vec::new());
static TELEMETRY_INIT: Once = Once::new();

#[tokio::test(flavor = "current_thread")]
async fn malformed_response_is_stored_before_json_validation()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 1_024);
    let (run_id, context) = run_and_context(&database).await?;
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
            .execute(run_id, build_generation_request(&context)?, &context)
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
    let (run_id, context) = run_and_context(&database).await?;
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
        .execute(run_id, build_generation_request(&context)?, &context)
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
    assert_eq!(state, "completed");

    let (slow_run_id, slow_context) = run_and_context(&database).await?;
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
    let (run_id, context) = run_and_context(&database).await?;
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
        .execute(run_id, build_generation_request(&context)?, &context)
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
    let (run_id, context) = run_and_context(&database).await?;
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
        .execute(run_id, build_generation_request(&context)?, &context)
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

    let (provider_run_id, provider_context) = run_and_context(&database).await?;
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
            )
            .await
            .is_err()
    );
    assert_eq!(permanent.call_count()?, 1);

    let (raw_run_id, raw_context) = run_and_context(&database).await?;
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
    let (run_id, context) = run_and_context(&database).await?;
    let provider = ScriptedProvider::new([Ok(valid_response("request-replay"))]);
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );
    let request = build_generation_request(&context)?;

    let first = pipeline.execute(run_id, request.clone(), &context).await?;
    sqlx::query("update knowledge.analysis_runs set state = 'persisted' where run_id = $1")
        .bind(run_id)
        .execute(database.database.pool())
        .await?;
    let replay = pipeline.execute(run_id, request, &context).await?;

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
    assert_eq!(state, "completed");
    assert_eq!(output_count, 1);

    database.cleanup().await?;
    Ok(())
}

async fn run_and_context(
    database: &TestDatabase,
) -> Result<(uuid::Uuid, ratatoskr_knowledge::PreparedContext), Box<dyn std::error::Error>> {
    let document = Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:pipeline")?,
        content_digest: digest('a')?,
        title: Some("Pipeline".to_owned()),
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            text: "Evidence.".to_owned(),
        }],
        provenance: Vec::new(),
    };
    let source = database
        .database
        .register_source(&SourceReference {
            tenant: TenantRef::of_user(UserId::new_v7()),
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
    let run = database
        .database
        .create_run(&AnalysisIdentity {
            source_revision_id: source.id,
            contract_version: "article_v1".to_owned(),
            prompt_version: "article_prompt_v1".to_owned(),
            context_builder_version: "document_context_v1".to_owned(),
            model_policy: "fake_default_v1".to_owned(),
        })
        .await?;
    Ok((run.id, prepare_context(&document, 1_000)?))
}

fn digest(digit: char) -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    })
}

fn valid_response(request_id: &str) -> ProviderResponse {
    ProviderResponse {
        bytes: br#"{
            "summary": "A grounded summary.",
            "key_points": [{"text": "Evidence exists.", "source_block_indexes": [0]}]
        }"#
        .to_vec(),
        request_id: Some(request_id.to_owned()),
        usage: ProviderUsage {
            input_tokens: 20,
            output_tokens: 10,
        },
    }
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
    let (run_id, context) = run_and_context(&database).await?;
    let transport = FakeTransport::start(Vec::new()).await?;
    let provider = controlled_openrouter(&database, &transport, 3)?;
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );

    let outcome = pipeline
        .execute(run_id, build_generation_request(&context)?, &context)
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

#[tokio::test]
async fn cancelled_mid_request_keeps_durable_state_and_replays_once()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context) = run_and_context(&database).await?;
    let request = build_generation_request(&context)?;
    let stalled = FakeTransport::start(vec![FakeReply::stall()]).await?;
    let stalled_provider = controlled_openrouter(&database, &stalled, 3)?;

    let task_database = database.database.clone();
    let task_blobs = blobs.clone();
    let task_context = context.clone();
    let task_request = request.clone();
    let handle = tokio::spawn(async move {
        let pipeline = ArticlePipeline::new(
            &task_database,
            &stalled_provider,
            &task_blobs,
            std::time::Duration::from_secs(5),
        );
        pipeline.execute(run_id, task_request, &task_context).await
    });
    let mut waited = 0;
    while stalled.request_count()? == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        waited += 1;
        assert!(
            waited < 500,
            "the stalled transport never received a request"
        );
    }
    handle.abort();
    let joined = handle.await;
    assert!(joined.is_err(), "the task should have been cancelled");

    let (state, outcome): (String, String) = sqlx::query_as(
        "select runs.state, attempts.outcome
         from knowledge.analysis_runs runs
         join knowledge.analysis_attempts attempts
             on attempts.run_id = runs.run_id and attempts.ordinal = 1
         where runs.run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "model_requested");
    assert_eq!(outcome, "requested");
    let outputs: i64 =
        sqlx::query_scalar("select count(*) from knowledge.analysis_outputs where run_id = $1")
            .bind(run_id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(outputs, 0);

    let healthy = FakeTransport::start(vec![FakeReply::bytes(200, valid_envelope_bytes())]).await?;
    let healthy_provider = controlled_openrouter(&database, &healthy, 3)?;
    let replay_pipeline = ArticlePipeline::new(
        &database.database,
        &healthy_provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );
    let replayed = replay_pipeline.execute(run_id, request, &context).await?;
    assert_eq!(replayed.summary, "A grounded summary.");
    let (state, attempts, outputs): (String, i64, i64) = sqlx::query_as(
        "select state,
                (select count(*) from knowledge.analysis_attempts where run_id = $1),
                (select count(*) from knowledge.analysis_outputs where run_id = $1)
         from knowledge.analysis_runs where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "completed");
    assert_eq!(attempts, 2);
    assert_eq!(outputs, 1);

    database.cleanup().await?;
    Ok(())
}

// A second cancellation consumes the last ordinal; the next replay must fail
// the run explicitly instead of leaving it stuck mid-state.
#[tokio::test]
async fn exhausted_cancellation_replay_fails_explicitly() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (second_run_id, second_context) = run_and_context(&database).await?;
    sqlx::query(
        "insert into knowledge.analysis_attempts (
            run_id, ordinal, reason, provider, model_policy, outcome
         ) values ($1, 1, 'initial', 'openrouter', 'fake_default_v1', 'requested')",
    )
    .bind(second_run_id)
    .execute(database.database.pool())
    .await?;
    sqlx::query("update knowledge.analysis_runs set state = 'model_requested' where run_id = $1")
        .bind(second_run_id)
        .execute(database.database.pool())
        .await?;
    let exhausted = FakeTransport::start(vec![FakeReply::stall()]).await?;
    let exhausted_provider = controlled_openrouter(&database, &exhausted, 1)?;
    let task_database = database.database.clone();
    let task_blobs = blobs.clone();
    let task_context = second_context.clone();
    let second_request = build_generation_request(&second_context)?;
    let second_handle = tokio::spawn(async move {
        let pipeline = ArticlePipeline::new(
            &task_database,
            &exhausted_provider,
            &task_blobs,
            std::time::Duration::from_millis(50),
        );
        pipeline
            .execute(second_run_id, second_request, &task_context)
            .await
    });
    let mut waited = 0;
    while exhausted.request_count()? == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        waited += 1;
        assert!(
            waited < 500,
            "the stalled transport never received a request"
        );
    }
    second_handle.abort();
    let _ignored = second_handle.await;

    let healthy_again =
        FakeTransport::start(vec![FakeReply::bytes(200, valid_envelope_bytes())]).await?;
    let again_provider = controlled_openrouter(&database, &healthy_again, 1)?;
    let again_pipeline = ArticlePipeline::new(
        &database.database,
        &again_provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );
    let replay_outcome = again_pipeline
        .execute(
            second_run_id,
            build_generation_request(&second_context)?,
            &second_context,
        )
        .await;
    assert!(replay_outcome.is_err());
    let final_state: String =
        sqlx::query_scalar("select state from knowledge.analysis_runs where run_id = $1")
            .bind(second_run_id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(final_state, "failed");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn flaky_transport_keeps_retry_and_repair_bounded() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);

    let (failing_run_id, failing_context) = run_and_context(&database).await?;
    let always_fault = FakeTransport::start(Vec::new()).await?;
    let failing_provider = controlled_openrouter(&database, &always_fault, 3)?;
    let failing_pipeline = ArticlePipeline::new(
        &database.database,
        &failing_provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );
    let failing = failing_pipeline
        .execute(
            failing_run_id,
            build_generation_request(&failing_context)?,
            &failing_context,
        )
        .await;
    assert!(failing.is_err());
    let (state, attempts): (String, i64) = sqlx::query_as(
        "select state,
                (select count(*) from knowledge.analysis_attempts where run_id = $1)
         from knowledge.analysis_runs where run_id = $1",
    )
    .bind(failing_run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "failed");
    assert_eq!(attempts, 2);
    assert_eq!(always_fault.request_count()?, 6);

    let (repair_run_id, repair_context) = run_and_context(&database).await?;
    let invalid_content = FakeReply::bytes(
        200,
        br#"{
            "id": "gen-1755858000-recorded000000003",
            "choices": [{"message": {"role": "assistant",
                "content": "{\"summary\":\"\",\"key_points\":[]}"}}],
            "usage": {"prompt_tokens": 20, "completion_tokens": 4}
        }"#
        .to_vec(),
    );
    let flaky = FakeTransport::start(vec![
        invalid_content,
        FakeReply::bytes(502, Vec::new()),
        FakeReply::bytes(502, Vec::new()),
        FakeReply::bytes(200, valid_envelope_bytes()),
    ])
    .await?;
    let repair_provider = controlled_openrouter(&database, &flaky, 3)?;
    let repair_pipeline = ArticlePipeline::new(
        &database.database,
        &repair_provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );
    let repaired = repair_pipeline
        .execute(
            repair_run_id,
            build_generation_request(&repair_context)?,
            &repair_context,
        )
        .await?;
    assert_eq!(repaired.summary, "A grounded summary.");
    let (repair_state, repair_attempts, repair_outputs): (String, i64, i64) = sqlx::query_as(
        "select state,
                (select count(*) from knowledge.analysis_attempts where run_id = $1),
                (select count(*) from knowledge.analysis_outputs where run_id = $1)
         from knowledge.analysis_runs where run_id = $1",
    )
    .bind(repair_run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(repair_state, "completed");
    assert_eq!(repair_attempts, 2);
    assert_eq!(repair_outputs, 1);
    assert_eq!(flaky.request_count()?, 4);

    database.cleanup().await?;
    Ok(())
}

fn controlled_openrouter(
    database: &TestDatabase,
    transport: &FakeTransport,
    max_tries: u32,
) -> Result<ControlledProvider<OpenRouterProvider>, Box<dyn std::error::Error>> {
    let inner = OpenRouterProvider::new(OpenRouterSettings {
        base_url: format!("http://{}/api/v1", transport.local_addr()),
        model: "openai/gpt-oss-20b".to_owned(),
        credential: ProviderSecret::new("sk-or-v1-LEAKME".to_owned()),
        max_output_tokens: 16,
        response_byte_cap: 4_096,
        call_deadline: std::time::Duration::from_secs(2),
        connect_timeout: std::time::Duration::from_secs(2),
        retry: RetryPolicy::new(max_tries, 0, 0),
    })?;
    Ok(ControlledProvider::new(
        inner,
        Arc::new(RateLimiter::new(std::time::Duration::ZERO)),
        BudgetLedger::new(database.database.pool().clone()),
        SpendControls {
            limits: BudgetLimits {
                daily_tokens: u64::MAX - 1,
                monthly_tokens: u64::MAX - 1,
                daily_cost_micro_usd: u64::MAX - 1,
                monthly_cost_micro_usd: u64::MAX - 1,
            },
            prices: TokenPrices {
                input_micro_usd_per_mtoken: 0,
                output_micro_usd_per_mtoken: 0,
            },
            max_output_tokens: 16,
        },
    ))
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

fn valid_envelope_bytes() -> Vec<u8> {
    br#"{
        "id": "gen-1755858000-recorded000000002",
        "choices": [{"message": {"role": "assistant", "content": "{\"summary\":\"A grounded summary.\",\"key_points\":[{\"text\":\"Evidence exists.\",\"source_block_indexes\":[0]}]}"}}],
        "usage": {"prompt_tokens": 20, "completion_tokens": 10}
    }"#
    .to_vec()
}
