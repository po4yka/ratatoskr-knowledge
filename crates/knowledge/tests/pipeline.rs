//! Durable fake-provider pipeline tests.

use std::sync::{Arc, Mutex};

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    AnalysisIdentity, ArticlePipeline, BlobStore, GenerationRequest, LlmProvider, PipelineError,
    ProviderError, ProviderResponse, ProviderUsage, ScriptedProvider, SourceReference,
    build_generation_request, prepare_context,
};

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
    let telemetry = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(SharedWriter(Arc::clone(&telemetry)))
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

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
    let captured = String::from_utf8(telemetry.lock().map_err(lock_error)?.clone())?;
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
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

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
    async fn generate_json(
        &self,
        _request: GenerationRequest,
    ) -> Result<ProviderResponse, ProviderError> {
        tokio::time::sleep(std::time::Duration::from_mins(1)).await;
        Err(ProviderError::Permanent)
    }
}
