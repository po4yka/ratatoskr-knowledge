//! Embeddings seam boundary tests: wire mapping and budget refusal.

use std::sync::Arc;
use std::time::Duration;

use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport};
use ratatoskr_knowledge::{
    BudgetLedger, BudgetLimits, ControlledEmbeddings, EmbeddingProvider as _, EmbeddingResponse,
    EmbeddingsSettings, OpenAiCompatibleEmbeddings, ProviderError, ProviderFailureClass,
    ProviderSecret, ProviderUsage, RateLimiter, RetryPolicy, ScriptedEmbeddingProvider,
    ScriptedEmbeddingSuccess, TokenPrices, embeddings_request_body,
};

const CREDENTIAL: &str = "sk-embeddings-LEAKME";
const MODEL: &str = "text-embedding-fake-001";

const SUCCESS_ENVELOPE: &str = r#"{
    "object": "list",
    "model": "text-embedding-fake-001",
    "data": [
        {"object": "embedding", "index": 0, "embedding": [0.25, -0.5, 1.0, -1.0]},
        {"object": "embedding", "index": 1, "embedding": [0.5, 0.5, -0.25, 0.125]}
    ],
    "usage": {"prompt_tokens": 19, "total_tokens": 19},
    "unexpected_top_level": {"nested": true}
}"#;
const RATE_LIMITED_ENVELOPE: &str =
    r#"{"error": {"message": "slow down", "type": "rate_limit_error"}}"#;

#[tokio::test]
async fn openai_compatible_embeddings_maps_wire() -> Result<(), Box<dyn std::error::Error>> {
    let inputs = vec!["first chunk".to_owned(), "second chunk".to_owned()];

    let body = embeddings_request_body(MODEL, &inputs)?;
    assert_eq!(body["model"], MODEL);
    let embedded_inputs = body["input"].as_array().ok_or("input must be an array")?;
    assert_eq!(embedded_inputs.len(), inputs.len());
    assert_eq!(embedded_inputs[0], inputs[0]);
    assert_eq!(embedded_inputs[1], inputs[1]);
    assert!(
        !body.to_string().contains(CREDENTIAL),
        "no credential may enter a serialized body"
    );

    let transport = FakeTransport::start(vec![FakeReply::bytes(
        200,
        SUCCESS_ENVELOPE.as_bytes().to_vec(),
    )])
    .await?;
    let provider = OpenAiCompatibleEmbeddings::new(adapter_settings(transport.local_addr()))?;

    let identity = provider.identity();
    assert_eq!(identity.provider, "openai-compatible");
    assert_eq!(identity.model, MODEL);
    assert_eq!(identity.dimensions, 4);
    assert_eq!(identity.prompt_version, "none.v1");

    let response = provider.embed(inputs).await?;

    assert_eq!(transport.request_count()?, 1);
    let recorded = transport.recorded()?;
    assert_eq!(recorded[0].path, "/api/v1/embeddings");
    let expected_authorization = format!("Bearer {CREDENTIAL}");
    assert_eq!(
        recorded[0].authorization.as_deref(),
        Some(expected_authorization.as_str())
    );
    assert_eq!(
        response,
        EmbeddingResponse {
            vectors: vec![vec![0.25, -0.5, 1.0, -1.0], vec![0.5, 0.5, -0.25, 0.125]],
            input_tokens: 19,
        }
    );

    let limited_transport = FakeTransport::start(vec![FakeReply::bytes(
        429,
        RATE_LIMITED_ENVELOPE.as_bytes().to_vec(),
    )])
    .await?;
    let limited =
        OpenAiCompatibleEmbeddings::new(adapter_settings(limited_transport.local_addr()))?;
    let failure = limited
        .embed(vec!["chunk".to_owned()])
        .await
        .err()
        .ok_or("expected a rate-limit failure")?;
    assert_eq!(failure.error, ProviderError::Transient);
    assert_eq!(failure.class, ProviderFailureClass::RateLimited);
    assert_eq!(failure.http_status, Some(429));

    let oversized_transport = FakeTransport::start(vec![FakeReply::oversized(8_192)]).await?;
    let oversized =
        OpenAiCompatibleEmbeddings::new(adapter_settings(oversized_transport.local_addr()))?;
    let failure = oversized
        .embed(vec!["chunk".to_owned()])
        .await
        .err()
        .ok_or("expected a size failure")?;
    assert_eq!(failure.error, ProviderError::Permanent);
    assert_eq!(failure.class, ProviderFailureClass::SizeLimit);
    assert_eq!(oversized_transport.request_count()?, 1);
    Ok(())
}

fn adapter_settings(address: std::net::SocketAddr) -> EmbeddingsSettings {
    EmbeddingsSettings {
        base_url: format!("http://{address}/api/v1"),
        model: MODEL.to_owned(),
        credential: ProviderSecret::new(CREDENTIAL.to_owned()),
        dimensions: 4,
        prompt_version: "none.v1".to_owned(),
        max_input_characters: 2_048,
        response_byte_cap: 4_096,
        call_deadline: Duration::from_millis(400),
        connect_timeout: Duration::from_millis(400),
        retry: RetryPolicy::new(1, 0, 0),
    }
}

#[tokio::test]
async fn controlled_embeddings_refuses_exhausted_budget() -> Result<(), Box<dyn std::error::Error>>
{
    let database = ratatoskr_knowledge::test_support::TestDatabase::create().await?;
    let ledger = BudgetLedger::new(database.database.pool().clone());
    // The wrapper keys spend windows on the inner identity's provider string,
    // and the scripted fake reports "scripted_fake".
    ledger
        .record_usage(
            "scripted_fake",
            "fake_default_v1",
            ProviderUsage {
                input_tokens: 90,
                output_tokens: 10,
            },
            0,
        )
        .await?;
    let inner =
        ScriptedEmbeddingProvider::new(4, [Ok(ScriptedEmbeddingSuccess { input_tokens: 7 })]);
    let probe = inner.clone();
    let controlled = ControlledEmbeddings::new(
        inner,
        Arc::new(RateLimiter::new(Duration::ZERO)),
        ledger,
        BudgetLimits {
            daily_tokens: 100,
            monthly_tokens: u64::MAX - 1,
            daily_cost_micro_usd: u64::MAX - 1,
            monthly_cost_micro_usd: u64::MAX - 1,
        },
        TokenPrices {
            input_micro_usd_per_mtoken: 0,
            output_micro_usd_per_mtoken: 0,
        },
    );

    let failure = controlled
        .embed(vec!["projected chunk text".to_owned()])
        .await
        .err()
        .ok_or("expected budget exhaustion")?;

    assert_eq!(failure.error, ProviderError::BudgetExhausted);
    assert_eq!(failure.class, ProviderFailureClass::BudgetExhausted);
    assert_eq!(controlled.identity().provider, "scripted_fake");
    assert_eq!(
        probe.call_count()?,
        0,
        "an exhausted budget must refuse before any provider call"
    );
    let rows: i64 = sqlx::query_scalar("select count(*) from knowledge.provider_usage")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(rows, 1, "a refusal records nothing as usage");

    database.cleanup().await?;
    Ok(())
}
