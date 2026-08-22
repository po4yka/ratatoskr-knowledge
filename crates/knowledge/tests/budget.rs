//! Durable spend-budget ledger and pre-call enforcement tests.

use std::sync::Arc;
use std::time::Duration;

use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport};
use ratatoskr_knowledge::{
    BudgetLedger, BudgetLimits, ControlledProvider, GenerationRequest, LlmProvider as _,
    OpenRouterProvider, OpenRouterSettings, ProviderError, ProviderFailureClass, ProviderSecret,
    RateLimiter, RetryPolicy, SpendControls, TokenPrices,
};
use serde_json::json;

const SUCCESS_ENVELOPE: &str = include_str!("fixtures/openrouter/success.json");
const CREDENTIAL: &str = "sk-or-v1-LEAKME";

#[tokio::test]
async fn projected_daily_overrun_blocks_before_transport() -> Result<(), Box<dyn std::error::Error>>
{
    let database = ratatoskr_knowledge::test_support::TestDatabase::create().await?;
    let transport = FakeTransport::start(vec![FakeReply::bytes(
        200,
        SUCCESS_ENVELOPE.as_bytes().to_vec(),
    )])
    .await?;
    let ledger = BudgetLedger::new(database.database.pool().clone());
    ledger
        .record_usage(
            "openrouter",
            "openai/gpt-oss-20b",
            ratatoskr_knowledge::ProviderUsage {
                input_tokens: 90,
                output_tokens: 10,
            },
            0,
        )
        .await?;
    let provider = controlled_provider(&database, &transport, tight_limits(), zero_prices())?;

    let failure = provider
        .generate_json(sample_request())
        .await
        .err()
        .ok_or("expected budget exhaustion")?;

    assert_eq!(failure.error, ProviderError::BudgetExhausted);
    assert_eq!(failure.class, ProviderFailureClass::BudgetExhausted);
    assert_eq!(transport.request_count()?, 0);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn monthly_ceiling_counts_earlier_days() -> Result<(), Box<dyn std::error::Error>> {
    let database = ratatoskr_knowledge::test_support::TestDatabase::create().await?;
    sqlx::query(
        "insert into knowledge.provider_usage (
            usage_id, provider, model, input_tokens, output_tokens,
            estimated_cost_micro_usd, recorded_at
         ) values ($1, 'openrouter', 'openai/gpt-oss-20b', 140, 0, 0,
                   now() - interval '1 day')",
    )
    .bind(uuid::Uuid::now_v7())
    .execute(database.database.pool())
    .await?;
    let transport = FakeTransport::start(vec![FakeReply::bytes(
        200,
        SUCCESS_ENVELOPE.as_bytes().to_vec(),
    )])
    .await?;
    let limits = BudgetLimits {
        daily_tokens: 100_000,
        monthly_tokens: 150,
        daily_cost_micro_usd: u64::MAX - 1,
        monthly_cost_micro_usd: u64::MAX - 1,
    };
    let provider = controlled_provider(&database, &transport, limits, zero_prices())?;

    let failure = provider
        .generate_json(sample_request())
        .await
        .err()
        .ok_or("expected monthly exhaustion")?;

    assert_eq!(failure.error, ProviderError::BudgetExhausted);
    assert_eq!(transport.request_count()?, 0);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn cost_ceiling_blocks_with_token_headroom() -> Result<(), Box<dyn std::error::Error>> {
    let database = ratatoskr_knowledge::test_support::TestDatabase::create().await?;
    let transport = FakeTransport::start(vec![FakeReply::bytes(
        200,
        SUCCESS_ENVELOPE.as_bytes().to_vec(),
    )])
    .await?;
    let limits = BudgetLimits {
        daily_tokens: u64::MAX - 1,
        monthly_tokens: u64::MAX - 1,
        daily_cost_micro_usd: 1,
        monthly_cost_micro_usd: u64::MAX - 1,
    };
    let prices = TokenPrices {
        input_micro_usd_per_mtoken: 1_000_000,
        output_micro_usd_per_mtoken: 0,
    };
    let provider = controlled_provider(&database, &transport, limits, prices)?;

    let failure = provider
        .generate_json(sample_request())
        .await
        .err()
        .ok_or("expected cost exhaustion")?;

    assert_eq!(failure.error, ProviderError::BudgetExhausted);
    assert_eq!(transport.request_count()?, 0);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn successful_usage_is_recorded_once_with_cost() -> Result<(), Box<dyn std::error::Error>> {
    let database = ratatoskr_knowledge::test_support::TestDatabase::create().await?;
    let transport = FakeTransport::start(vec![FakeReply::bytes(
        200,
        SUCCESS_ENVELOPE.as_bytes().to_vec(),
    )])
    .await?;
    let prices = TokenPrices {
        input_micro_usd_per_mtoken: 2_000_000,
        output_micro_usd_per_mtoken: 8_000_000,
    };
    let provider = controlled_provider(&database, &transport, generous_limits(), prices)?;

    let response = provider.generate_json(sample_request()).await?;

    assert_eq!(response.usage.input_tokens, 57);
    assert_eq!(response.usage.output_tokens, 40);
    assert_eq!(transport.request_count()?, 1);
    let ledger = BudgetLedger::new(database.database.pool().clone());
    let (tokens, cost) = ledger
        .window_totals("openrouter", ratatoskr_knowledge::BudgetWindow::Daily)
        .await?;
    assert_eq!(tokens, 97);
    let expected_cost = (57_u64 * 2_000_000 + 40 * 8_000_000).div_ceil(1_000_000);
    assert_eq!(cost, expected_cost);
    let rows: i64 = sqlx::query_scalar("select count(*) from knowledge.provider_usage")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(rows, 1);

    database.cleanup().await?;
    Ok(())
}

fn controlled_provider(
    database: &ratatoskr_knowledge::test_support::TestDatabase,
    transport: &FakeTransport,
    limits: BudgetLimits,
    prices: TokenPrices,
) -> Result<ControlledProvider<OpenRouterProvider>, Box<dyn std::error::Error>> {
    let inner = OpenRouterProvider::new(OpenRouterSettings {
        base_url: format!("http://{}/api/v1", transport.local_addr()),
        model: "openai/gpt-oss-20b".to_owned(),
        credential: ProviderSecret::new(CREDENTIAL.to_owned()),
        max_output_tokens: 16,
        response_byte_cap: 4_096,
        call_deadline: Duration::from_secs(5),
        connect_timeout: Duration::from_secs(5),
        retry: RetryPolicy::new(3, 0, 0),
    })?;
    Ok(ControlledProvider::new(
        inner,
        Arc::new(RateLimiter::new(Duration::ZERO)),
        BudgetLedger::new(database.database.pool().clone()),
        SpendControls {
            limits,
            prices,
            max_output_tokens: 16,
        },
    ))
}

fn sample_request() -> GenerationRequest {
    GenerationRequest {
        prompt_version: "article_prompt_v1".to_owned(),
        system_policy: "fixed policy".to_owned(),
        task_instruction: "fixed task".to_owned(),
        output_schema: json!({"type": "object"}),
        source_content: "block 0 paragraph: \"Evidence.\"".to_owned(),
    }
}

fn tight_limits() -> BudgetLimits {
    BudgetLimits {
        daily_tokens: 100,
        monthly_tokens: u64::MAX - 1,
        daily_cost_micro_usd: u64::MAX - 1,
        monthly_cost_micro_usd: u64::MAX - 1,
    }
}

fn generous_limits() -> BudgetLimits {
    BudgetLimits {
        daily_tokens: u64::MAX - 1,
        monthly_tokens: u64::MAX - 1,
        daily_cost_micro_usd: u64::MAX - 1,
        monthly_cost_micro_usd: u64::MAX - 1,
    }
}

fn zero_prices() -> TokenPrices {
    TokenPrices {
        input_micro_usd_per_mtoken: 0,
        output_micro_usd_per_mtoken: 0,
    }
}
