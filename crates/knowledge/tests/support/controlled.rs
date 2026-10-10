//! The controlled `OpenRouter` provider that the transport-level pipeline tests run against.

use std::sync::Arc;

use ratatoskr_knowledge::test_support::{FakeTransport, TestDatabase};
use ratatoskr_knowledge::{
    BudgetLedger, BudgetLimits, ControlledProvider, OpenRouterProvider, OpenRouterSettings,
    ProviderSecret, RateLimiter, RetryPolicy, SpendControls, TokenPrices,
};

pub(crate) fn controlled_openrouter(
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
