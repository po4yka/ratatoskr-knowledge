//! Inference provider construction shared by the recap and ingest workers.

use std::sync::Arc;
use std::time::Duration;

use ratatoskr_knowledge::{
    BudgetLedger, BudgetLimits, Config, ControlledProvider, Database, ExtractiveArticleProvider,
    GenerationRequest, IngestProviderMode, LlmProvider, OpenRouterProvider, OpenRouterSettings,
    ProviderFailure, ProviderIdentity, ProviderResponse, RateLimiter, RetryPolicy, SpendControls,
    TokenPrices,
};

/// The controlled `OpenRouter` provider every real analysis goes through.
pub(crate) type ControlledOpenRouter = ControlledProvider<OpenRouterProvider>;

/// The configured provider could not be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the inference provider could not be constructed")]
pub struct ProviderRuntimeError;

/// Builds the process-wide limiter that spaces every inference request.
///
/// The recap and ingest workers share the one limiter, so `provider_requests_per_minute` bounds
/// the process, not each worker.
///
/// # Errors
///
/// Returns [`ProviderRuntimeError`] when the configured rate does not divide a minute.
pub fn inference_limiter(config: &Config) -> Result<Arc<RateLimiter>, ProviderRuntimeError> {
    let spacing =
        Duration::try_from_secs_f64(60.0 / f64::from(config.limits.provider_requests_per_minute))
            .map_err(|_| ProviderRuntimeError)?;
    Ok(Arc::new(RateLimiter::new(spacing)))
}

/// Builds the controlled `OpenRouter` provider from the process configuration.
///
/// The caller passes the one process-wide `limiter`; the spend ledger and the call bounds are
/// the same wherever a model is called.
pub(crate) fn openrouter_provider(
    config: &Config,
    database: &Database,
    limiter: Arc<RateLimiter>,
) -> Result<Box<ControlledOpenRouter>, ProviderRuntimeError> {
    let configured = config
        .provider
        .openrouter
        .as_ref()
        .ok_or(ProviderRuntimeError)?;
    let call_deadline = Duration::from_millis(config.limits.provider_timeout_ms);
    let inner = OpenRouterProvider::new(OpenRouterSettings {
        base_url: configured.base_url.clone(),
        model: configured.model.clone(),
        credential: configured.api_key.clone(),
        max_output_tokens: config.limits.provider_max_output_tokens,
        response_byte_cap: config.limits.raw_response_bytes,
        call_deadline,
        connect_timeout: call_deadline.min(Duration::from_secs(5)),
        retry: RetryPolicy::new(3, 200, 2_000),
    })
    .map_err(|_| ProviderRuntimeError)?;
    Ok(Box::new(ControlledProvider::new(
        inner,
        limiter,
        BudgetLedger::new(database.pool().clone()),
        SpendControls {
            limits: BudgetLimits {
                daily_tokens: config.limits.provider_daily_token_budget,
                monthly_tokens: config.limits.provider_monthly_token_budget,
                daily_cost_micro_usd: config.limits.provider_daily_cost_micro_usd,
                monthly_cost_micro_usd: config.limits.provider_monthly_cost_micro_usd,
            },
            prices: TokenPrices {
                input_micro_usd_per_mtoken: configured.input_micro_usd_per_mtoken,
                output_micro_usd_per_mtoken: configured.output_micro_usd_per_mtoken,
            },
            max_output_tokens: config.limits.provider_max_output_tokens,
        },
    )))
}

/// The provider of the ingest worker, chosen by `RATATOSKR__INGEST__PROVIDER_MODE`.
#[derive(Debug)]
pub enum IngestProvider {
    /// Deterministic first-paragraph provider; answers the article family only.
    Fixture(ExtractiveArticleProvider),
    /// The controlled `OpenRouter` provider.
    OpenRouter(Box<ControlledOpenRouter>),
}

impl IngestProvider {
    /// Builds the provider the ingest section selects.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderRuntimeError`] when the selected provider is not configured.
    pub fn from_config(
        config: &Config,
        database: &Database,
        limiter: Arc<RateLimiter>,
    ) -> Result<Self, ProviderRuntimeError> {
        match config.ingest.provider_mode {
            IngestProviderMode::Fixture => Ok(Self::Fixture(ExtractiveArticleProvider)),
            IngestProviderMode::OpenRouter => {
                openrouter_provider(config, database, limiter).map(Self::OpenRouter)
            }
        }
    }
}

impl LlmProvider for IngestProvider {
    fn identity(&self) -> ProviderIdentity {
        match self {
            Self::Fixture(provider) => provider.identity(),
            Self::OpenRouter(provider) => provider.identity(),
        }
    }

    async fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> Result<ProviderResponse, ProviderFailure> {
        match self {
            Self::Fixture(provider) => provider.generate_json(request).await,
            Self::OpenRouter(provider) => provider.generate_json(request).await,
        }
    }
}
