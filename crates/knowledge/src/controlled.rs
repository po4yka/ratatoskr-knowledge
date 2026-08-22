//! Composition wrapper ordering rate, budget, execution, and usage recording.

use std::sync::Arc;

use crate::{
    BudgetError, BudgetLedger, BudgetLimits, GenerationRequest, LlmProvider, ProviderError,
    ProviderFailure, ProviderFailureClass, ProviderIdentity, RateLimiter, TokenPrices,
};

/// Conservative projection inputs applied before every inner call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpendControls {
    /// Daily and monthly token and cost ceilings.
    pub limits: BudgetLimits,
    /// Per-token price configuration for cost estimation.
    pub prices: TokenPrices,
    /// Configured output bound used as the projected output tokens.
    pub max_output_tokens: u32,
}

/// Wraps any provider with the shared rate limiter and durable budget ledger.
///
/// Admission is spaced by the limiter; spend is refused before the call using
/// a conservative projection; actual usage is recorded after each response.
/// A failed usage record never discards a valid response; it is logged with
/// bounded fields only.
#[derive(Debug)]
pub struct ControlledProvider<P> {
    inner: P,
    limiter: Arc<RateLimiter>,
    ledger: BudgetLedger,
    controls: SpendControls,
}

/// Supplied-context characters per estimated input token.
const CHARACTERS_PER_TOKEN: usize = 4;

impl<P> ControlledProvider<P> {
    /// Composes the controls around one inner provider.
    #[must_use]
    pub fn new(
        inner: P,
        limiter: Arc<RateLimiter>,
        ledger: BudgetLedger,
        controls: SpendControls,
    ) -> Self {
        Self {
            inner,
            limiter,
            ledger,
            controls,
        }
    }
}

fn projected_input_tokens(request: &GenerationRequest) -> u64 {
    let characters = request
        .system_policy
        .chars()
        .count()
        .saturating_add(request.task_instruction.chars().count())
        .saturating_add(request.source_content.chars().count());
    u64::try_from(characters.div_ceil(CHARACTERS_PER_TOKEN)).unwrap_or(u64::MAX)
}

impl<P: LlmProvider> LlmProvider for ControlledProvider<P> {
    fn identity(&self) -> ProviderIdentity {
        self.inner.identity()
    }

    async fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> Result<crate::ProviderResponse, ProviderFailure> {
        self.limiter.admit().await;
        let identity = self.inner.identity();
        let projection = projected_input_tokens(&request);
        let output_bound = u64::from(self.controls.max_output_tokens);
        if let Err(error) = self
            .ledger
            .ensure_within_budget(
                &identity.provider,
                projection,
                output_bound,
                &self.controls.prices,
                &self.controls.limits,
            )
            .await
        {
            return Err(budget_failure(&error));
        }
        let outcome = self.inner.generate_json(request).await;
        if let Ok(response) = &outcome
            && let Some(cost) = self
                .controls
                .prices
                .estimate_cost_micro_usd(response.usage.input_tokens, response.usage.output_tokens)
            && let Err(BudgetError::Query(_)) = self
                .ledger
                .record_usage(&identity.provider, &identity.model, response.usage, cost)
                .await
        {
            tracing::warn!(
                operation = "provider_call",
                outcome = "usage_record_failed",
                provider = %identity.provider,
                model = %identity.model
            );
        }
        outcome
    }
}

fn budget_failure(error: &BudgetError) -> ProviderFailure {
    match error {
        BudgetError::Exhausted => ProviderFailure {
            error: ProviderError::BudgetExhausted,
            class: ProviderFailureClass::BudgetExhausted,
            http_status: None,
        },
        // The ledger shares the analysis database; treat unavailability as a
        // retryable transport condition rather than losing the run.
        BudgetError::Query(_) => ProviderFailure {
            error: ProviderError::Transient,
            class: ProviderFailureClass::Unclassified,
            http_status: None,
        },
        BudgetError::Overflow => ProviderFailure {
            error: ProviderError::Permanent,
            class: ProviderFailureClass::Unclassified,
            http_status: None,
        },
    }
}
