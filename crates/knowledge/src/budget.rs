//! Durable token and estimated-cost accounting for real provider calls.

use sqlx::PgPool;
use uuid::Uuid;

use crate::ProviderUsage;

/// Daily and monthly spend ceilings enforced before each provider call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetLimits {
    /// Maximum input plus output tokens per UTC day.
    pub daily_tokens: u64,
    /// Maximum input plus output tokens per UTC month.
    pub monthly_tokens: u64,
    /// Maximum estimated cost in micro-US dollars per UTC day.
    pub daily_cost_micro_usd: u64,
    /// Maximum estimated cost in micro-US dollars per UTC month.
    pub monthly_cost_micro_usd: u64,
}

/// Per-token price configuration in micro-US dollars per million tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenPrices {
    /// Input-token price in micro-US dollars per million tokens.
    pub input_micro_usd_per_mtoken: u64,
    /// Output-token price in micro-US dollars per million tokens.
    pub output_micro_usd_per_mtoken: u64,
}

/// Ledger accounting window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetWindow {
    /// The current UTC day.
    Daily,
    /// The current UTC month.
    Monthly,
}

/// Budget refusal or ledger failure carrying no request content.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BudgetError {
    /// The projected or recorded spend exceeds a configured ceiling.
    #[error("the configured provider spend ceiling is exhausted")]
    Exhausted,
    /// A ledger query failed.
    #[error("a budget ledger query failed")]
    Query(#[source] sqlx::Error),
    /// A token or cost value cannot fit its database representation.
    #[error("a budget value exceeds its database representation")]
    Overflow,
}

impl TokenPrices {
    /// Estimates one call's cost with ceiling rounding; `None` on overflow.
    #[must_use]
    pub fn estimate_cost_micro_usd(&self, input_tokens: u64, output_tokens: u64) -> Option<u64> {
        const MICROS_PER_MTOKEN: u128 = 1_000_000;
        let total = (u128::from(input_tokens))
            .saturating_mul(u128::from(self.input_micro_usd_per_mtoken))
            .saturating_add(
                u128::from(output_tokens)
                    .saturating_mul(u128::from(self.output_micro_usd_per_mtoken)),
            );
        u64::try_from(total.div_ceil(MICROS_PER_MTOKEN)).ok()
    }
}

/// Knowledge-owned durable usage ledger backing the spend ceilings.
#[derive(Debug, Clone)]
pub struct BudgetLedger {
    pool: PgPool,
}

impl BudgetLedger {
    /// Creates a ledger over the owned database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Records one response's actual usage and estimated cost.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] when persistence fails or values overflow.
    pub async fn record_usage(
        &self,
        provider: &str,
        model: &str,
        usage: ProviderUsage,
        estimated_cost_micro_usd: u64,
    ) -> Result<(), BudgetError> {
        let input_tokens = i64::try_from(usage.input_tokens).map_err(|_| BudgetError::Overflow)?;
        let output_tokens =
            i64::try_from(usage.output_tokens).map_err(|_| BudgetError::Overflow)?;
        let cost = i64::try_from(estimated_cost_micro_usd).map_err(|_| BudgetError::Overflow)?;
        sqlx::query(
            "insert into knowledge.provider_usage (
                usage_id, provider, model, input_tokens, output_tokens,
                estimated_cost_micro_usd
             ) values ($1, $2, $3, $4, $5, $6)",
        )
        .bind(Uuid::now_v7())
        .bind(provider)
        .bind(model)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(cost)
        .execute(&self.pool)
        .await
        .map_err(BudgetError::Query)?;
        Ok(())
    }

    /// Returns `(tokens, cost)` recorded for one provider inside the window.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] when the query fails or totals overflow.
    pub async fn window_totals(
        &self,
        provider: &str,
        window: BudgetWindow,
    ) -> Result<(u64, u64), BudgetError> {
        let (tokens, cost): (i64, i64) = match window {
            BudgetWindow::Daily => {
                sqlx::query_as(
                    "select coalesce(sum(input_tokens + output_tokens), 0)::bigint,
                            coalesce(sum(estimated_cost_micro_usd), 0)::bigint
                     from knowledge.provider_usage
                     where provider = $1 and recorded_at >= date_trunc('day', now())",
                )
                .bind(provider)
                .fetch_one(&self.pool)
                .await
            }
            BudgetWindow::Monthly => {
                sqlx::query_as(
                    "select coalesce(sum(input_tokens + output_tokens), 0)::bigint,
                            coalesce(sum(estimated_cost_micro_usd), 0)::bigint
                     from knowledge.provider_usage
                     where provider = $1 and recorded_at >= date_trunc('month', now())",
                )
                .bind(provider)
                .fetch_one(&self.pool)
                .await
            }
        }
        .map_err(BudgetError::Query)?;
        Ok((
            u64::try_from(tokens).map_err(|_| BudgetError::Overflow)?,
            u64::try_from(cost).map_err(|_| BudgetError::Overflow)?,
        ))
    }

    /// Refuses the call when its conservative projection would exceed either
    /// the token or cost ceiling in any window.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::Exhausted`] when the projection would overrun a
    /// ceiling, and [`BudgetError`] for ledger failures.
    pub async fn ensure_within_budget(
        &self,
        provider: &str,
        projected_input_tokens: u64,
        projected_output_tokens: u64,
        prices: &TokenPrices,
        limits: &BudgetLimits,
    ) -> Result<(), BudgetError> {
        let projected_cost = prices
            .estimate_cost_micro_usd(projected_input_tokens, projected_output_tokens)
            .ok_or(BudgetError::Overflow)?;
        let projected_tokens = projected_input_tokens.saturating_add(projected_output_tokens);
        let (day_tokens, day_cost) = self.window_totals(provider, BudgetWindow::Daily).await?;
        let (month_tokens, month_cost) =
            self.window_totals(provider, BudgetWindow::Monthly).await?;
        if month_tokens.saturating_add(projected_tokens) > limits.monthly_tokens
            || day_tokens.saturating_add(projected_tokens) > limits.daily_tokens
            || month_cost.saturating_add(projected_cost) > limits.monthly_cost_micro_usd
            || day_cost.saturating_add(projected_cost) > limits.daily_cost_micro_usd
        {
            return Err(BudgetError::Exhausted);
        }
        Ok(())
    }
}
