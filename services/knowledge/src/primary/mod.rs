//! Supervised primary event-stream runtime.

mod bus;
mod worker;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use ratatoskr_knowledge::{
    BlobStore, BudgetLedger, BudgetLimits, Config, ControlledProvider, Database,
    GithubReadmeSettings, GithubRepositoryReadmeResolver, OpenRouterProvider, OpenRouterSettings,
    RateLimiter, RetryPolicy, RuntimeRole, SpendControls, TokenPrices,
};
use tokio::sync::watch;

use crate::{Lifecycle, Metrics};

type PrimaryProvider = ControlledProvider<OpenRouterProvider>;

/// Primary runtime construction or bounded shutdown failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PrimaryRuntimeError {
    /// Required provider or owner-service configuration was invalid.
    #[error("primary runtime configuration is invalid")]
    Configuration,
    /// A primary supervisor did not join inside the configured bound.
    #[error("primary runtime did not drain inside its shutdown bound")]
    Shutdown,
}

/// Owned primary supervisor handles.
#[derive(Debug)]
pub struct PrimaryRuntime {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl PrimaryRuntime {
    /// Starts no work for the explicit admin role, otherwise constructs every required
    /// dependency before spawning intake, leased workers, and outbox publication.
    ///
    /// # Errors
    ///
    /// Returns [`PrimaryRuntimeError::Configuration`] before readiness when any required
    /// provider or GitHub resolver dependency cannot be constructed.
    pub async fn start(
        config: &Config,
        database: &Database,
        blobs: &BlobStore,
        lifecycle: &Lifecycle,
        metrics: Arc<Metrics>,
        drain: watch::Receiver<bool>,
    ) -> Result<Self, PrimaryRuntimeError> {
        if config.runtime_role == RuntimeRole::Admin {
            return Ok(Self {
                handles: Vec::new(),
            });
        }
        let provider = Arc::new(build_provider(config, database)?);
        let token_path = config
            .primary
            .github_token_file
            .as_deref()
            .ok_or(PrimaryRuntimeError::Configuration)?;
        let token = GithubRepositoryReadmeResolver::token_from_file(token_path)
            .await
            .map_err(|_| PrimaryRuntimeError::Configuration)?;
        let resolver = Arc::new(
            GithubRepositoryReadmeResolver::new(GithubReadmeSettings {
                base_url: reqwest::Url::parse(&config.primary.github_base_url)
                    .map_err(|_| PrimaryRuntimeError::Configuration)?,
                service_token: token,
                timeout: Duration::from_millis(config.limits.provider_timeout_ms),
                response_bytes: config.primary.readme_response_bytes,
            })
            .map_err(|_| PrimaryRuntimeError::Configuration)?,
        );

        let mut handles = Vec::new();
        handles.push(bus::spawn_intake_supervisor(
            config.clone(),
            database.clone(),
            lifecycle.clone(),
            Arc::clone(&metrics),
            drain.clone(),
        ));
        handles.push(bus::spawn_outbox_supervisor(
            config.clone(),
            database.clone(),
            lifecycle.clone(),
            Arc::clone(&metrics),
            drain.clone(),
        ));
        let workers_failed = Arc::new(AtomicBool::new(false));
        handles.push(worker::spawn_dependency_supervisor(
            config.clone(),
            database.clone(),
            Arc::clone(&resolver),
            Arc::clone(&workers_failed),
            lifecycle.clone(),
            drain.clone(),
        ));
        for ordinal in 0..config.primary.worker_count {
            let handle = worker::spawn_worker(
                ordinal,
                config.clone(),
                database.clone(),
                blobs.clone(),
                Arc::clone(&provider),
                Arc::clone(&resolver),
                lifecycle.clone(),
                Arc::clone(&metrics),
                Arc::clone(&workers_failed),
                drain.clone(),
            );
            handles.push(handle);
        }
        Ok(Self { handles })
    }

    /// Joins all primary tasks inside one process-wide bound.
    ///
    /// # Errors
    ///
    /// Returns [`PrimaryRuntimeError::Shutdown`] on timeout or task panic.
    pub async fn join(self, timeout: Duration) -> Result<(), PrimaryRuntimeError> {
        self.join_until(tokio::time::Instant::now() + timeout).await
    }

    /// Joins all primary tasks before a process-wide shared deadline.
    ///
    /// # Errors
    ///
    /// Returns [`PrimaryRuntimeError::Shutdown`] on timeout or task panic.
    pub async fn join_until(
        self,
        deadline: tokio::time::Instant,
    ) -> Result<(), PrimaryRuntimeError> {
        let mut handles = self.handles;
        let mut failed = false;
        while let Some(mut handle) = handles.pop() {
            if let Ok(result) = tokio::time::timeout_at(deadline, &mut handle).await {
                failed |= result.is_err();
            } else {
                handle.abort();
                let _result = handle.await;
                for handle in &handles {
                    handle.abort();
                }
                for handle in handles {
                    let _result = handle.await;
                }
                return Err(PrimaryRuntimeError::Shutdown);
            }
        }
        if failed {
            Err(PrimaryRuntimeError::Shutdown)
        } else {
            Ok(())
        }
    }
}

fn build_provider(
    config: &Config,
    database: &Database,
) -> Result<PrimaryProvider, PrimaryRuntimeError> {
    let configured = config
        .provider
        .openrouter
        .as_ref()
        .ok_or(PrimaryRuntimeError::Configuration)?;
    let inner = OpenRouterProvider::new(OpenRouterSettings {
        base_url: configured.base_url.clone(),
        model: configured.model.clone(),
        credential: configured.api_key.clone(),
        max_output_tokens: config.limits.provider_max_output_tokens,
        response_byte_cap: config.limits.raw_response_bytes,
        call_deadline: Duration::from_millis(config.limits.provider_timeout_ms),
        connect_timeout: Duration::from_millis(config.limits.provider_timeout_ms)
            .min(Duration::from_secs(5)),
        retry: RetryPolicy::new(1, 200, 200),
    })
    .map_err(|_| PrimaryRuntimeError::Configuration)?;
    let spacing =
        Duration::try_from_secs_f64(60.0 / f64::from(config.limits.provider_requests_per_minute))
            .map_err(|_| PrimaryRuntimeError::Configuration)?;
    Ok(ControlledProvider::new(
        inner,
        Arc::new(RateLimiter::new(spacing)),
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
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn shutdown_timeout_aborts_and_awaits_every_remaining_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&dropped);
        let handle = tokio::spawn(async move {
            let _signal = DropSignal(signal);
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        let runtime = PrimaryRuntime {
            handles: vec![handle],
        };

        assert!(matches!(
            runtime.join(Duration::from_millis(10)).await,
            Err(PrimaryRuntimeError::Shutdown)
        ));
        assert!(dropped.load(Ordering::Acquire), "aborted task was detached");
    }
}
