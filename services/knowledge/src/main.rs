#![forbid(unsafe_code)]

//! Ratatoskr Knowledge service process.

use std::future::IntoFuture as _;
use std::sync::Arc;
use std::time::Duration;

use ratatoskr_knowledge::{
    BlobStore, BudgetLedger, BudgetLimits, ChunkPolicy, Config, ControlledEmbeddings, Database,
    EmbeddingsSettings, HybridRetriever, Indexer, IndexerLimits, OpenAiCompatibleEmbeddings,
    RateLimiter, RetryPolicy, TokenPrices, init_telemetry,
};
use ratatoskr_knowledge_service::{HybridSearchRetriever, Lifecycle, Metrics, admin_router};
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    if std::env::args().nth(1).as_deref() == Some("check-config") {
        return Ok(());
    }
    init_telemetry()?;
    tokio::fs::create_dir_all(&config.storage.blob_root).await?;
    let _blobs = BlobStore::new(
        &config.storage.blob_root,
        config
            .limits
            .blob_bytes
            .min(u64::try_from(config.limits.raw_response_bytes)?),
    );
    let database = Database::connect(
        &config.storage.database_url,
        config.limits.database_connections,
        Duration::from_millis(config.limits.database_acquire_timeout_ms),
    )
    .await?;
    database.apply_schema().await?;

    let lifecycle = Lifecycle::starting();
    let metrics = Arc::new(Metrics::new());
    let (drain_tx, drain_rx) = watch::channel(false);

    // One shared control stack backs both background indexing and hybrid
    // search; without an embeddings credential both stay offline.
    let stack = build_embeddings_stack(&config, &database)?;
    let retriever: Option<Arc<HybridSearchRetriever>> = match &stack {
        Some(stack) => Some(Arc::new(HybridRetriever::new(
            stack.controlled_embeddings()?,
        ))),
        None => None,
    };
    spawn_indexing_worker(
        build_indexing_worker(&config, &database, stack.as_ref())?,
        Duration::from_millis(config.limits.embeddings_poll_interval_ms),
        drain_rx.clone(),
        Arc::clone(&metrics),
    );

    let listener = tokio::net::TcpListener::bind(config.admin.listen_address).await?;
    lifecycle.mark_ready();
    serve_admin(
        listener,
        AdminServer {
            lifecycle,
            database: database.clone(),
            metrics,
            retriever,
        },
        drain_tx,
        drain_rx,
        Duration::from_millis(config.limits.shutdown_timeout_ms),
    )
    .await?;
    database.close().await;
    Ok(())
}

/// Shared embeddings controls backing every production call path.
struct EmbeddingStack {
    settings: EmbeddingsSettings,
    limiter: Arc<RateLimiter>,
    ledger: BudgetLedger,
    limits: BudgetLimits,
    prices: TokenPrices,
}

impl EmbeddingStack {
    /// Builds one controlled adapter over the shared limiter and ledger.
    ///
    /// # Errors
    ///
    /// Returns [`ratatoskr_knowledge::EmbeddingsWireError`] when the
    /// validated settings cannot build a transport.
    fn controlled_embeddings(
        &self,
    ) -> Result<
        ControlledEmbeddings<OpenAiCompatibleEmbeddings>,
        ratatoskr_knowledge::EmbeddingsWireError,
    > {
        let adapter = OpenAiCompatibleEmbeddings::new(self.settings.clone())?;
        Ok(ControlledEmbeddings::new(
            adapter,
            Arc::clone(&self.limiter),
            self.ledger.clone(),
            self.limits,
            self.prices,
        ))
    }
}

/// Builds the shared embeddings controls; `None` keeps the process offline.
fn build_embeddings_stack(
    config: &Config,
    database: &Database,
) -> Result<Option<EmbeddingStack>, Box<dyn std::error::Error>> {
    let Some(embeddings) = &config.provider.embeddings else {
        return Ok(None);
    };
    let dimensions = u16::try_from(embeddings.dimensions)
        .map_err(|_| Box::<dyn std::error::Error>::from("embeddings dimensions must fit u16"))?;
    let spacing =
        Duration::try_from_secs_f64(60.0 / f64::from(config.limits.embeddings_requests_per_minute))
            .map_err(|_| {
                Box::<dyn std::error::Error>::from("embeddings rate must divide a minute")
            })?;
    let settings = EmbeddingsSettings {
        base_url: embeddings.base_url.clone(),
        model: embeddings.model.clone(),
        credential: embeddings.api_key.clone(),
        dimensions,
        prompt_version: embeddings.prompt_version.clone(),
        max_input_characters: config.limits.embeddings_max_input_characters,
        response_byte_cap: config.limits.raw_response_bytes,
        call_deadline: Duration::from_millis(config.limits.embeddings_timeout_ms),
        connect_timeout: Duration::from_millis(config.limits.embeddings_timeout_ms)
            .min(Duration::from_secs(5)),
        retry: RetryPolicy::new(3, 200, 2_000),
    };
    Ok(Some(EmbeddingStack {
        settings,
        limiter: Arc::new(RateLimiter::new(spacing)),
        ledger: BudgetLedger::new(database.pool().clone()),
        limits: BudgetLimits {
            daily_tokens: config.limits.embeddings_daily_token_budget,
            monthly_tokens: config.limits.embeddings_monthly_token_budget,
            daily_cost_micro_usd: config.limits.embeddings_daily_cost_micro_usd,
            monthly_cost_micro_usd: config.limits.embeddings_monthly_cost_micro_usd,
        },
        prices: TokenPrices {
            input_micro_usd_per_mtoken: embeddings.input_micro_usd_per_mtoken,
            output_micro_usd_per_mtoken: 0,
        },
    }))
}

/// Background indexing composition; absent credentials disable it entirely.
enum IndexingWorker {
    Disabled,
    Enabled(Box<Indexer<ControlledEmbeddings<OpenAiCompatibleEmbeddings>>>),
}

fn build_indexing_worker(
    config: &Config,
    database: &Database,
    stack: Option<&EmbeddingStack>,
) -> Result<IndexingWorker, Box<dyn std::error::Error>> {
    let Some(stack) = stack else {
        return Ok(IndexingWorker::Disabled);
    };
    let policy = ChunkPolicy::new(
        config.limits.chunk_target_characters,
        config.limits.chunk_overlap_characters,
    )?;
    Ok(IndexingWorker::Enabled(Box::new(Indexer::new(
        database,
        stack.controlled_embeddings()?,
        policy,
        IndexerLimits {
            batch_sources: usize::try_from(config.limits.embeddings_batch_sources)
                .unwrap_or(usize::MAX),
            max_input_characters: config.limits.embeddings_max_input_characters,
            max_failure_attempts: i32::try_from(config.limits.embeddings_max_failure_attempts)
                .unwrap_or(i32::MAX),
        },
    ))))
}
/// Spawns the single drain-aware indexing poll task.
///
/// The first pass runs immediately at startup; quiet passes then repeat on
/// the poll interval. The task observes the same drain signal as the HTTP
/// server, so shutdown stays inside the configured bound.
fn spawn_indexing_worker(
    worker: IndexingWorker,
    poll_interval: Duration,
    mut drain: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            if *drain.borrow_and_update() {
                break;
            }
            run_indexing_pass(&worker, &metrics).await;
            tokio::select! {
                _ = interval.tick() => {}
                _ = drain.changed() => break,
            }
        }
    });
}

/// Runs passes until one makes no progress.
///
/// Sources that can never progress (no projection, exhausted failure bound)
/// report zero work and wait for the next interval instead of spinning.
async fn run_indexing_pass(worker: &IndexingWorker, metrics: &Metrics) {
    let IndexingWorker::Enabled(indexer) = worker else {
        return;
    };
    loop {
        match indexer.process_pending().await {
            Ok(outcome) => {
                metrics.record_index_pass();
                metrics.record_indexed(outcome.indexed);
                metrics.record_index_failures(outcome.failed);
                if outcome.indexed == 0 && outcome.failed == 0 {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

/// Operator-plane dependencies served by the admin listener.
struct AdminServer {
    lifecycle: Lifecycle,
    database: Database,
    metrics: Arc<Metrics>,
    retriever: Option<Arc<HybridSearchRetriever>>,
}

async fn serve_admin(
    listener: tokio::net::TcpListener,
    server: AdminServer,
    drain_tx: watch::Sender<bool>,
    mut drain_rx: watch::Receiver<bool>,
    shutdown_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let AdminServer {
        lifecycle,
        database,
        metrics,
        retriever,
    } = server;
    let server = axum::serve(
        listener,
        admin_router(lifecycle.clone(), database, metrics, retriever),
    )
    .with_graceful_shutdown(async move {
        let _ignored = drain_rx.changed().await;
    })
    .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        result = shutdown_signal() => {
            result?;
            lifecycle.begin_drain();
            let _ignored = drain_tx.send(true);
            tokio::time::timeout(shutdown_timeout, &mut server).await??;
        }
    }
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<(), std::io::Error> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}
