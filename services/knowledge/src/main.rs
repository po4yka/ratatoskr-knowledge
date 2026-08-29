#![forbid(unsafe_code)]

//! Ratatoskr Knowledge service process.

use std::future::IntoFuture as _;
use std::sync::Arc;
use std::time::Duration;

use ratatoskr_knowledge::{
    BlobStore, BudgetLedger, BudgetLimits, ChunkPolicy, Config, ControlledEmbeddings, Database,
    DeletionReceipt, EmbeddingsSettings, HybridRetriever, Indexer, IndexerLimits,
    OpenAiCompatibleEmbeddings, RateLimiter, ResultReaderSecret, RetryPolicy, TokenPrices,
    init_telemetry,
};
use ratatoskr_knowledge_service::{
    HybridSearchRetriever, Lifecycle, Metrics, admin_router, spawn_channel_recap_worker,
};
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    match std::env::args().nth(1).as_deref() {
        Some("check-config") => return Ok(()),
        Some("delete-source") => return run_delete_source_job(config).await,
        Some("delete-tenant") => return run_delete_tenant_job(config).await,
        Some("reindex-embeddings") => return run_reindex_embeddings_job(config).await,
        Some("reindex-search-documents") => return run_reindex_search_documents_job(config).await,
        _ => {}
    }
    init_telemetry()?;
    tokio::fs::create_dir_all(&config.storage.blob_root).await?;
    let blobs = BlobStore::new(
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

    let lifecycle = if config.channel_recap.enabled {
        Lifecycle::starting_with_channel_recap()
    } else {
        Lifecycle::starting()
    };
    let metrics = Arc::new(Metrics::new());
    let (drain_tx, drain_rx) = watch::channel(false);
    let recap_worker = config.channel_recap.enabled.then(|| {
        spawn_channel_recap_worker(
            config.clone(),
            database.clone(),
            blobs.clone(),
            lifecycle.clone(),
            drain_rx.clone(),
        )
    });

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
    let serve_result = serve_admin(
        listener,
        AdminServer {
            lifecycle,
            database: database.clone(),
            metrics,
            retriever,
            result_reader_secret: config.channel_recap.result_reader_service_secret.clone(),
        },
        drain_tx.clone(),
        drain_rx,
        Duration::from_millis(config.limits.shutdown_timeout_ms),
    )
    .await;
    let _ignored = drain_tx.send(true);
    if let Some(worker) = recap_worker {
        tokio::time::timeout(
            Duration::from_millis(config.limits.shutdown_timeout_ms),
            worker,
        )
        .await??;
    }
    serve_result?;
    database.close().await;
    Ok(())
}

/// One-shot `delete-source` job: strict config, scoped deletion, receipt.
///
/// Exits zero on full success; any failure surfaces as a nonzero exit
/// with completed work already persisted by the deletion transaction.
async fn run_delete_source_job(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args().skip(2).collect::<Vec<_>>();
    let [tenant_ref, owner_context, source_document_id] = arguments.as_slice() else {
        return Err("usage: delete-source <tenant> <owner_context> <source_document_id>".into());
    };
    let (database, blobs) = connect_job_stack(&config).await?;
    let receipt = ratatoskr_knowledge::delete_source(
        &database,
        &blobs,
        tenant_ref,
        owner_context,
        source_document_id,
    )
    .await?;
    print_deletion_receipt(&receipt);
    database.close().await;
    Ok(())
}

/// One-shot `delete-tenant` job: strict config, tenant deletion, receipt.
async fn run_delete_tenant_job(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args().skip(2).collect::<Vec<_>>();
    let [tenant_ref] = arguments.as_slice() else {
        return Err("usage: delete-tenant <tenant>".into());
    };
    let (database, blobs) = connect_job_stack(&config).await?;
    let receipt = ratatoskr_knowledge::delete_tenant(&database, &blobs, tenant_ref).await?;
    print_deletion_receipt(&receipt);
    database.close().await;
    Ok(())
}

/// One-shot `reindex-embeddings` job: strict config, planned execution,
/// printed totals, and honest exit codes. Zero on full success; nonzero
/// when any source failed, with completed per-source work persisted.
async fn run_reindex_embeddings_job(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args().skip(2).collect::<Vec<_>>();
    let scope = parse_reindex_scope("reindex-embeddings", &arguments)?;
    if config.provider.embeddings.is_none() {
        return Err("reindex-embeddings requires embeddings configuration".into());
    }
    let (database, _blobs) = connect_job_stack(&config).await?;
    let stack = build_embeddings_stack(&config, &database)?
        .ok_or("reindex-embeddings requires embeddings configuration")?;
    let policy = ChunkPolicy::new(
        config.limits.chunk_target_characters,
        config.limits.chunk_overlap_characters,
    )?;
    let summary = ratatoskr_knowledge::execute_reindex(
        &database,
        &stack.controlled_embeddings()?,
        policy,
        config.limits.embeddings_max_input_characters,
        &scope,
        print_reindex_progress,
    )
    .await?;
    print_reindex_totals(summary.sources_processed, summary.failures);
    database.close().await;
    if summary.failures > 0 {
        return Err(format!(
            "reindex-embeddings finished with {} failed source(s)",
            summary.failures
        )
        .into());
    }
    Ok(())
}

/// One-shot search rebuild: no source acquisition, only durable calculated
/// inputs created by the persist transaction.
async fn run_reindex_search_documents_job(
    config: Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args().skip(2).collect::<Vec<_>>();
    let scope = parse_reindex_scope("reindex-search-documents", &arguments)?;
    let (database, _blobs) = connect_job_stack(&config).await?;
    let summary = ratatoskr_knowledge::rebuild_search_documents(
        &database,
        &scope,
        print_search_reindex_progress,
    )
    .await?;
    print_search_reindex_totals(summary.sources_processed, summary.failures);
    database.close().await;
    if summary.failures > 0 {
        return Err(format!(
            "reindex-search-documents finished with {} failed source(s)",
            summary.failures
        )
        .into());
    }
    Ok(())
}

/// Parses the shared, deliberately small reindex job scope grammar.
///
/// `--source-doc` is intentionally paired with `--tenant`: a logical
/// document identifier is only unique within its tenant and owner context.
fn parse_reindex_scope(
    command: &str,
    arguments: &[String],
) -> Result<ratatoskr_knowledge::ReindexScope, Box<dyn std::error::Error>> {
    match arguments {
        [] => Ok(ratatoskr_knowledge::ReindexScope::unrestricted()),
        [flag, tenant_ref] if flag == "--tenant" => {
            Ok(ratatoskr_knowledge::ReindexScope::for_tenant(tenant_ref))
        }
        [tenant_flag, tenant_ref, source_flag, source]
            if tenant_flag == "--tenant" && source_flag == "--source-doc" =>
        {
            let (owner_context, source_document_id) = source
                .split_once(':')
                .ok_or("--source-doc must be <owner_context>:<document_id>")?;
            if owner_context.is_empty() || source_document_id.is_empty() {
                return Err("--source-doc must be <owner_context>:<document_id>".into());
            }
            Ok(ratatoskr_knowledge::ReindexScope::for_source(
                tenant_ref,
                owner_context,
                source_document_id,
            ))
        }
        _ => Err(format!(
            "usage: {command} [--tenant <ref> [--source-doc <owner_context>:<document_id>]]"
        )
        .into()),
    }
}

/// Prints one committed search-projection rebuild result.
fn print_search_reindex_progress(source_ref_id: impl std::fmt::Display) {
    use std::io::Write as _;
    let _ignored = writeln!(std::io::stdout(), "source {source_ref_id} rebuilt");
}

/// Prints one search-rebuild job's final summary.
fn print_search_reindex_totals(processed: usize, failed: usize) {
    use std::io::Write as _;
    let _ignored = writeln!(
        std::io::stdout(),
        "reindex-search-documents processed={processed} failed={failed}"
    );
}

/// Prints one committed embeddings reindex result in deterministic plan order.
fn print_reindex_progress(
    source_ref_id: impl std::fmt::Display,
    outcome: ratatoskr_knowledge::ReindexSourceOutcome,
) {
    use std::io::Write as _;
    let text = match outcome {
        ratatoskr_knowledge::ReindexSourceOutcome::Rebuilt { chunks } => {
            format!("source {source_ref_id} chunks={chunks}")
        }
        ratatoskr_knowledge::ReindexSourceOutcome::Failed => {
            format!("source {source_ref_id} failed")
        }
    };
    let _ignored = writeln!(std::io::stdout(), "{text}");
}

/// Prints the one-line job totals as plain stdout.
fn print_reindex_totals(processed: usize, failed: usize) {
    use std::io::Write as _;
    let _ignored = writeln!(
        std::io::stdout(),
        "reindex-embeddings processed={processed} failed={failed}"
    );
}

/// Connects the job's database and owned blob root without starting the
/// admin listener, the indexing worker, or telemetry.
async fn connect_job_stack(
    config: &Config,
) -> Result<(Database, BlobStore), Box<dyn std::error::Error>> {
    tokio::fs::create_dir_all(&config.storage.blob_root).await?;
    let database = Database::connect(
        &config.storage.database_url,
        config.limits.database_connections,
        Duration::from_millis(config.limits.database_acquire_timeout_ms),
    )
    .await?;
    database.apply_schema().await?;
    let blobs = BlobStore::new(
        &config.storage.blob_root,
        config
            .limits
            .blob_bytes
            .min(u64::try_from(config.limits.raw_response_bytes)?),
    );
    Ok((database, blobs))
}

/// Prints the machine-readable receipt summary as plain stdout lines.
fn print_deletion_receipt(receipt: &DeletionReceipt) {
    use std::io::Write as _;
    let mut stdout = std::io::stdout();
    let counts = &receipt.counts;
    let _ignored = writeln!(
        stdout,
        "deleted scope={} tenant={}",
        receipt.scope.as_str(),
        receipt.scope.tenant_ref()
    );
    let _ignored = writeln!(
        stdout,
        "deleted source_refs={} analysis_runs={} analysis_attempts={} \
         analysis_outputs={} search_projection_inputs={} search_documents={} embedding_chunks={} \
         embedding_failures={}",
        counts.source_refs,
        counts.analysis_runs,
        counts.analysis_attempts,
        counts.analysis_outputs,
        counts.search_projection_inputs,
        counts.search_documents,
        counts.embedding_chunks,
        counts.embedding_failures
    );
    let _ignored = writeln!(
        stdout,
        "removed_blobs={} orphan_blobs={} external_source_blobs={}",
        receipt.blob_digests_removed.len(),
        receipt.orphan_digests_removed.len(),
        receipt.external_source_blob_digests.len()
    );
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
    result_reader_secret: Option<ResultReaderSecret>,
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
        result_reader_secret,
    } = server;
    let server = axum::serve(
        listener,
        admin_router(
            lifecycle.clone(),
            database,
            metrics,
            retriever,
            result_reader_secret,
        ),
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
