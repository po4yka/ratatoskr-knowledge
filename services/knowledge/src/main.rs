#![forbid(unsafe_code)]

//! Ratatoskr Knowledge service process.

use std::future::IntoFuture as _;
use std::time::Duration;

use ratatoskr_knowledge::{BlobStore, Config, Database, init_telemetry};
use ratatoskr_knowledge_service::{Lifecycle, admin_router};
use tokio::sync::oneshot;

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
    let listener = tokio::net::TcpListener::bind(config.admin.listen_address).await?;
    lifecycle.mark_ready();
    serve_admin(
        listener,
        lifecycle,
        Duration::from_millis(config.limits.shutdown_timeout_ms),
    )
    .await?;
    database.close().await;
    Ok(())
}

async fn serve_admin(
    listener: tokio::net::TcpListener,
    lifecycle: Lifecycle,
    shutdown_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = axum::serve(listener, admin_router(lifecycle.clone()))
        .with_graceful_shutdown(async move {
            let _ignored = shutdown_rx.await;
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        result = shutdown_signal() => {
            result?;
            lifecycle.begin_drain();
            let _ignored = shutdown_tx.send(());
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
