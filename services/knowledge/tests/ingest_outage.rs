//! Readiness of the ingest worker follows its broker (XR-021 CONTRACTS.md S02 and S09).
//!
//! This test runs its own private broker process, so it does not share the disposable broker of the
//! other ingest tests.

mod ingest_support;

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ingest_support::{PLATFORM_ACK_WAIT, TestResult, config, eventually, provision_with_ack_wait};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{BlobStore, ScriptedProvider};
use ratatoskr_knowledge_service::{
    Lifecycle, Listener, Metrics, admin_router, spawn_ingest_workers_with_provider,
};
use tokio::sync::watch;
use tower::ServiceExt as _;

fn spawn_broker(port: u16, store: &std::path::Path) -> std::io::Result<Child> {
    let binary =
        std::env::var("KNOWLEDGE_TEST_NATS_SERVER").unwrap_or_else(|_| "nats-server".to_owned());
    Command::new(binary)
        .args([
            "-js",
            "-a",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "--store_dir",
        ])
        .arg(store)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

async fn ready_status(app: &axum::Router) -> Result<StatusCode, Box<dyn std::error::Error>> {
    let response = app
        .clone()
        .oneshot(Request::builder().uri("/ready").body(Body::empty())?)
        .await?;
    Ok(response.status())
}

async fn wait_for_broker(url: &str) -> TestResult {
    eventually("the private broker accepts connections", || async {
        Ok(async_nats::connect(url).await.is_ok())
    })
    .await
}

/// Readiness follows the broker: not ready while it is down, ready again once the consumers have
/// reconnected and verified their durables.
#[tokio::test]
async fn broker_outage_flips_readiness_false_and_recovers() -> TestResult {
    // The kernel picks the port; the broker restarts on the same one, so it is reserved only
    // for the instant it takes to start the first broker.
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let url = format!("nats://127.0.0.1:{port}");
    let store = TemporaryBlobRoot::create().await?;
    let mut broker = spawn_broker(port, store.path())?;
    wait_for_broker(&url).await?;
    let context = provision_with_ack_wait(&url, PLATFORM_ACK_WAIT).await?;
    drop(context);

    let database = TestDatabase::create().await?;
    let blobs_root = TemporaryBlobRoot::create().await?;
    let lifecycle = Lifecycle::starting_with_ingest();
    let (drain, receiver) = watch::channel(false);
    let handle = spawn_ingest_workers_with_provider(
        config(&url),
        database.database.clone(),
        BlobStore::new(blobs_root.path(), 1_048_576),
        lifecycle.clone(),
        receiver,
        Arc::new(ScriptedProvider::new([])),
    );
    let app = admin_router(lifecycle.clone(), Arc::new(Metrics::new()));
    lifecycle.mark_listener_bound(Listener::Operator);
    lifecycle.mark_listener_bound(Listener::Api);
    lifecycle.mark_ready();
    eventually("ingest is ready", || async {
        Ok(ready_status(&app).await? == StatusCode::OK)
    })
    .await?;

    broker.kill()?;
    broker.wait()?;
    eventually("ingest is not ready while the broker is down", || async {
        Ok(ready_status(&app).await? == StatusCode::SERVICE_UNAVAILABLE)
    })
    .await?;

    let mut restarted = spawn_broker(port, store.path())?;
    wait_for_broker(&url).await?;
    eventually("ingest is ready again", || async {
        Ok(ready_status(&app).await? == StatusCode::OK)
    })
    .await?;

    let _ignored = drain.send(true);
    tokio::time::timeout(Duration::from_secs(10), handle).await??;
    restarted.kill()?;
    restarted.wait()?;
    database.cleanup().await?;
    Ok(())
}
