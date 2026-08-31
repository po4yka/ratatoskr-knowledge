//! Ordered primary-runtime shutdown regression test.

use std::sync::Arc;
use std::time::Duration;

use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{BlobStore, Config};
use ratatoskr_knowledge_service::{Lifecycle, Metrics, PrimaryRuntime};
use tokio::sync::watch;

#[tokio::test]
async fn shutdown_stops_claims_settles_delivery_joins_workers_then_closes_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let token_file = root.path().join("github-service-token");
    tokio::fs::write(&token_file, "synthetic-service-token\n").await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        tokio::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o600)).await?;
    }
    let token_path = token_file.to_string_lossy().into_owned();
    let config = Config::from_environment([
        ("RATATOSKR__RUNTIME__ROLE", "primary"),
        (
            "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
            "synthetic-provider-token",
        ),
        (
            "RATATOSKR__PROVIDER__OPENROUTER__MODEL",
            "scripted/knowledge",
        ),
        (
            "RATATOSKR__PROVIDER__OPENROUTER__BASE_URL",
            "http://127.0.0.1:9/v1",
        ),
        ("RATATOSKR__PRIMARY__GITHUB_TOKEN_FILE", token_path.as_str()),
        ("RATATOSKR__PRIMARY__GITHUB_BASE_URL", "http://127.0.0.1:9/"),
        ("RATATOSKR__PRIMARY__BUS_ENDPOINT", "nats://127.0.0.1:9"),
        ("RATATOSKR__PRIMARY__WORKER_COUNT", "2"),
    ])?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let lifecycle = Lifecycle::starting_primary(false);
    let (drain_tx, drain_rx) = watch::channel(false);
    let runtime = PrimaryRuntime::start(
        &config,
        &database.database,
        &blobs,
        &lifecycle,
        Arc::new(Metrics::new()),
        drain_rx,
    )
    .await?;

    lifecycle.begin_drain();
    drain_tx.send(true)?;
    runtime.join(Duration::from_secs(3)).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from knowledge.analysis_work where lease_owner is not null",
        )
        .fetch_one(database.database.pool())
        .await?,
        0,
        "a joined runtime must not leave a live claim"
    );

    database.database.close().await;
    assert!(database.database.pool().acquire().await.is_err());
    database.cleanup().await?;
    Ok(())
}
