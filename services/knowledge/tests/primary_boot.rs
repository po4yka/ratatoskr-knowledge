//! Primary-role readiness regression tests.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{BlobStore, Config, PRIMARY_EVENT_SUBJECTS};
use ratatoskr_knowledge_service::{Lifecycle, Metrics, PrimaryRuntime, admin_router};
use tokio::sync::watch;
use tower::ServiceExt as _;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end regression keeps absent and exact fleet-owned durable phases together"
)]
async fn primary_role_requires_exact_durable_and_live_supervisors()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let lifecycle = Lifecycle::starting_primary(false);
    let router = admin_router(
        lifecycle.clone(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
        None,
    );
    lifecycle.mark_ready();
    assert_eq!(ready(&router).await?, StatusCode::SERVICE_UNAVAILABLE);

    lifecycle.set_primary_workers_ready(true);
    lifecycle.set_primary_outbox_ready(true);
    assert_eq!(ready(&router).await?, StatusCode::SERVICE_UNAVAILABLE);
    lifecycle.set_primary_bus_ready(true);
    assert_eq!(ready(&router).await?, StatusCode::OK);

    lifecycle.set_primary_bus_ready(false);
    assert_eq!(ready(&router).await?, StatusCode::SERVICE_UNAVAILABLE);
    lifecycle.set_primary_bus_ready(true);
    assert_eq!(
        ready(&router).await?,
        StatusCode::OK,
        "reconnect did not restore readiness"
    );

    lifecycle.set_primary_workers_ready(false);
    assert_eq!(ready(&router).await?, StatusCode::SERVICE_UNAVAILABLE);
    lifecycle.set_primary_workers_ready(true);
    lifecycle.set_primary_outbox_ready(false);
    assert_eq!(ready(&router).await?, StatusCode::SERVICE_UNAVAILABLE);

    let nats_url = std::env::var("KNOWLEDGE_TEST_NATS_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:14223".to_owned());
    let stream = primary_stream(&nats_url).await?;
    let _ignored = stream.delete_consumer("ratatoskr_knowledge_main").await;
    let root = TemporaryBlobRoot::create().await?;
    let token_file = root.path().join("github-token");
    tokio::fs::write(&token_file, "synthetic-service-token\n").await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        tokio::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o600)).await?;
    }
    let github = probe_listener(ProbeKind::Github).await?;
    let provider = probe_listener(ProbeKind::Provider).await?;
    let config = primary_config(&nats_url, &token_file, github.0, provider.0)?;
    let blobs = BlobStore::new(root.path(), 4_096);

    let absent_lifecycle = Lifecycle::starting_primary(false);
    absent_lifecycle.mark_ready();
    let (absent_tx, absent_rx) = watch::channel(false);
    let absent_runtime = PrimaryRuntime::start(
        &config,
        &database.database,
        &blobs,
        &absent_lifecycle,
        Arc::new(Metrics::new()),
        absent_rx,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        stream
            .get_consumer::<async_nats::jetstream::consumer::pull::Config>(
                "ratatoskr_knowledge_main"
            )
            .await
            .is_err(),
        "Knowledge created the fleet-owned durable"
    );
    absent_tx.send(true)?;
    absent_runtime.join(Duration::from_secs(3)).await?;

    stream
        .get_or_create_consumer(
            "ratatoskr_knowledge_main",
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("ratatoskr_knowledge_main".to_owned()),
                filter_subjects: PRIMARY_EVENT_SUBJECTS
                    .iter()
                    .map(|subject| (*subject).to_owned())
                    .collect(),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                replay_policy: async_nats::jetstream::consumer::ReplayPolicy::Instant,
                ..async_nats::jetstream::consumer::pull::Config::default()
            },
        )
        .await?;
    let live_lifecycle = Lifecycle::starting_primary(false);
    live_lifecycle.mark_ready();
    let live_router = admin_router(
        live_lifecycle.clone(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
        None,
    );
    let (live_tx, live_rx) = watch::channel(false);
    let live_runtime = PrimaryRuntime::start(
        &config,
        &database.database,
        &blobs,
        &live_lifecycle,
        Arc::new(Metrics::new()),
        live_rx,
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while ready(&live_router).await? != StatusCode::OK {
        if tokio::time::Instant::now() >= deadline {
            return Err("exact durable never made primary ready".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    live_lifecycle.set_primary_workers_ready(false);
    assert_eq!(ready(&live_router).await?, StatusCode::SERVICE_UNAVAILABLE);
    live_tx.send(true)?;
    live_runtime.join(Duration::from_secs(3)).await?;

    let wrong_token_file = root.path().join("wrong-github-token");
    tokio::fs::write(&wrong_token_file, "wrong-service-token\n").await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        tokio::fs::set_permissions(&wrong_token_file, std::fs::Permissions::from_mode(0o600))
            .await?;
    }
    let unauthorized = primary_config(&nats_url, &wrong_token_file, github.0, provider.0)?;
    let unauthorized_lifecycle = Lifecycle::starting_primary(false);
    unauthorized_lifecycle.mark_ready();
    let unauthorized_router = admin_router(
        unauthorized_lifecycle.clone(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
        None,
    );
    let (unauthorized_tx, unauthorized_rx) = watch::channel(false);
    let unauthorized_runtime = PrimaryRuntime::start(
        &unauthorized,
        &database.database,
        &blobs,
        &unauthorized_lifecycle,
        Arc::new(Metrics::new()),
        unauthorized_rx,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        ready(&unauthorized_router).await?,
        StatusCode::SERVICE_UNAVAILABLE,
        "TCP reachability hid an invalid owner-service credential"
    );
    unauthorized_tx.send(true)?;
    unauthorized_runtime.join(Duration::from_secs(3)).await?;

    database.cleanup().await?;
    Ok(())
}

async fn primary_stream(
    nats_url: &str,
) -> Result<async_nats::jetstream::stream::Stream, Box<dyn std::error::Error>> {
    let client = async_nats::connect(nats_url).await?;
    let context = async_nats::jetstream::new(client);
    Ok(context
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "ratatoskr_events".to_owned(),
            subjects: vec!["evt.>".to_owned()],
            max_messages: 1_000,
            max_bytes: 16_777_216,
            ..async_nats::jetstream::stream::Config::default()
        })
        .await?)
}

fn primary_config(
    nats_url: &str,
    token_file: &std::path::Path,
    github: std::net::SocketAddr,
    provider: std::net::SocketAddr,
) -> Result<Config, Box<dyn std::error::Error>> {
    let token = token_file.to_string_lossy().into_owned();
    let github = format!("http://{github}/");
    let provider = format!("http://{provider}/v1");
    Ok(Config::from_environment([
        ("RATATOSKR__RUNTIME__ROLE", "primary"),
        ("RATATOSKR__PRIMARY__BUS_ENDPOINT", nats_url),
        ("RATATOSKR__PRIMARY__GITHUB_TOKEN_FILE", token.as_str()),
        ("RATATOSKR__PRIMARY__GITHUB_BASE_URL", github.as_str()),
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
            provider.as_str(),
        ),
        ("RATATOSKR__PRIMARY__WORKER_COUNT", "1"),
    ])?)
}

#[derive(Clone, Copy)]
enum ProbeKind {
    Github,
    Provider,
}

async fn probe_listener(
    kind: ProbeKind,
) -> Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>), std::io::Error> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut request = [0_u8; 8_192];
            let Ok(length) = stream.read(&mut request).await else {
                continue;
            };
            let Some(request) = request.get(..length) else {
                continue;
            };
            let request = String::from_utf8_lossy(request);
            let authorized = match kind {
                ProbeKind::Github => {
                    request.starts_with("POST /internal/v1/repository-readmes/resolve ")
                        && request.contains("authorization: Bearer synthetic-service-token")
                }
                ProbeKind::Provider => {
                    request.starts_with("GET /v1/key ")
                        && request.contains("authorization: Bearer synthetic-provider-token")
                }
            };
            let response = match (kind, authorized) {
                (ProbeKind::Github, true) => {
                    "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                }
                (ProbeKind::Provider, true) => {
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                }
                _ => "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            };
            let _result = stream.write_all(response.as_bytes()).await;
        }
    });
    Ok((address, task))
}

async fn ready(router: &axum::Router) -> Result<StatusCode, Box<dyn std::error::Error>> {
    let response = router
        .clone()
        .oneshot(Request::builder().uri("/ready").body(Body::empty())?)
        .await?;
    Ok(response.status())
}
