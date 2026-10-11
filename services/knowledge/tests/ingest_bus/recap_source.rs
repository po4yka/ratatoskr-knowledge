//! The reachability of the digest source does not gate readiness (XR-021 CONTRACTS.md R3-09).
//!
//! The recap worker's bus connection and durable are readiness conditions because a missing
//! durable is Knowledge's own misconfiguration. The channel-digests API is not: ingest and search
//! answer without it, so its absence is the gauge `knowledge_channel_recap_source_up` and nothing
//! more, and the recap command stays on the durable until the source answers.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, pull};
use async_nats::jetstream::{self, stream};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ratatoskr_knowledge::{ChannelRecapProviderMode, Config, DigestSourceSecret, RateLimiter};
use ratatoskr_knowledge_service::{
    Lifecycle, Listener, Metrics, admin_router, spawn_channel_recap_worker,
};
use tokio::sync::watch;
use tower::ServiceExt as _;

use crate::fixtures::{
    ARTICLE_ANALYSIS, EVENT_A, HttpStub, OWNER, envelope, extracted_document, recap_command, routed,
};
use crate::harness::{BROKER, Harness, Worker};
use crate::ingest_support::{TestResult, eventually};
use crate::log_capture::capture;

const COMMANDS_STREAM: &str = "ratatoskr_commands";
const RECAP_DURABLE: &str = "ratatoskr_knowledge_channel_recap";
const RECAP_SUBJECT: &str = "cmd.knowledge.channel_digest_recap.requested.v1";

/// Recreates the commands stream, and the recap durable unless `with_durable` is false.
async fn provision_commands(
    context: &jetstream::Context,
    with_durable: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let _absent_is_fine = context.delete_stream(COMMANDS_STREAM).await;
    let commands = context
        .create_stream(stream::Config {
            name: COMMANDS_STREAM.to_owned(),
            subjects: vec!["cmd.>".to_owned()],
            ..stream::Config::default()
        })
        .await?;
    if with_durable {
        commands
            .create_consumer(pull::Config {
                durable_name: Some(RECAP_DURABLE.to_owned()),
                filter_subject: RECAP_SUBJECT.to_owned(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                deliver_policy: DeliverPolicy::All,
                ..pull::Config::default()
            })
            .await?;
    }
    Ok(())
}

/// A loopback address nothing listens on, until a stub is started on it.
fn unreachable_address() -> Result<SocketAddr, std::io::Error> {
    std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()
}

/// The ingest worker and the recap worker of one process, with its operator router.
struct Process {
    lifecycle: Lifecycle,
    metrics: Arc<Metrics>,
    app: axum::Router,
    ingest: Worker,
    drain: watch::Sender<bool>,
    recap: tokio::task::JoinHandle<()>,
}

impl Process {
    fn start(harness: &Harness, digest_source: SocketAddr) -> Self {
        let mut config: Config = harness.config();
        config.channel_recap.enabled = true;
        config.channel_recap.provider_mode = ChannelRecapProviderMode::Scripted;
        let endpoint = config.ingest.bus_endpoint.clone();
        config.channel_recap.bus_endpoint = endpoint;
        config.channel_recap.digest_source_base_url = format!("http://{digest_source}/");
        config.channel_recap.digest_source_service_secret =
            Some(DigestSourceSecret::new("digest-secret".to_owned()));
        let lifecycle = Lifecycle::starting_with_channel_recap();
        lifecycle.require_ingest();
        lifecycle.mark_listener_bound(Listener::Operator);
        lifecycle.mark_listener_bound(Listener::Api);
        lifecycle.mark_ready();
        let metrics = Arc::new(Metrics::new());
        let (drain, receiver) = watch::channel(false);
        let recap = spawn_channel_recap_worker(
            config.clone(),
            harness.database.database.clone(),
            harness.blobs(),
            lifecycle.clone(),
            receiver,
            Arc::new(RateLimiter::new(Duration::ZERO)),
            Arc::clone(&metrics),
        );
        let ingest = harness.spawn_with_lifecycle(
            config,
            routed(&[("article_prompt_v1", ARTICLE_ANALYSIS)]),
            lifecycle.clone(),
        );
        Self {
            app: admin_router(lifecycle.clone(), Arc::clone(&metrics)),
            lifecycle,
            metrics,
            ingest,
            drain,
            recap,
        }
    }

    async fn ready_status(&self) -> Result<StatusCode, Box<dyn std::error::Error>> {
        let response = self
            .app
            .clone()
            .oneshot(Request::builder().uri("/ready").body(Body::empty())?)
            .await?;
        Ok(response.status())
    }

    fn source_up(&self) -> Option<u64> {
        self.metrics.render().lines().find_map(|line| {
            line.strip_prefix("knowledge_channel_recap_source_up ")
                .and_then(|value| value.parse().ok())
        })
    }

    async fn stop(self) -> TestResult {
        let _ignored = self.drain.send(true);
        tokio::time::timeout(Duration::from_secs(10), self.recap).await??;
        self.ingest.stop().await
    }
}

async fn recap_durable(harness: &Harness) -> Result<(usize, u64, u64), Box<dyn std::error::Error>> {
    let mut consumer: jetstream::consumer::PullConsumer = harness
        .context()
        .get_consumer_from_stream(RECAP_DURABLE, COMMANDS_STREAM)
        .await?;
    let info = consumer.info().await?;
    Ok((
        info.num_ack_pending,
        info.num_pending,
        info.delivered.consumer_sequence,
    ))
}

#[tokio::test]
async fn knowledge_is_ready_while_the_digest_source_is_down() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    provision_commands(harness.context(), true).await?;
    let command = recap_command()?;
    let process = Process::start(&harness, unreachable_address()?);

    eventually("/ready is 200 while the digest source is down", || async {
        Ok(process.ready_status().await? == StatusCode::OK)
    })
    .await?;
    eventually("the gauge reports the source down", || async {
        Ok(process.source_up() == Some(0))
    })
    .await?;
    harness
        .publish(
            RECAP_SUBJECT,
            "018f0000-0000-7000-8000-000000000c51",
            command.command,
        )
        .await?;
    harness
        .publish(
            "evt.content.document.extracted.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                "content.document.extracted.v1",
                "ratatoskr-extractor",
                Some(OWNER),
                &extracted_document()?,
            ),
        )
        .await?;
    eventually("ingest still makes the document searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    // The worker must not pull while the source is down, however long it waits.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(
        recap_durable(&harness).await?,
        (0, 1, 0),
        "the recap command is still on the durable, never delivered"
    );
    assert_eq!(process.ready_status().await?, StatusCode::OK);
    assert_eq!(process.source_up(), Some(0));
    process.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn the_recap_resumes_when_the_source_appears() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    provision_commands(harness.context(), true).await?;
    let command = recap_command()?;
    let address = unreachable_address()?;
    let process = Process::start(&harness, address);
    harness
        .publish(
            RECAP_SUBJECT,
            "018f0000-0000-7000-8000-000000000c51",
            command.command,
        )
        .await?;
    eventually("the gauge reports the source down", || async {
        Ok(process.source_up() == Some(0))
    })
    .await?;
    assert_eq!(recap_durable(&harness).await?.2, 0, "nothing delivered yet");

    let source = HttpStub::start_at(
        address,
        vec![
            ("/ready".to_owned(), 200, Vec::new()),
            (command.manifest_path.clone(), 200, command.manifest.clone()),
        ],
    )
    .await?;
    eventually("the gauge reports the source up", || async {
        Ok(process.source_up() == Some(1))
    })
    .await?;
    eventually("the queued command is processed", || async {
        Ok(recap_durable(&harness).await? == (0, 0, 1))
    })
    .await?;
    assert_eq!(source.hits(&command.manifest_path), 1);
    assert_eq!(process.ready_status().await?, StatusCode::OK);
    process.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn an_unreachable_source_is_logged_once_per_change() -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let harness = Harness::start().await?;
    provision_commands(harness.context(), true).await?;
    let address = unreachable_address()?;
    let process = Process::start(&harness, address);

    eventually("the gauge reports the source down", || async {
        Ok(process.source_up() == Some(0))
    })
    .await?;
    // Several probe intervals pass; a per-second warning would repeat for each of them.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        logs.lines_containing("source_unavailable"),
        1,
        "one warning for the transition to unreachable:\n{}",
        logs.text()
    );

    let _source = HttpStub::start_at(address, vec![("/ready".to_owned(), 200, Vec::new())]).await?;
    eventually("the gauge reports the source up", || async {
        Ok(process.source_up() == Some(1))
    })
    .await?;
    assert_eq!(logs.lines_containing("source_unavailable"), 1);
    assert_eq!(
        logs.lines_containing("source_reachable"),
        1,
        "one line for the transition back:\n{}",
        logs.text()
    );
    process.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_missing_recap_durable_still_makes_knowledge_unready() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    provision_commands(harness.context(), false).await?;
    let source = HttpStub::start(vec![("/ready".to_owned(), 200, Vec::new())]).await?;
    let process = Process::start(&harness, source.address());

    // The source answers and ingest is healthy, so only the missing durable can hold readiness.
    for _ in 0..8 {
        assert_eq!(
            process.ready_status().await?,
            StatusCode::SERVICE_UNAVAILABLE,
            "the recap durable is missing"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    provision_commands(harness.context(), true).await?;
    eventually("ready once the durable exists", || async {
        Ok(process.lifecycle.is_ready())
    })
    .await?;
    process.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}
