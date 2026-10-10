//! Helpers shared by the ingest integration-test binaries: the broker topology, the worker
//! configuration and a bounded polling loop.

use std::future::Future;
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, pull};
use async_nats::jetstream::{self, stream};
use ratatoskr_knowledge::{Config, IngestProviderMode, ProviderSecret};
use ratatoskr_knowledge_service::{EVENTS_STREAM, IngestFamily};

pub(crate) type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The ack wait the platform provisions on every ingest durable.
pub(crate) const PLATFORM_ACK_WAIT: Duration = Duration::from_mins(2);

/// Recreates the events stream and the four durables as the platform provisions them, with an
/// ack wait of `ack_wait`: [`PLATFORM_ACK_WAIT`], or a shorter one so a delivery abandoned by a
/// killed worker comes back within the test deadline.
pub(crate) async fn provision_with_ack_wait(
    url: &str,
    ack_wait: Duration,
) -> Result<jetstream::Context, Box<dyn std::error::Error>> {
    let context = jetstream::new(async_nats::connect(url).await?);
    let _absent_is_fine = context.delete_stream(EVENTS_STREAM).await;
    let stream = context
        .create_stream(stream::Config {
            name: EVENTS_STREAM.to_owned(),
            subjects: vec!["evt.>".to_owned()],
            max_messages: 10_000,
            max_bytes: 67_108_864,
            duplicate_window: Duration::from_mins(2),
            ..stream::Config::default()
        })
        .await?;
    for family in IngestFamily::ALL {
        stream
            .create_consumer(pull::Config {
                durable_name: Some(family.durable().to_owned()),
                filter_subject: family.filter().to_owned(),
                ack_policy: AckPolicy::Explicit,
                ack_wait,
                deliver_policy: DeliverPolicy::All,
                max_deliver: -1,
                ..pull::Config::default()
            })
            .await?;
    }
    Ok(context)
}

pub(crate) fn config(url: &str) -> Config {
    let mut config = Config::default();
    config.ingest.enabled = true;
    url.clone_into(&mut config.ingest.bus_endpoint);
    config.ingest.provider_mode = IngestProviderMode::Fixture;
    config.ingest.github_readme_service_secret =
        Some(ProviderSecret::new("readme-secret".to_owned()));
    config.limits.provider_timeout_ms = 5_000;
    config
}

pub(crate) async fn eventually<F, Fut>(what: &str, mut check: F) -> TestResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, Box<dyn std::error::Error>>>,
{
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if check().await? {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!("timed out waiting for: {what}").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
