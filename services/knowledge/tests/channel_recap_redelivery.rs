//! XR-021 CONTRACTS.md R2-05 (c) and (d): recap redelivery is bounded and a slow handler keeps its
//! message by acknowledging progress.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream::{self, consumer};
use futures_util::StreamExt as _;
use ratatoskr_channel_digest_contracts::manifest_path;
use ratatoskr_knowledge::test_support::recap::{fixture_manifest_bytes, fixture_source};
use ratatoskr_knowledge::test_support::{
    FakeReply, FakeTransport, TemporaryBlobRoot, TestDatabase,
};
use ratatoskr_knowledge::{
    BlobStore, ChannelRecapProviderMode, Config, DigestSourceSecret, RateLimiter,
};
use ratatoskr_knowledge_service::{
    Lifecycle, Metrics, RedeliverySchedule, run_with_progress, spawn_channel_recap_worker,
};
use tokio::sync::watch;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const COMMANDS_STREAM: &str = "ratatoskr_commands";
const RECAP_DURABLE: &str = "ratatoskr_knowledge_channel_recap";
const RECAP_SUBJECT: &str = "cmd.knowledge.channel_digest_recap.requested.v1";

fn nats_url() -> String {
    std::env::var("KNOWLEDGE_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:14223".to_owned())
}

/// `min(2 s * 2^(delivered-1), 60 s)`: the loop never runs hotter than once a minute after the
/// sixth delivery.
#[test]
fn redelivery_delay_doubles_from_two_seconds_and_caps_at_one_minute() {
    let seconds: Vec<u64> = (1..=7)
        .map(|delivered| RedeliverySchedule::STANDARD.delay(delivered).as_secs())
        .collect();
    assert_eq!(seconds, [2, 4, 8, 16, 32, 60, 60]);
    assert_eq!(RedeliverySchedule::STANDARD.delay(1_000).as_secs(), 60);
    assert_eq!(
        RedeliverySchedule::STANDARD.delay(0).as_secs(),
        2,
        "a missing delivery count is the first delivery"
    );
}

fn recap_command() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let (_manifest, request) =
        fixture_manifest_bytes(vec![fixture_source(910, 3, "redelivery fixture")], "en", 1)?;
    let _path = manifest_path(&request.manifest_ref);
    Ok(serde_json::to_vec(&serde_json::json!({
        "command_id": "018f0000-0000-7000-8000-000000000c61",
        "command_type": "knowledge.channel_digest_recap.requested.v1",
        "issued_at": "2026-08-21T10:00:01Z",
        "producer": "ratatoskr-channel-digests",
        "aggregate_id": format!("channel-digest-run:{}", request.digest_run_id),
        "correlation_id": format!("operation:{}", request.operation_id.0),
        "tenant_id": request.owner,
        "schema_version": 1,
        "payload": request,
    }))?)
}

async fn provision_recap_stream(
    context: &jetstream::Context,
) -> Result<jetstream::stream::Stream, Box<dyn std::error::Error>> {
    let _ignored = context.delete_stream(COMMANDS_STREAM).await;
    let stream = context
        .create_stream(jetstream::stream::Config {
            name: COMMANDS_STREAM.to_owned(),
            subjects: vec![RECAP_SUBJECT.to_owned()],
            max_messages: 1_000,
            max_bytes: 16_777_216,
            ..jetstream::stream::Config::default()
        })
        .await?;
    stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(RECAP_DURABLE.to_owned()),
            filter_subject: RECAP_SUBJECT.to_owned(),
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: Duration::from_secs(30),
            ..consumer::pull::Config::default()
        })
        .await?;
    Ok(stream)
}

fn redeliveries(metrics: &Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix("knowledge_channel_recap_redeliveries_total "))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// A persistent failure of the inbox insert is redelivered with a growing gap, not every 2 seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_inbox_is_redelivered_with_growing_gaps_and_counted() -> TestResult {
    let database = TestDatabase::create().await?;
    sqlx::raw_sql(
        "create function knowledge.refuse_recap_inbox() returns trigger language plpgsql as
         $$ begin raise exception 'recap inbox refused for the redelivery test'; end $$;
         create trigger refuse_recap_inbox before insert on knowledge.channel_recap_inbox
         for each row execute function knowledge.refuse_recap_inbox();",
    )
    .execute(database.database.pool())
    .await?;
    let blobs_root = TemporaryBlobRoot::create().await?;
    let source = FakeTransport::start(
        std::iter::repeat_with(|| FakeReply::bytes(200, Vec::new()))
            .take(32)
            .collect(),
    )
    .await?;
    let url = nats_url();
    let context = jetstream::new(async_nats::connect(&url).await?);
    provision_recap_stream(&context).await?;

    let mut config = Config::default();
    config.channel_recap.enabled = true;
    config.channel_recap.provider_mode = ChannelRecapProviderMode::Scripted;
    config.channel_recap.bus_endpoint.clone_from(&url);
    config.channel_recap.digest_source_base_url = format!("http://{}/", source.local_addr());
    config.channel_recap.digest_source_service_secret =
        Some(DigestSourceSecret::new("digest-secret".to_owned()));
    config.limits.provider_timeout_ms = 5_000;
    let metrics = Arc::new(Metrics::new());
    let (drain, receiver) = watch::channel(false);
    let worker = spawn_channel_recap_worker(
        config,
        database.database.clone(),
        BlobStore::new(blobs_root.path(), 1_048_576),
        Lifecycle::starting_with_channel_recap(),
        receiver,
        Arc::new(RateLimiter::new(Duration::ZERO)),
        Arc::clone(&metrics),
    );
    context
        .publish(RECAP_SUBJECT.to_owned(), recap_command()?.into())
        .await?
        .await?;

    let started = Instant::now();
    let mut seen_at = Vec::new();
    while seen_at.len() < 3 && started.elapsed() < Duration::from_secs(30) {
        if redeliveries(&metrics) > u64::try_from(seen_at.len())? {
            seen_at.push(Instant::now());
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let _ignored = drain.send(true);
    let stopped = tokio::time::timeout(Duration::from_secs(10), worker).await;
    let _ignored = context.delete_stream(COMMANDS_STREAM).await;
    database.cleanup().await?;
    stopped??;

    let [first, second, third] = seen_at.as_slice() else {
        return Err("three failed deliveries should have been counted".into());
    };
    let first_gap = second.duration_since(*first);
    let second_gap = third.duration_since(*second);
    assert!(
        first_gap >= Duration::from_millis(1_900),
        "the first redelivery waits the 2 second base, waited {first_gap:?}"
    );
    assert!(
        second_gap >= Duration::from_millis(3_900),
        "the second redelivery waits twice as long, waited {second_gap:?}"
    );
    Ok(())
}

/// A handler longer than the ack wait keeps its message: the worker acknowledges progress every
/// third of the wait, so the broker never redelivers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_handler_acknowledges_progress_and_is_delivered_once() -> TestResult {
    let context = jetstream::new(async_nats::connect(&nats_url()).await?);
    let name = format!("knowledge_progress_{}", uuid::Uuid::now_v7().simple());
    let subject = format!("cmd.progress.{}", uuid::Uuid::now_v7().simple());
    let stream = context
        .create_stream(jetstream::stream::Config {
            name: name.clone(),
            subjects: vec![subject.clone()],
            ..jetstream::stream::Config::default()
        })
        .await?;
    let ack_wait = Duration::from_secs(3);
    let consumer = stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some("progress".to_owned()),
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait,
            ..consumer::pull::Config::default()
        })
        .await?;
    context.publish(subject, "work".into()).await?.await?;

    let outcome = async {
        let mut messages = consumer
            .stream()
            .max_messages_per_batch(1)
            .messages()
            .await?;
        let message = messages.next().await.ok_or("no message")??;
        assert_eq!(
            message.info().map_err(|error| error.to_string())?.delivered,
            1
        );

        let finished = run_with_progress(&message, ack_wait, async {
            tokio::time::sleep(Duration::from_secs(7)).await;
            "done"
        })
        .await;
        assert_eq!(finished, "done");
        let again = tokio::time::timeout(Duration::from_secs(1), messages.next()).await;
        assert!(
            again.is_err(),
            "the message was redelivered during the handler"
        );
        message.ack().await.map_err(|error| error.to_string())?;
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let mut observed = consumer.clone();
    let info = observed
        .info()
        .await
        .map(|info| info.delivered.consumer_sequence);
    let _ignored = context.delete_stream(&name).await;
    outcome?;
    assert_eq!(info?, 1, "the broker delivered the message exactly once");
    Ok(())
}
