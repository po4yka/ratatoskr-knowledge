//! The workers against a broker that enforces the Knowledge identity of XR-021 CONTRACTS.md S03.
//!
//! `deploy/nats/identity.conf` is the Knowledge stanza of the single reviewed ACL. This test boots
//! an authorization-enabled broker with exactly that stanza, provisions the durables as the
//! platform would, and runs the real recap and ingest workers under the Knowledge nkey. The
//! refusals are observed the only way a client can see them: as a missing acknowledgement, because
//! a denied publish is visible in the server log alone.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull};
use async_nats::jetstream::{self, stream};
use ratatoskr_knowledge::test_support::recap::{
    fixture_source, model_output, provider_response, seed_recap_run, seed_recap_run_variant,
};
use ratatoskr_knowledge::test_support::{
    FakeReply, FakeTransport, TemporaryBlobRoot, TestDatabase,
};
use ratatoskr_knowledge::{
    BlobStore, ChannelRecapContextPolicy, ChannelRecapOutputLanguage, ChannelRecapPipeline,
    ChannelRecapRunState, ChannelRecapRunStore, Config, DigestSourceSecret, IngestProviderMode,
    ProviderResponse, ProviderSecret, ProviderUsage, ScriptedProvider,
    build_channel_recap_provider_request, prepare_channel_recap_context,
};
use ratatoskr_knowledge_service::{
    EVENTS_STREAM, IngestFamily, Lifecycle, spawn_channel_recap_worker,
    spawn_ingest_workers_with_provider,
};
use tokio::sync::watch;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PORT: u16 = 57_063;
const COMMANDS_STREAM: &str = "ratatoskr_commands";
const RECAP_DURABLE: &str = "ratatoskr_knowledge_channel_recap";
const RECAP_SUBJECT: &str = "cmd.knowledge.channel_digest_recap.requested.v1";
const FOREIGN_DURABLE: &str = "ratatoskr_channel_digest_recap_completed";
const PLACEHOLDER_PREFIX: &str = "UREPLACE_ME_WITH_THE_PUBLIC_NKEY_OF_RATATOSKR_KNOWLEDGE_";
const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";
const SOCIAL: &str = r#"{
  "social_source_id":"018f0000-0000-7000-8000-000000000201",
  "platform":"x", "external_post_id":"1234567890123456789",
  "owner":"user:018f0000-0000-7000-8000-000000000005",
  "captured_at":"2026-08-17T10:00:00Z", "text":"A useful post.",
  "content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
  "acquisition":"official_api", "saved_authority":"authoritative_platform_state",
  "completeness":"complete", "upstream_availability":"available"
}"#;
const SOCIAL_ANALYSIS: &str = r#"{"summary":"Useful post.","topics":["rust"],"evidence_excerpt":"useful post","confidence":"grounded"}"#;

/// Kills the broker when the test ends, passed or failed.
struct Broker(Child);

impl Drop for Broker {
    fn drop(&mut self) {
        let _ignored = self.0.kill();
        let _ignored = self.0.wait();
    }
}

/// The Knowledge stanza with the placeholder nkey replaced by `public_key`.
fn knowledge_stanza(public_key: &str) -> Result<String, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/nats/identity.conf"
    ))?;
    if !text.contains(PLACEHOLDER_PREFIX) {
        return Err("the identity fragment carries no placeholder nkey".into());
    }
    Ok(text
        .lines()
        .map(|line| match line.split_once(PLACEHOLDER_PREFIX) {
            Some((before, rest)) => {
                let after =
                    rest.trim_start_matches(|c: char| c.is_ascii_alphanumeric() || c == '_');
                format!("{before}{public_key}{after}")
            }
            None => line.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

fn start_broker(
    directory: &Path,
    admin_public: &str,
    stanza: &str,
) -> Result<Broker, Box<dyn std::error::Error>> {
    let configuration = directory.join("broker.conf");
    std::fs::write(
        &configuration,
        format!(
            "port: {PORT}\nhost: 127.0.0.1\nserver_name: ratatoskr_test\n\
             jetstream {{ store_dir: \"{}\" }}\n\
             authorization {{\n  users: [\n    {{ nkey: {admin_public}, permissions: {{ publish: \">\", subscribe: \">\" }} }},\n{stanza}\n  ]\n}}\n",
            directory.join("store").display()
        ),
    )?;
    let binary =
        std::env::var("KNOWLEDGE_TEST_NATS_SERVER").unwrap_or_else(|_| "nats-server".to_owned());
    Ok(Broker(
        Command::new(binary)
            .arg("-c")
            .arg(&configuration)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    ))
}

async fn eventually<F, Fut>(what: &str, mut check: F) -> TestResult
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool, Box<dyn std::error::Error>>>,
{
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if check().await.unwrap_or(false) {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!("timed out waiting for: {what}").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Creates both streams and every durable the Knowledge identity may use, plus one it may not.
async fn provision(context: &jetstream::Context) -> TestResult {
    let commands = context
        .create_stream(stream::Config {
            name: COMMANDS_STREAM.to_owned(),
            subjects: vec!["cmd.>".to_owned()],
            ..stream::Config::default()
        })
        .await?;
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
    let events = context
        .create_stream(stream::Config {
            name: EVENTS_STREAM.to_owned(),
            subjects: vec!["evt.>".to_owned()],
            duplicate_window: Duration::from_mins(2),
            ..stream::Config::default()
        })
        .await?;
    for family in IngestFamily::ALL {
        events
            .create_consumer(pull::Config {
                durable_name: Some(family.durable().to_owned()),
                filter_subject: family.filter().to_owned(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_mins(2),
                deliver_policy: DeliverPolicy::All,
                max_deliver: -1,
                ..pull::Config::default()
            })
            .await?;
    }
    events
        .create_consumer(pull::Config {
            durable_name: Some(FOREIGN_DURABLE.to_owned()),
            filter_subject: "evt.knowledge.channel_digest_recap.completed.v1".to_owned(),
            ack_policy: AckPolicy::Explicit,
            ..pull::Config::default()
        })
        .await?;
    Ok(())
}

async fn messages_on(
    context: &jetstream::Context,
    subject: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let events = context.get_stream(EVENTS_STREAM).await?;
    let mut consumer = events
        .create_consumer(pull::Config {
            filter_subject: subject.to_owned(),
            ack_policy: AckPolicy::None,
            deliver_policy: DeliverPolicy::All,
            ..pull::Config::default()
        })
        .await?;
    Ok(usize::try_from(consumer.info().await?.num_pending)?)
}

fn knowledge_config(seed: &Path, digest_source: std::net::SocketAddr) -> Config {
    let url = format!("nats://127.0.0.1:{PORT}");
    let mut config = Config::default();
    config.ingest.enabled = true;
    config.ingest.bus_endpoint.clone_from(&url);
    config.ingest.bus_credentials_file = Some(seed.to_path_buf());
    config.ingest.provider_mode = IngestProviderMode::Fixture;
    config.ingest.github_readme_service_secret =
        Some(ProviderSecret::new("readme-secret".to_owned()));
    config.channel_recap.enabled = true;
    config.channel_recap.bus_endpoint = url;
    config.channel_recap.bus_credentials_file = Some(seed.to_path_buf());
    config.channel_recap.digest_source_base_url = format!("http://{digest_source}/");
    config.channel_recap.digest_source_service_secret =
        Some(DigestSourceSecret::new("digest-secret".to_owned()));
    config.limits.provider_timeout_ms = 5_000;
    config
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario proves every permitted and refused operation under one identity"
)]
async fn knowledge_workers_run_under_the_pinned_identity_and_nothing_more() -> TestResult {
    let admin = nkeys::KeyPair::new_user();
    let knowledge = nkeys::KeyPair::new_user();
    let directory = TemporaryBlobRoot::create().await?;
    let seed = directory.path().join("knowledge.nkey");
    std::fs::write(&seed, knowledge.seed()?)?;
    let _broker = start_broker(
        directory.path(),
        &admin.public_key(),
        &knowledge_stanza(&knowledge.public_key())?,
    )?;
    let url = format!("nats://127.0.0.1:{PORT}");
    let admin_client = {
        let options = async_nats::ConnectOptions::with_nkey(admin.seed()?);
        let mut connected = None;
        for _ in 0..100 {
            if let Ok(client) = options.clone().connect(&url).await {
                connected = Some(client);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        connected.ok_or("the authorized broker did not start")?
    };
    let admin_context = jetstream::new(admin_client);
    provision(&admin_context).await?;

    // Two terminal recap facts waiting in the outbox, one completed and one failed.
    let database = TestDatabase::create().await?;
    let blobs_root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(blobs_root.path(), 1_048_576);
    let runs = ChannelRecapRunStore::new(&database.database);
    let failed_recap = seed_recap_run(
        &database.database,
        vec![fixture_source(900, 1, "authorized bus fixture")],
        "en",
    )
    .await?;
    runs.settle_failed(
        failed_recap.run_id,
        ChannelRecapRunState::ContextPrepared,
        &serde_json::from_value(serde_json::json!({
            "owner": failed_recap.request.owner,
            "operation_id": failed_recap.request.operation_id,
            "digest_run_id": failed_recap.request.digest_run_id,
            "manifest_digest": failed_recap.request.manifest_digest,
            "failure_code": "provider_unavailable",
            "failed_at": "2026-08-21T10:01:00Z",
        }))?,
    )
    .await?;
    complete_a_recap(&database, &blobs).await?;

    let digest_source = FakeTransport::start(
        std::iter::repeat_with(|| FakeReply::bytes(200, Vec::new()))
            .take(64)
            .collect(),
    )
    .await?;
    let config = knowledge_config(&seed, digest_source.local_addr());
    let recap_lifecycle = Lifecycle::starting_with_channel_recap();
    let ingest_lifecycle = Lifecycle::starting_with_ingest();
    let (drain, receiver) = watch::channel(false);
    let recap_worker = spawn_channel_recap_worker(
        config.clone(),
        database.database.clone(),
        blobs.clone(),
        recap_lifecycle,
        receiver.clone(),
    );
    let ingest_worker = spawn_ingest_workers_with_provider(
        config,
        database.database.clone(),
        blobs,
        ingest_lifecycle,
        receiver,
        Arc::new(ScriptedProvider::new([Ok(ProviderResponse {
            bytes: SOCIAL_ANALYSIS.as_bytes().to_vec(),
            request_id: Some("scripted-authorized".to_owned()),
            usage: ProviderUsage {
                input_tokens: 20,
                output_tokens: 10,
            },
        })])),
    );

    // The recap path: the consumer fetches and acknowledges a command (a malformed one is
    // terminated), and the outbox publisher then publishes both terminal facts.
    admin_context
        .publish(RECAP_SUBJECT.to_owned(), b"not a command".to_vec().into())
        .await?
        .await?;
    // The ingest path: a social capture is consumed, analysed and its completion published.
    let source: serde_json::Value = serde_json::from_str(SOCIAL)?;
    admin_context
        .publish(
            "evt.social.source.captured.v1".to_owned(),
            serde_json::to_vec(&serde_json::json!({
                "event_id": "018f0000-0000-7000-8000-000000000c01",
                "event_type": "social.source.captured.v1",
                "occurred_at": "2026-08-27T06:00:00Z",
                "producer": "ratatoskr-x",
                "aggregate_id": "social_source:018f0000-0000-7000-8000-000000000201",
                "correlation_id": "operation:018f0000-0000-7000-8000-000000000c02",
                "tenant_id": OWNER,
                "schema_version": 1,
                "payload": {"source": source},
            }))?
            .into(),
        )
        .await?
        .await?;

    for subject in [
        "evt.knowledge.channel_digest_recap.completed.v1",
        "evt.knowledge.channel_digest_recap.failed.v1",
        "evt.knowledge.analysis.completed.v1",
    ] {
        eventually(subject, || async {
            Ok(messages_on(&admin_context, subject).await? == 1)
        })
        .await?;
    }
    let documents: i64 = sqlx::query_scalar("select count(*) from knowledge.search_documents")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(documents, 1, "the ingest consumer analysed the capture");
    let mut recap_consumer: PullConsumer = admin_context
        .get_consumer_from_stream(RECAP_DURABLE, COMMANDS_STREAM)
        .await?;
    let recap_info = recap_consumer.info().await?;
    assert_eq!(
        (recap_info.num_ack_pending, recap_info.num_pending),
        (0, 0),
        "the command was fetched and acknowledged"
    );

    // Refusals, as the Knowledge identity itself.
    let own_client = async_nats::ConnectOptions::with_nkey(knowledge.seed()?)
        .connect(&url)
        .await?;
    let own = jetstream::new(own_client);
    let denied_publish = tokio::time::timeout(Duration::from_secs(3), async {
        own.publish(
            "evt.platform.notification.raised.v1".to_owned(),
            b"{}".to_vec().into(),
        )
        .await?
        .await
    })
    .await;
    assert!(
        !matches!(denied_publish, Ok(Ok(_))),
        "a publish outside the Knowledge allowlist must not be acknowledged"
    );
    assert_eq!(
        messages_on(&admin_context, "evt.platform.notification.raised.v1").await?,
        0,
        "the refused message never reached the stream"
    );
    let denied_inspection = tokio::time::timeout(
        Duration::from_secs(3),
        own.get_consumer_from_stream::<pull::Config, _, _>(FOREIGN_DURABLE, EVENTS_STREAM),
    )
    .await;
    assert!(
        !matches!(denied_inspection, Ok(Ok(_))),
        "another service's durable must not be inspectable"
    );

    let _ignored = drain.send(true);
    tokio::time::timeout(Duration::from_secs(10), recap_worker).await??;
    tokio::time::timeout(Duration::from_secs(10), ingest_worker).await??;
    database.cleanup().await?;
    Ok(())
}

/// A second recap run, executed to completion, so a completed fact waits in the outbox.
async fn complete_a_recap(
    database: &TestDatabase,
    blobs: &BlobStore,
) -> Result<(), Box<dyn std::error::Error>> {
    let seeded = seed_recap_run_variant(
        &database.database,
        vec![fixture_source(901, 2, "authorized bus second fixture")],
        "en",
        1,
    )
    .await?;
    let context = prepare_channel_recap_context(
        &seeded.verified,
        ChannelRecapContextPolicy {
            max_sources: 100,
            max_channels: 20,
            max_characters: 100_000,
        },
    )?;
    let provider = ScriptedProvider::new([Ok(provider_response(
        &model_output(&context, &[]),
        "scripted-authorized-recap",
    )?)]);
    ChannelRecapPipeline::new(&database.database, &provider, blobs, Duration::from_secs(5))
        .execute(
            seeded.run_id,
            build_channel_recap_provider_request(&context, ChannelRecapOutputLanguage::En, 2_048)?,
            &context,
            &seeded.verified.digest_hex,
            ChannelRecapOutputLanguage::En,
        )
        .await?;
    Ok(())
}
