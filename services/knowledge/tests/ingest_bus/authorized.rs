//! The workers against a broker that enforces the Knowledge identity of XR-021 CONTRACTS.md S03.
//!
//! `deploy/nats/identity.conf` is the Knowledge stanza of the single reviewed ACL. These tests
//! boot an authorization-enabled broker with exactly that stanza and provision the durables as the
//! platform would. The first runs the real recap and ingest workers under the Knowledge nkey and
//! feeds every durable one valid message: each must be fetched, acknowledged and answered with its
//! completion fact, and the process must report ready. Removing one durable's grants from the
//! stanza makes that test fail, because that consumer can no longer connect. The second observes
//! the refusals the only way a client can: as a missing acknowledgement, because a denied publish
//! is visible in the server log alone.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull};
use async_nats::jetstream::{self, stream};
use ratatoskr_channel_digest_contracts::manifest_path;
use ratatoskr_knowledge::test_support::recap::{
    fixture_manifest_bytes, fixture_source, seed_recap_run,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, ChannelRecapProviderMode, ChannelRecapRunState, ChannelRecapRunStore, Config,
    DigestSourceSecret, IngestProviderMode, ProviderSecret, RateLimiter,
};
use ratatoskr_knowledge_service::{
    EVENTS_STREAM, IngestFamily, Lifecycle, spawn_channel_recap_worker,
    spawn_ingest_workers_with_provider,
};
use tokio::sync::watch;

use crate::fixtures::{
    ARCHIVE_ANALYSIS, ARTICLE_ANALYSIS, CAPTURED, CONTRACT_CONVERSATION, CONVERSATION_ADDED,
    EVENT_A, EVENT_B, EVENT_C, EVENT_D, HttpStub, OWNER, REPOSITORY_ANALYSIS_ABSENT,
    REPOSITORY_REQUESTED, SOCIAL_ANALYSIS, envelope, extracted_document, repository_request,
    routed, social_payload,
};
use crate::ingest_support::{TestResult, eventually};

const COMMANDS_STREAM: &str = "ratatoskr_commands";
const RECAP_DURABLE: &str = "ratatoskr_knowledge_channel_recap";
const RECAP_SUBJECT: &str = "cmd.knowledge.channel_digest_recap.requested.v1";
const FOREIGN_DURABLE: &str = "ratatoskr_channel_digest_recap_completed";
const PLACEHOLDER_PREFIX: &str = "UREPLACE_ME_WITH_THE_PUBLIC_NKEY_OF_RATATOSKR_KNOWLEDGE_";

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

/// A loopback port nothing listens on; the kernel picks it, so concurrent runs cannot collide.
fn free_port() -> Result<u16, std::io::Error> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

/// An authorization-enabled broker with the Knowledge identity, its topology and an admin handle.
struct Authorized {
    _broker: Broker,
    _directory: TemporaryBlobRoot,
    url: String,
    seed: PathBuf,
    knowledge: nkeys::KeyPair,
    admin: jetstream::Context,
}

impl Authorized {
    async fn boot() -> Result<Self, Box<dyn std::error::Error>> {
        let admin_key = nkeys::KeyPair::new_user();
        let knowledge = nkeys::KeyPair::new_user();
        let directory = TemporaryBlobRoot::create().await?;
        let seed = directory.path().join("knowledge.nkey");
        std::fs::write(&seed, knowledge.seed()?)?;
        let port = free_port()?;
        let broker = start_broker(
            directory.path(),
            port,
            &admin_key.public_key(),
            &knowledge_stanza(&knowledge.public_key())?,
        )?;
        let url = format!("nats://127.0.0.1:{port}");
        let options = async_nats::ConnectOptions::with_nkey(admin_key.seed()?);
        let mut connected = None;
        for _ in 0..100 {
            if let Ok(client) = options.clone().connect(&url).await {
                connected = Some(client);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let admin = jetstream::new(connected.ok_or("the authorized broker did not start")?);
        provision(&admin).await?;
        Ok(Self {
            _broker: broker,
            _directory: directory,
            url,
            seed,
            knowledge,
            admin,
        })
    }

    fn config(&self, digest_source: std::net::SocketAddr) -> Config {
        let mut config = Config::default();
        config.ingest.enabled = true;
        config.ingest.bus_endpoint.clone_from(&self.url);
        config.ingest.bus_credentials_file = Some(self.seed.clone());
        config.ingest.provider_mode = IngestProviderMode::Fixture;
        config.ingest.github_readme_service_secret =
            Some(ProviderSecret::new("readme-secret".to_owned()));
        config.channel_recap.enabled = true;
        config.channel_recap.provider_mode = ChannelRecapProviderMode::Scripted;
        config.channel_recap.bus_endpoint.clone_from(&self.url);
        config.channel_recap.bus_credentials_file = Some(self.seed.clone());
        config.channel_recap.digest_source_base_url = format!("http://{digest_source}/");
        config.channel_recap.digest_source_service_secret =
            Some(DigestSourceSecret::new("digest-secret".to_owned()));
        config.limits.provider_timeout_ms = 5_000;
        config
    }

    /// Unacknowledged, undelivered and delivered counts of one durable.
    async fn durable(
        &self,
        stream_name: &str,
        durable: &str,
    ) -> Result<(usize, u64, u64), Box<dyn std::error::Error>> {
        let mut consumer: PullConsumer = self
            .admin
            .get_consumer_from_stream(durable, stream_name)
            .await?;
        let info = consumer.info().await?;
        Ok((
            info.num_ack_pending,
            info.num_pending,
            info.delivered.consumer_sequence,
        ))
    }

    /// How many messages `subject` holds on the events stream.
    async fn messages_on(&self, subject: &str) -> Result<usize, Box<dyn std::error::Error>> {
        let events = self.admin.get_stream(EVENTS_STREAM).await?;
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

    async fn publish(&self, subject: &str, bytes: Vec<u8>) -> TestResult {
        self.admin
            .publish(subject.to_owned(), bytes.into())
            .await?
            .await?;
        Ok(())
    }
}

fn start_broker(
    directory: &Path,
    port: u16,
    admin_public: &str,
    stanza: &str,
) -> Result<Broker, Box<dyn std::error::Error>> {
    let configuration = directory.join("broker.conf");
    std::fs::write(
        &configuration,
        format!(
            "port: {port}\nhost: 127.0.0.1\nserver_name: ratatoskr_test\n\
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

/// A recap run that failed before the workers start, so a failed fact waits in the outbox.
async fn seed_failed_recap(database: &TestDatabase) -> TestResult {
    let failed = seed_recap_run(
        &database.database,
        vec![fixture_source(900, 1, "authorized bus fixture")],
        "en",
    )
    .await?;
    ChannelRecapRunStore::new(&database.database)
        .settle_failed(
            failed.run_id,
            ChannelRecapRunState::ContextPrepared,
            &serde_json::from_value(serde_json::json!({
                "owner": failed.request.owner,
                "operation_id": failed.request.operation_id,
                "digest_run_id": failed.request.digest_run_id,
                "manifest_digest": failed.request.manifest_digest,
                "failure_code": "provider_unavailable",
                "failed_at": "2026-08-21T10:01:00Z",
            }))?,
        )
        .await?;
    Ok(())
}

/// A valid recap command and the digest-source route that serves its manifest.
struct RecapCommand {
    command: Vec<u8>,
    manifest_path: String,
    manifest: Vec<u8>,
}

fn recap_command() -> Result<RecapCommand, Box<dyn std::error::Error>> {
    let (manifest, request) = fixture_manifest_bytes(
        vec![fixture_source(901, 2, "authorized bus recap fixture")],
        "en",
        1,
    )?;
    let command = serde_json::to_vec(&serde_json::json!({
        "command_id": "018f0000-0000-7000-8000-000000000c51",
        "command_type": "knowledge.channel_digest_recap.requested.v1",
        "issued_at": "2026-08-21T10:00:01Z",
        "producer": "ratatoskr-channel-digests",
        "aggregate_id": format!("channel-digest-run:{}", request.digest_run_id),
        "correlation_id": format!("operation:{}", request.operation_id.0),
        "tenant_id": request.owner,
        "schema_version": 1,
        "payload": request,
    }))?;
    Ok(RecapCommand {
        command,
        manifest_path: manifest_path(&request.manifest_ref),
        manifest,
    })
}

/// One valid message for each of the five durables.
async fn publish_one_valid_message_per_durable(
    fabric: &Authorized,
    recap_command: Vec<u8>,
) -> TestResult {
    fabric.publish(RECAP_SUBJECT, recap_command).await?;
    fabric
        .publish(
            "evt.content.document.extracted.v1",
            envelope(
                EVENT_A,
                "content.document.extracted.v1",
                "ratatoskr-extractor",
                Some(OWNER),
                &extracted_document()?,
            ),
        )
        .await?;
    fabric
        .publish(
            "evt.social.source.captured.v1",
            envelope(
                EVENT_B,
                CAPTURED,
                "ratatoskr-x",
                Some(OWNER),
                &social_payload("2026-08-17T10:00:00Z")?,
            ),
        )
        .await?;
    fabric
        .publish(
            "evt.ai_archive.conversation.added.v1",
            envelope(
                EVENT_C,
                CONVERSATION_ADDED,
                "ratatoskr-chatgpt",
                Some(OWNER),
                &serde_json::from_str(CONTRACT_CONVERSATION)?,
            ),
        )
        .await?;
    fabric
        .publish(
            "evt.knowledge.repository_analysis.requested.v1",
            envelope(
                EVENT_D,
                REPOSITORY_REQUESTED,
                "ratatoskr-github",
                Some(OWNER),
                &serde_json::to_value(repository_request(OWNER, None)?)?,
            ),
        )
        .await
}

/// Every durable fetched exactly one message and holds none open.
async fn assert_every_durable_consumed_and_acknowledged(fabric: &Authorized) -> TestResult {
    let mut durables = vec![(COMMANDS_STREAM, RECAP_DURABLE)];
    durables.extend(
        IngestFamily::ALL
            .into_iter()
            .map(|family| (EVENTS_STREAM, family.durable())),
    );
    for (stream_name, durable) in durables {
        eventually(
            &format!("{durable} is consumed and acknowledged"),
            || async { Ok(fabric.durable(stream_name, durable).await? == (0, 0, 1)) },
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn every_knowledge_consumer_runs_and_acknowledges_under_the_pinned_identity() -> TestResult {
    let fabric = Authorized::boot().await?;
    let database = TestDatabase::create().await?;
    seed_failed_recap(&database).await?;
    let blobs_root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(blobs_root.path(), 1_048_576);
    let recap = recap_command()?;
    let digest_source = HttpStub::start(vec![
        ("/ready".to_owned(), 200, Vec::new()),
        (recap.manifest_path.clone(), 200, recap.manifest.clone()),
    ])
    .await?;

    let config = fabric.config(digest_source.address());
    let lifecycle = Lifecycle::starting_with_channel_recap();
    lifecycle.require_ingest();
    lifecycle.mark_ready();
    let (drain, receiver) = watch::channel(false);
    let recap_worker = spawn_channel_recap_worker(
        config.clone(),
        database.database.clone(),
        blobs.clone(),
        lifecycle.clone(),
        receiver.clone(),
        Arc::new(RateLimiter::new(Duration::ZERO)),
    );
    let ingest_worker = spawn_ingest_workers_with_provider(
        config,
        database.database.clone(),
        blobs,
        lifecycle.clone(),
        receiver,
        routed(&[
            ("article_prompt_v1", ARTICLE_ANALYSIS),
            ("social_prompt_v1", SOCIAL_ANALYSIS),
            ("archive_prompt_v1", ARCHIVE_ANALYSIS),
            ("repository_prompt_v1", REPOSITORY_ANALYSIS_ABSENT),
        ]),
    );
    eventually("both workers report ready under the identity", || async {
        Ok(lifecycle.is_ready())
    })
    .await?;

    publish_one_valid_message_per_durable(&fabric, recap.command).await?;
    assert_every_durable_consumed_and_acknowledged(&fabric).await?;
    for subject in [
        "evt.knowledge.channel_digest_recap.completed.v1",
        "evt.knowledge.channel_digest_recap.failed.v1",
        "evt.knowledge.analysis.completed.v1",
        "evt.knowledge.ai_archive_analysis.completed.v1",
        "evt.knowledge.repository_analysis.completed.v1",
    ] {
        eventually(subject, || async {
            Ok(fabric.messages_on(subject).await? == 1)
        })
        .await?;
    }
    let documents: i64 = sqlx::query_scalar("select count(*) from knowledge.search_documents")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(
        documents, 4,
        "the document, social, archive and repository analyses are all searchable"
    );
    assert_eq!(
        digest_source.hits(&recap.manifest_path),
        1,
        "the recap manifest was fetched once"
    );
    assert!(
        lifecycle.is_ready(),
        "the process is still ready after it served every durable"
    );

    let _ignored = drain.send(true);
    tokio::time::timeout(Duration::from_secs(10), recap_worker).await??;
    tokio::time::timeout(Duration::from_secs(10), ingest_worker).await??;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn the_knowledge_identity_cannot_publish_or_inspect_outside_its_allowlist() -> TestResult {
    let fabric = Authorized::boot().await?;
    let own = jetstream::new(
        async_nats::ConnectOptions::with_nkey(fabric.knowledge.seed()?)
            .connect(&fabric.url)
            .await?,
    );
    // Control: the identity does reach its own durable, so the refusals below are the ACL's.
    own.get_consumer_from_stream::<pull::Config, _, _>(
        IngestFamily::Documents.durable(),
        EVENTS_STREAM,
    )
    .await?;

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
        fabric
            .messages_on("evt.platform.notification.raised.v1")
            .await?,
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
    Ok(())
}
