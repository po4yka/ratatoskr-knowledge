//! The harness of the notes intake tests: a disposable database, a Knowledge blob root, a notes
//! blob root in the layout of `ratatoskr-notes`, the shared broker with the two notes durables as
//! Platform provisions them, and the change events that `ratatoskr-notes` publishes.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::{
    self,
    consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull},
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{BlobStore, Config, ProviderResponse, ProviderUsage, ScriptedProvider};
use ratatoskr_knowledge_service::{
    EVENTS_STREAM, Lifecycle, Metrics, Redelivery, RedeliverySchedule,
    spawn_ingest_workers_with_schedule,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::watch;

use crate::ingest_support::{
    PLATFORM_ACK_WAIT, TestResult, config, eventually, provision_with_ack_wait,
};

pub(crate) const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";
pub(crate) const CONNECTION: &str = "018f0000-0000-7000-8000-00000000c001";
pub(crate) const FILE: &str = "018f0000-0000-7000-8000-00000000f001";
pub(crate) const FILES_DURABLE: &str = "ratatoskr_knowledge_notes_files";
pub(crate) const FILES_SUBJECT: &str = "evt.notes.file.changed.v1";
pub(crate) const VAULTS_DURABLE: &str = "ratatoskr_knowledge_notes_vaults";
const VAULTS_FILTER: &str = "evt.notes.vault.>";
pub(crate) const NOTE_TEXT: &str = "# Garden\n\nThe garden plan starts in spring.\n";
pub(crate) const EXCERPT: &str = "The garden plan starts in spring.";

/// A redelivery schedule short enough that a negative acknowledgement comes back inside a test.
const FAST_REDELIVERY: RedeliverySchedule =
    RedeliverySchedule::new(Duration::from_millis(20), Duration::from_millis(100));

/// One broker, one test at a time: the stream and the durables are shared by name.
pub(crate) static BROKER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn nats_url() -> String {
    std::env::var("KNOWLEDGE_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:14223".to_owned())
}

/// A disposable database, a Knowledge blob root, a notes blob root and the shared broker.
pub(crate) struct Setup {
    pub(crate) database: TestDatabase,
    blobs: BlobStore,
    _knowledge_root: TemporaryBlobRoot,
    pub(crate) notes_root: TemporaryBlobRoot,
    context: jetstream::Context,
    pub(crate) url: String,
    pub(crate) metrics: Arc<Metrics>,
}

/// A running ingest worker.
pub(crate) struct Worker {
    drain: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
}

impl Worker {
    pub(crate) async fn stop(self) -> TestResult {
        let _ignored = self.drain.send(true);
        tokio::time::timeout(Duration::from_secs(10), self.handle).await??;
        Ok(())
    }
}

impl Setup {
    pub(crate) async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let url = nats_url();
        let context = provision_with_ack_wait(&url, PLATFORM_ACK_WAIT).await?;
        let stream = context.get_stream(EVENTS_STREAM).await?;
        for (durable, filter) in [
            (FILES_DURABLE, FILES_SUBJECT),
            (VAULTS_DURABLE, VAULTS_FILTER),
        ] {
            stream
                .get_or_create_consumer(
                    durable,
                    pull::Config {
                        durable_name: Some(durable.to_owned()),
                        filter_subject: filter.to_owned(),
                        ack_policy: AckPolicy::Explicit,
                        ack_wait: PLATFORM_ACK_WAIT,
                        deliver_policy: DeliverPolicy::All,
                        max_deliver: -1,
                        ..pull::Config::default()
                    },
                )
                .await?;
        }
        let knowledge_root = TemporaryBlobRoot::create().await?;
        Ok(Self {
            database: TestDatabase::create().await?,
            blobs: BlobStore::new(knowledge_root.path(), 16_777_216),
            _knowledge_root: knowledge_root,
            notes_root: TemporaryBlobRoot::create().await?,
            context,
            url,
            metrics: Arc::new(Metrics::new()),
        })
    }

    /// The worker configuration, with the notes blob root of this test.
    pub(crate) fn config(&self) -> Config {
        let mut config = config(&self.url);
        config.blobs.notes_root = Some(self.notes_root.path().to_path_buf());
        config
    }

    pub(crate) fn spawn_with(&self, config: Config, provider: Arc<ScriptedProvider>) -> Worker {
        let (drain, receiver) = watch::channel(false);
        let handle = spawn_ingest_workers_with_schedule(
            config,
            self.database.database.clone(),
            self.blobs.clone(),
            Lifecycle::starting_with_ingest(),
            receiver,
            provider,
            Redelivery::new(Arc::clone(&self.metrics), FAST_REDELIVERY),
        );
        Worker { drain, handle }
    }

    pub(crate) fn spawn(&self, provider: Arc<ScriptedProvider>) -> Worker {
        self.spawn_with(self.config(), provider)
    }

    /// Writes `bytes` into the notes blob root as `ratatoskr-notes` does and returns the revision.
    pub(crate) fn store_note(&self, bytes: &[u8]) -> Result<String, std::io::Error> {
        let revision = format!("{:x}", Sha256::digest(bytes));
        let (prefix, rest) = revision.split_at(2);
        let directory = self.notes_root.path().join("sha256").join(prefix);
        std::fs::create_dir_all(&directory)?;
        std::fs::write(directory.join(rest), bytes)?;
        Ok(revision)
    }

    pub(crate) async fn publish(
        &self,
        subject: &str,
        message_id: &str,
        bytes: Vec<u8>,
    ) -> TestResult {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", message_id);
        self.context
            .publish_with_headers(subject.to_owned(), headers, bytes.into())
            .await?
            .await?;
        Ok(())
    }

    /// `(ack pending, pending, delivered)` of one durable.
    pub(crate) async fn durable(
        &self,
        name: &str,
    ) -> Result<(usize, u64, u64), Box<dyn std::error::Error>> {
        let mut consumer: PullConsumer = self
            .context
            .get_consumer_from_stream(name, EVENTS_STREAM)
            .await?;
        let info = consumer.info().await?;
        Ok((
            info.num_ack_pending,
            info.num_pending,
            info.delivered.consumer_sequence,
        ))
    }

    /// Waits until the durable has delivered `deliveries` messages and holds none open, and
    /// returns its last state, so the caller asserts it.
    pub(crate) async fn settle(
        &self,
        name: &str,
        deliveries: u64,
    ) -> Result<(usize, u64, u64), Box<dyn std::error::Error>> {
        // A durable that never settles is reported by the caller's assertion on the state.
        let _timed_out = eventually("the durable settles", || async {
            Ok(self.durable(name).await? == (0, 0, deliveries))
        })
        .await;
        self.durable(name).await
    }

    pub(crate) async fn count(&self, sql: &str) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(sql)
            .fetch_one(self.database.database.pool())
            .await
    }

    /// The analysis runs of one file revision.
    pub(crate) async fn runs_for(&self, file_id: &str, revision: &str) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "select count(*) from knowledge.analysis_runs r
             join knowledge.source_refs s on s.source_ref_id = r.source_ref_id
             where s.owner_context = 'ratatoskr-notes' and s.source_document_id = $1
               and s.content_digest_hex = $2",
        )
        .bind(file_id)
        .bind(revision)
        .fetch_one(self.database.database.pool())
        .await
    }

    pub(crate) async fn finish(self, worker: Worker) -> TestResult {
        worker.stop().await?;
        self.database.cleanup().await?;
        Ok(())
    }
}

/// One file change as `ratatoskr-notes` publishes it.
pub(crate) struct Change<'a> {
    pub(crate) file_id: &'a str,
    pub(crate) sequence: u64,
    pub(crate) kind: &'a str,
    pub(crate) path: &'a str,
    pub(crate) revision: &'a str,
    pub(crate) media_type: &'a str,
    pub(crate) length: usize,
}

impl Change<'_> {
    pub(crate) fn payload(&self) -> serde_json::Value {
        let mut payload = serde_json::json!({
            "connection_id": CONNECTION,
            "file_id": self.file_id,
            "connection_sequence": self.sequence,
            "change_kind": self.kind,
            "path": self.path,
            "revision": self.revision,
            "media_type": self.media_type,
        });
        if self.kind != "deleted"
            && let Some(object) = payload.as_object_mut()
        {
            object.insert(
                "blob".to_owned(),
                serde_json::json!({
                    "owner_service": "ratatoskr-notes",
                    "digest": { "algorithm": "sha256", "hex": self.revision },
                    "media_type": self.media_type,
                    "length_bytes": self.length,
                }),
            );
        }
        payload
    }

    pub(crate) fn envelope(&self, event_id: &str) -> Vec<u8> {
        envelope(
            event_id,
            "notes.file.changed.v1",
            &format!("notes_file:{}", self.file_id),
            &self.payload(),
        )
    }
}

pub(crate) fn envelope(
    event_id: &str,
    event_type: &str,
    aggregate_id: &str,
    payload: &serde_json::Value,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": "2026-10-14T06:00:00Z",
        "producer": "ratatoskr-notes",
        "tenant_id": OWNER,
        "aggregate_id": aggregate_id,
        "correlation_id": format!("event:{event_id}"),
        "schema_version": 1,
        "payload": payload,
    }))
    .unwrap_or_default()
}

/// The event id of the `index`th test event.
pub(crate) fn event_id(index: u32) -> String {
    format!("018f0000-0000-7000-8000-{index:012x}")
}

/// A provider that answers `calls` note analyses that cite [`EXCERPT`].
pub(crate) fn analyses(calls: usize) -> Arc<ScriptedProvider> {
    let body = serde_json::json!({
        "summary": "A garden plan for the spring.",
        "topics": ["garden"],
        "evidence_excerpt": EXCERPT,
    })
    .to_string();
    Arc::new(ScriptedProvider::new((0..calls).map(|_| {
        Ok(ProviderResponse {
            bytes: body.as_bytes().to_vec(),
            request_id: Some("scripted-notes".to_owned()),
            usage: ProviderUsage {
                input_tokens: 20,
                output_tokens: 10,
            },
        })
    })))
}
