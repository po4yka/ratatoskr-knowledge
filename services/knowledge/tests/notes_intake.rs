//! Note file changes against a real `JetStream` and database (changeset OBS-023, workspace change
//! `add-notes-vault-connections`, spec `notes-analysis-intake`).
//!
//! The two notes durables are provisioned here as Platform provisions them. The note bytes are
//! written into a disposable notes blob root in the layout of `ratatoskr-notes`.

mod ingest_support;
#[path = "log_capture/mod.rs"]
mod log_capture;

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::{
    self,
    consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull},
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, ChunkPolicy, Config, EMBEDDING_STORAGE_DIMENSIONS, Indexer, IndexerLimits,
    ProviderResponse, ProviderUsage, ScriptedEmbeddingProvider, ScriptedEmbeddingSuccess,
    ScriptedProvider, SearchQuery, search_page,
};
use ratatoskr_knowledge_service::{
    EVENTS_STREAM, Lifecycle, Metrics, Redelivery, RedeliverySchedule,
    spawn_ingest_workers_with_schedule,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::watch;

use crate::ingest_support::{
    PLATFORM_ACK_WAIT, TestResult, config, eventually, provision_with_ack_wait,
};
use crate::log_capture::capture;

const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";
const CONNECTION: &str = "018f0000-0000-7000-8000-00000000c001";
const FILE: &str = "018f0000-0000-7000-8000-00000000f001";
const FILES_DURABLE: &str = "ratatoskr_knowledge_notes_files";
const FILES_SUBJECT: &str = "evt.notes.file.changed.v1";
const VAULTS_DURABLE: &str = "ratatoskr_knowledge_notes_vaults";
const VAULTS_FILTER: &str = "evt.notes.vault.>";
const NOTE_TEXT: &str = "# Garden\n\nThe garden plan starts in spring.\n";
const EXCERPT: &str = "The garden plan starts in spring.";

/// A redelivery schedule short enough that a negative acknowledgement comes back inside a test.
const FAST_REDELIVERY: RedeliverySchedule =
    RedeliverySchedule::new(Duration::from_millis(20), Duration::from_millis(100));

/// One broker, one test at a time: the stream and the durables are shared by name.
static BROKER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn nats_url() -> String {
    std::env::var("KNOWLEDGE_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:14223".to_owned())
}

/// A disposable database, a Knowledge blob root, a notes blob root and the shared broker.
struct Setup {
    database: TestDatabase,
    blobs: BlobStore,
    _knowledge_root: TemporaryBlobRoot,
    notes_root: TemporaryBlobRoot,
    context: jetstream::Context,
    url: String,
    metrics: Arc<Metrics>,
}

/// A running ingest worker.
struct Worker {
    drain: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
}

impl Worker {
    async fn stop(self) -> TestResult {
        let _ignored = self.drain.send(true);
        tokio::time::timeout(Duration::from_secs(10), self.handle).await??;
        Ok(())
    }
}

impl Setup {
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
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
    fn config(&self) -> Config {
        let mut config = config(&self.url);
        config.blobs.notes_root = Some(self.notes_root.path().to_path_buf());
        config
    }

    fn spawn_with(&self, config: Config, provider: Arc<ScriptedProvider>) -> Worker {
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

    fn spawn(&self, provider: Arc<ScriptedProvider>) -> Worker {
        self.spawn_with(self.config(), provider)
    }

    /// Writes `bytes` into the notes blob root as `ratatoskr-notes` does and returns the revision.
    fn store_note(&self, bytes: &[u8]) -> Result<String, std::io::Error> {
        let revision = format!("{:x}", Sha256::digest(bytes));
        let (prefix, rest) = revision.split_at(2);
        let directory = self.notes_root.path().join("sha256").join(prefix);
        std::fs::create_dir_all(&directory)?;
        std::fs::write(directory.join(rest), bytes)?;
        Ok(revision)
    }

    async fn publish(&self, subject: &str, message_id: &str, bytes: Vec<u8>) -> TestResult {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", message_id);
        self.context
            .publish_with_headers(subject.to_owned(), headers, bytes.into())
            .await?
            .await?;
        Ok(())
    }

    /// `(ack pending, pending, delivered)` of one durable.
    async fn durable(&self, name: &str) -> Result<(usize, u64, u64), Box<dyn std::error::Error>> {
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
    async fn settle(
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

    async fn count(&self, sql: &str) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(sql)
            .fetch_one(self.database.database.pool())
            .await
    }

    /// The analysis runs of one file revision.
    async fn runs_for(&self, file_id: &str, revision: &str) -> Result<i64, sqlx::Error> {
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

    async fn finish(self, worker: Worker) -> TestResult {
        worker.stop().await?;
        self.database.cleanup().await?;
        Ok(())
    }
}

/// One file change as `ratatoskr-notes` publishes it.
struct Change<'a> {
    file_id: &'a str,
    sequence: u64,
    kind: &'a str,
    path: &'a str,
    revision: &'a str,
    media_type: &'a str,
    length: usize,
}

impl Change<'_> {
    fn payload(&self) -> serde_json::Value {
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

    fn envelope(&self, event_id: &str) -> Vec<u8> {
        envelope(
            event_id,
            "notes.file.changed.v1",
            &format!("notes_file:{}", self.file_id),
            &self.payload(),
        )
    }
}

fn envelope(
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
fn event_id(index: u32) -> String {
    format!("018f0000-0000-7000-8000-{index:012x}")
}

/// A provider that answers `calls` note analyses that cite [`EXCERPT`].
fn analyses(calls: usize) -> Arc<ScriptedProvider> {
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

#[tokio::test]
async fn one_run_per_file_revision() -> TestResult {
    let _serial = BROKER.lock().await;
    let notes = Setup::start().await?;
    let revision = notes.store_note(NOTE_TEXT.as_bytes())?;
    let provider = analyses(1);
    let worker = notes.spawn(Arc::clone(&provider));
    let created = Change {
        file_id: FILE,
        sequence: 1,
        kind: "created",
        path: "Projects/Garden.md",
        revision: &revision,
        media_type: "text/markdown",
        length: NOTE_TEXT.len(),
    };
    let event = event_id(1);
    // The same event three times, under three broker message ids: one delivery and two
    // redeliveries of it, as Knowledge sees them.
    for copy in 0..3 {
        notes
            .publish(
                FILES_SUBJECT,
                &format!("{event}-{copy}"),
                created.envelope(&event),
            )
            .await?;
    }

    assert_eq!(
        notes.settle(FILES_DURABLE, 3).await?,
        (0, 0, 3),
        "the notes files durable delivered every copy and holds none open"
    );
    assert_eq!(
        notes.runs_for(FILE, &revision).await?,
        1,
        "one analysis run for the file revision"
    );
    assert_eq!(provider.call_count()?, 1, "the note is analysed once");
    notes.finish(worker).await
}

#[tokio::test]
async fn non_markdown_file_creates_no_run() -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let notes = Setup::start().await?;
    let image = b"\x89PNG\r\n\x1a\n not really an image";
    let revision = notes.store_note(image)?;
    let worker = notes.spawn(analyses(0));
    let created = Change {
        file_id: FILE,
        sequence: 1,
        kind: "created",
        path: "Attachments/photo.png",
        revision: &revision,
        media_type: "image/png",
        length: image.len(),
    };
    notes
        .publish(FILES_SUBJECT, &event_id(2), created.envelope(&event_id(2)))
        .await?;

    assert_eq!(
        notes.settle(FILES_DURABLE, 1).await?,
        (0, 0, 1),
        "the image change is delivered once and acknowledged"
    );
    assert_eq!(
        notes
            .count("select count(*) from knowledge.analysis_runs")
            .await?,
        0,
        "no analysis run for an image"
    );
    assert_eq!(
        logs.lines_containing("ingest delivery"),
        0,
        "the image change is neither rejected nor redelivered: {}",
        logs.text()
    );
    notes.finish(worker).await
}

/// The file id of the `index`th test file.
fn file_id(index: u32) -> String {
    format!("018f0000-0000-7000-8000-00000001{index:04x}")
}

/// Embeds every persisted analysis, as the background indexer does.
async fn index_embeddings(setup: &Setup) -> TestResult {
    let indexer = Indexer::new(
        &setup.database.database,
        ScriptedEmbeddingProvider::new(
            u16::try_from(EMBEDDING_STORAGE_DIMENSIONS)?,
            (0..64).map(|_| Ok(ScriptedEmbeddingSuccess { input_tokens: 12 })),
        ),
        ChunkPolicy::new(1_600, 200)?,
        IndexerLimits {
            batch_sources: 8,
            max_input_characters: 120_000,
            max_failure_attempts: 5,
        },
    );
    for _pass in 0..10 {
        if indexer.process_pending().await?.indexed == 0 {
            break;
        }
    }
    Ok(())
}

/// The owner's search results that are notes, browsing every document.
async fn note_search_results(setup: &Setup) -> Result<usize, Box<dyn std::error::Error>> {
    let page = search_page(
        setup.database.database.pool(),
        &SearchQuery::new(OWNER, "", 100, 0)?,
    )
    .await?;
    let lexical = search_page(
        setup.database.database.pool(),
        &SearchQuery::new(OWNER, "garden", 100, 0)?,
    )
    .await?;
    Ok(page
        .results
        .iter()
        .chain(&lexical.results)
        .filter(|result| result.owner_context == "ratatoskr-notes")
        .count())
}

#[tokio::test]
async fn disconnect_removes_all_derived_data() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    let worker = setup.spawn(analyses(30));
    for index in 1..=30 {
        let text = format!("# Note {index}\n\n{EXCERPT}\n");
        let revision = setup.store_note(text.as_bytes())?;
        let file = file_id(index);
        let path = format!("Daily/Note {index}.md");
        let created = Change {
            file_id: &file,
            sequence: u64::from(index),
            kind: "created",
            path: &path,
            revision: &revision,
            media_type: "text/markdown",
            length: text.len(),
        };
        setup
            .publish(
                FILES_SUBJECT,
                &event_id(100 + index),
                created.envelope(&event_id(100 + index)),
            )
            .await?;
    }
    assert_eq!(setup.settle(FILES_DURABLE, 30).await?, (0, 0, 30));
    index_embeddings(&setup).await?;
    let embeddings = "select count(*) from knowledge.embedding_chunks
                      where owner_context = 'ratatoskr-notes'";
    assert_eq!(
        note_search_results(&setup).await?,
        60,
        "30 notes are browsed and found"
    );
    assert!(
        setup.count(embeddings).await? >= 30,
        "every note is embedded"
    );

    setup
        .publish(
            "evt.notes.vault.disconnected.v1",
            &event_id(200),
            envelope(
                &event_id(200),
                "notes.vault.disconnected.v1",
                &format!("notes_connection:{CONNECTION}"),
                &serde_json::json!({
                    "connection_id": CONNECTION,
                    "channel": "plugin",
                    "vault_display_name": "Personal",
                }),
            ),
        )
        .await?;
    assert_eq!(setup.settle(VAULTS_DURABLE, 1).await?, (0, 0, 1));
    assert_eq!(
        note_search_results(&setup).await?,
        0,
        "no search result of the removed connection remains"
    );
    assert_eq!(
        setup.count(embeddings).await?,
        0,
        "no embedding of the removed connection remains"
    );
    setup.finish(worker).await
}

#[tokio::test]
async fn older_event_after_delete_creates_nothing() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    let first = NOTE_TEXT;
    let second = format!("{NOTE_TEXT}\nA second line.\n");
    let first_revision = setup.store_note(first.as_bytes())?;
    let second_revision = setup.store_note(second.as_bytes())?;
    let worker = setup.spawn(analyses(2));
    let change = |sequence, kind, revision, length| Change {
        file_id: FILE,
        sequence,
        kind,
        path: "Projects/Garden.md",
        revision,
        media_type: "text/markdown",
        length,
    };
    let steps = [
        change(1, "created", &first_revision, first.len()),
        change(3, "deleted", &first_revision, first.len()),
        change(2, "updated", &second_revision, second.len()),
    ];
    for (delivered, step) in (1..).zip(&steps) {
        let event = event_id(300 + delivered);
        setup
            .publish(FILES_SUBJECT, &event, step.envelope(&event))
            .await?;
        assert_eq!(
            setup.settle(FILES_DURABLE, u64::from(delivered)).await?,
            (0, 0, u64::from(delivered))
        );
    }
    assert_eq!(
        setup.runs_for(FILE, &second_revision).await?,
        0,
        "no run for the older update"
    );
    assert_eq!(
        setup
            .count(
                "select count(*) from knowledge.search_documents
                 where owner_context = 'ratatoskr-notes'"
            )
            .await?,
        0,
        "no search entry for the deleted file"
    );
    setup.finish(worker).await
}

/// The configuration key of the notes blob root.
const NOTES_ROOT_KEY: &str = "RATATOSKR__BLOBS__NOTES_ROOT";

/// The value of the label-free redelivery counter on the operator plane.
fn redeliveries(metrics: &Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix("knowledge_ingest_redeliveries_total "))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or_default()
}

#[tokio::test]
async fn missing_blob_is_transient() -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let setup = Setup::start().await?;
    let root = setup.notes_root.path().display().to_string();
    let config = Config::from_environment([
        ("RATATOSKR__INGEST__ENABLED", "true"),
        ("RATATOSKR__INGEST__BUS_ENDPOINT", setup.url.as_str()),
        ("RATATOSKR__INGEST__PROVIDER_MODE", "fixture"),
        (
            "RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET",
            "readme-secret",
        ),
        ("RATATOSKR__LIMITS__PROVIDER_TIMEOUT_MS", "5000"),
        (NOTES_ROOT_KEY, root.as_str()),
    ]);
    assert!(
        config.is_ok(),
        "the notes blob root is configured through {NOTES_ROOT_KEY}: {:?}",
        config.as_ref().err()
    );
    let worker = setup.spawn_with(config?, analyses(1));
    // A revision that ratatoskr-notes has not written to its root.
    let revision = "2".repeat(64);
    let created = Change {
        file_id: FILE,
        sequence: 1,
        kind: "created",
        path: "Projects/Garden.md",
        revision: &revision,
        media_type: "text/markdown",
        length: NOTE_TEXT.len(),
    };
    setup
        .publish(
            FILES_SUBJECT,
            &event_id(400),
            created.envelope(&event_id(400)),
        )
        .await?;

    let _timed_out = eventually("the change is redelivered", || async {
        Ok(setup.durable(FILES_DURABLE).await?.2 >= 3)
    })
    .await;
    let delivered = setup.durable(FILES_DURABLE).await?.2;
    assert!(
        delivered >= 3,
        "the change is negatively acknowledged and redelivered: {delivered} deliveries"
    );
    assert!(
        redeliveries(&setup.metrics) >= 2,
        "each redelivery waits the transient delay of the schedule"
    );
    assert_eq!(
        setup.runs_for(FILE, &revision).await?,
        0,
        "no run without the bytes"
    );
    assert!(
        logs.lines_containing("notes_blob") >= 1,
        "the redelivery is logged with the class notes_blob: {}",
        logs.text()
    );
    assert_eq!(
        logs.lines_containing("ingest delivery rejected"),
        0,
        "a missing blob is never terminal"
    );
    setup.finish(worker).await
}
