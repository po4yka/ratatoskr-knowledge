//! The ingest worker against a real `JetStream`: four durable consumers, their dispositions, and
//! the completion publisher (XR-021 CONTRACTS.md S02, S04 and S09).
//!
//! The durable and stream names are compiled constants of the service, so these tests share one
//! disposable broker and run one at a time.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull};
use futures_util::StreamExt as _;
use ratatoskr_ai_archive_contracts::AiArchiveProvenance;
use ratatoskr_document_contracts::{
    ContentDocumentExtracted, Document, DocumentAddress, DocumentBlock,
};
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, Extensions,
    MediaType,
};
use ratatoskr_knowledge::test_support::archive::conversation_from_json;
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, GenerationRequest, LlmProvider, ProviderError, ProviderFailure, ProviderIdentity,
    ProviderResponse, ProviderUsage, ScriptedProvider, SourceInbox,
};
use ratatoskr_knowledge_service::{
    EVENTS_STREAM, IngestFamily, Lifecycle, PUBLISH_READINESS_GRACE, PublishHealth,
    spawn_ingest_workers_with_provider,
};
use ratatoskr_social_contracts::{SocialSourceAnalysisCompleted, SocialSourceSnapshot};
use tokio::sync::watch;

mod ingest_support;

use ingest_support::{TestResult, config, eventually, provision};

/// One broker, one test at a time: the stream and the durables are shared by name.
static BROKER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
const ARCHIVE_ANALYSIS: &str = r#"{"summary":"The user asked for a one-line explanation.","summary_message_ids":["msg-0001"],"decisions":[{"text":"Keep the explanation to one line.","message_id":"msg-0001"}]}"#;
const ARTICLE_ANALYSIS: &str = r#"{"summary":"Quokkas crossed the island in March.","key_points":[{"text":"Quokkas were observed.","source_block_indexes":[0]}]}"#;
const CONTRACT_CONVERSATION: &str = include_str!(
    "../../../crates/knowledge/tests/fixtures/ai_archive.conversation.added.v1/claude-added.json"
);
const CONTRACT_TOMBSTONE: &str = include_str!(
    "../../../crates/knowledge/tests/fixtures/ai_archive.subject.tombstoned.v1/conversation-tombstoned.json"
);

fn nats_url() -> String {
    std::env::var("KNOWLEDGE_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:14223".to_owned())
}

/// Answers each analysis family with its own canned response, whatever the call order.
#[derive(Debug)]
struct RoutedProvider {
    answers: Vec<(&'static str, &'static str)>,
}

impl LlmProvider for RoutedProvider {
    fn identity(&self) -> ProviderIdentity {
        ProviderIdentity {
            provider: "scripted_fake".to_owned(),
            model: "fake_default_v1".to_owned(),
        }
    }

    fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> impl Future<Output = Result<ProviderResponse, ProviderFailure>> + Send {
        let answer = self
            .answers
            .iter()
            .find(|(prompt, _)| *prompt == request.prompt_version)
            .map(|(_, body)| *body);
        std::future::ready(match answer {
            Some(body) => Ok(response(body)),
            None => Err(ProviderFailure::from(ProviderError::Permanent)),
        })
    }
}

fn routed(answers: &[(&'static str, &'static str)]) -> Arc<RoutedProvider> {
    Arc::new(RoutedProvider {
        answers: answers.to_vec(),
    })
}

fn response(body: &str) -> ProviderResponse {
    ProviderResponse {
        bytes: body.as_bytes().to_vec(),
        request_id: Some("scripted-ingest".to_owned()),
        usage: ProviderUsage {
            input_tokens: 20,
            output_tokens: 10,
        },
    }
}

struct Harness {
    database: TestDatabase,
    blobs: BlobStore,
    _root: TemporaryBlobRoot,
    context: jetstream::Context,
    url: String,
}

impl Harness {
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let url = nats_url();
        let context = provision(&url).await?;
        let root = TemporaryBlobRoot::create().await?;
        Ok(Self {
            database: TestDatabase::create().await?,
            blobs: BlobStore::new(root.path(), 1_048_576),
            _root: root,
            context,
            url,
        })
    }

    fn spawn<P: LlmProvider + 'static>(&self, provider: Arc<P>) -> Worker {
        let lifecycle = Lifecycle::starting_with_ingest();
        let (drain, receiver) = watch::channel(false);
        let handle = spawn_ingest_workers_with_provider(
            config(&self.url),
            self.database.database.clone(),
            self.blobs.clone(),
            lifecycle,
            receiver,
            provider,
        );
        Worker { drain, handle }
    }

    async fn publish(
        &self,
        subject: &str,
        event_id: &str,
        bytes: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id);
        self.context
            .publish_with_headers(subject.to_owned(), headers, bytes.into())
            .await?
            .await?;
        Ok(())
    }

    async fn count(&self, sql: &str) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(sql)
            .fetch_one(self.database.database.pool())
            .await
    }

    async fn durable(
        &self,
        family: IngestFamily,
    ) -> Result<(usize, u64, u64), Box<dyn std::error::Error>> {
        let mut consumer: PullConsumer = self
            .context
            .get_consumer_from_stream(family.durable(), EVENTS_STREAM)
            .await?;
        let info = consumer.info().await?;
        Ok((
            info.num_ack_pending,
            info.num_pending,
            info.delivered.consumer_sequence,
        ))
    }

    /// Waits until the durable has delivered `deliveries` messages and holds none open.
    async fn settled(&self, family: IngestFamily, deliveries: u64) -> TestResult {
        eventually("the durable settles", || async {
            let (ack_pending, pending, delivered) = self.durable(family).await?;
            Ok(ack_pending == 0 && pending == 0 && delivered == deliveries)
        })
        .await
    }
}

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

fn envelope(
    event_id: &str,
    event_type: &str,
    producer: &str,
    tenant: Option<&str>,
    payload: &serde_json::Value,
) -> Vec<u8> {
    let mut value = serde_json::json!({
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": "2026-08-27T06:00:00Z",
        "producer": producer,
        "aggregate_id": "social_source:018f0000-0000-7000-8000-000000000201",
        "correlation_id": "operation:018f0000-0000-7000-8000-000000000b01",
        "schema_version": 1,
        "payload": payload,
    });
    if let (Some(tenant), Some(object)) = (tenant, value.as_object_mut()) {
        object.insert("tenant_id".to_owned(), serde_json::json!(tenant));
    }
    serde_json::to_vec(&value).unwrap_or_default()
}

fn social_payload(captured_at: &str) -> Result<serde_json::Value, serde_json::Error> {
    let mut source: serde_json::Value = serde_json::from_str(SOCIAL)?;
    if let Some(object) = source.as_object_mut() {
        object.insert("captured_at".to_owned(), serde_json::json!(captured_at));
    }
    Ok(serde_json::json!({ "source": source }))
}

fn removal_payload(removed_at: &str) -> serde_json::Value {
    serde_json::json!({
        "social_source_id": "018f0000-0000-7000-8000-000000000201",
        "owner": OWNER,
        "reason": "user_requested",
        "removed_at": removed_at,
    })
}

const CAPTURED: &str = "social.source.captured.v1";
const EVENT_A: &str = "018f0000-0000-7000-8000-000000000b11";
const EVENT_B: &str = "018f0000-0000-7000-8000-000000000b12";

fn extracted_document() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&"a".repeat(64))?,
    };
    let payload = ContentDocumentExtracted {
        document: Document {
            document_id: DocumentId::parse("018f0000-0000-7000-8000-000000000a01")?,
            source_address: DocumentAddress::parse("document:quokka-notes")?,
            content_digest: digest.clone(),
            title: Some("Quokka migration notes".to_owned()),
            language: None,
            blocks: vec![DocumentBlock::Paragraph {
                block_id: BlockId::new_v7(),
                text: "Quokkas were observed crossing the island in March.".to_owned(),
            }],
            provenance: Vec::new(),
        },
        document_blob: BlobRef {
            owner_service: BlobOwner::parse("ratatoskr-extractor")?,
            digest,
            media_type: MediaType::parse("application/json")?,
            length_bytes: 256,
        },
        extensions: Extensions::new(),
    };
    Ok(serde_json::to_value(payload)?)
}

async fn search_documents(harness: &Harness) -> Result<i64, sqlx::Error> {
    harness
        .count("select count(*) from knowledge.search_documents")
        .await
}

#[tokio::test]
async fn document_event_becomes_searchable() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(Arc::new(ScriptedProvider::new([Ok(response(
        ARTICLE_ANALYSIS,
    ))])));
    let document = extracted_document()?;
    harness
        .publish(
            "evt.content.document.extracted.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                "content.document.extracted.v1",
                "ratatoskr-extractor",
                Some(OWNER),
                &document,
            ),
        )
        .await?;

    eventually("the document is searchable", || async {
        Ok(search_documents(&harness).await? == 1)
    })
    .await?;
    harness.settled(IngestFamily::Documents, 1).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn social_capture_then_removal() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(routed(&[("social_prompt_v1", SOCIAL_ANALYSIS)]));

    harness
        .publish(
            "evt.social.source.captured.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                CAPTURED,
                "ratatoskr-x",
                Some(OWNER),
                &social_payload("2026-08-17T10:00:00Z")?,
            ),
        )
        .await?;
    eventually("the capture is searchable", || async {
        Ok(search_documents(&harness).await? == 1)
    })
    .await?;
    harness
        .publish(
            "evt.social.source.removed.v1",
            EVENT_B,
            envelope(
                EVENT_B,
                "social.source.removed.v1",
                "ratatoskr-x",
                Some(OWNER),
                &removal_payload("2026-08-18T10:00:00Z"),
            ),
        )
        .await?;
    eventually("the removal deletes the search document", || async {
        Ok(search_documents(&harness).await? == 0)
    })
    .await?;
    assert_eq!(
        harness
            .count("select count(*) from knowledge.social_tombstones")
            .await?,
        1
    );
    harness.settled(IngestFamily::Social, 2).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ai_archive_conversation_is_indexed_and_tombstone_removes_it() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(Arc::new(ScriptedProvider::new([Ok(response(
        ARCHIVE_ANALYSIS,
    ))])));
    let added: serde_json::Value = serde_json::from_str(CONTRACT_CONVERSATION)?;
    let tombstone: serde_json::Value = serde_json::from_str(CONTRACT_TOMBSTONE)?;

    harness
        .publish(
            "evt.ai_archive.conversation.added.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                "ai_archive.conversation.added.v1",
                "ratatoskr-chatgpt",
                Some(OWNER),
                &added,
            ),
        )
        .await?;
    eventually("the conversation is searchable", || async {
        Ok(search_documents(&harness).await? == 1)
    })
    .await?;
    assert_eq!(
        harness
            .count(
                "select count(*) from knowledge.event_outbox
                 where subject = 'knowledge.ai_archive_analysis.completed.v1'"
            )
            .await?,
        1
    );
    harness
        .publish(
            "evt.ai_archive.subject.tombstoned.v1",
            EVENT_B,
            envelope(
                EVENT_B,
                "ai_archive.subject.tombstoned.v1",
                "ratatoskr-chatgpt",
                Some(OWNER),
                &tombstone,
            ),
        )
        .await?;
    eventually("the tombstone deletes the search document", || async {
        Ok(search_documents(&harness).await? == 0)
    })
    .await?;
    harness.settled(IngestFamily::AiArchive, 2).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// Wrong producer, garbage and a missing tenant are terminated on first delivery, never looped.
#[tokio::test]
async fn wrong_producer_and_garbage_are_termed_not_looped() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(routed(&[]));
    let payload = social_payload("2026-08-17T10:00:00Z")?;

    harness
        .publish(
            "evt.social.source.captured.v1",
            EVENT_A,
            envelope(EVENT_A, CAPTURED, "ratatoskr-github", Some(OWNER), &payload),
        )
        .await?;
    harness
        .publish(
            "evt.social.source.captured.v1",
            EVENT_B,
            b"not an envelope".to_vec(),
        )
        .await?;
    harness
        .publish(
            "evt.social.source.captured.v1",
            "018f0000-0000-7000-8000-000000000b13",
            envelope(
                "018f0000-0000-7000-8000-000000000b13",
                CAPTURED,
                "ratatoskr-x",
                None,
                &payload,
            ),
        )
        .await?;

    harness.settled(IngestFamily::Social, 3).await?;
    // A redelivery would arrive after the 2 s nak delay; none may.
    tokio::time::sleep(Duration::from_secs(4)).await;
    harness.settled(IngestFamily::Social, 3).await?;
    assert_eq!(
        harness
            .count("select count(*) from knowledge.source_analysis_inbox")
            .await?,
        0
    );
    assert_eq!(
        harness
            .count("select count(*) from knowledge.ingest_failures")
            .await?,
        0
    );
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A failing analysis is redelivered with a delay; the fifth delivery is terminal and recorded.
#[tokio::test]
async fn transient_failure_is_naked_and_fifth_delivery_is_terminal() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(Arc::new(ScriptedProvider::new([
        Err(ProviderError::Transient),
        Err(ProviderError::Transient),
    ])));
    harness
        .publish(
            "evt.social.source.captured.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                CAPTURED,
                "ratatoskr-x",
                Some(OWNER),
                &social_payload("2026-08-17T10:00:00Z")?,
            ),
        )
        .await?;

    eventually("the fifth delivery is recorded", || async {
        Ok(harness
            .count("select count(*) from knowledge.ingest_failures where delivered_count = 5")
            .await?
            == 1)
    })
    .await?;
    let failure: (String, String) =
        sqlx::query_as("select family, event_id::text from knowledge.ingest_failures")
            .fetch_one(harness.database.database.pool())
            .await?;
    assert_eq!(failure, ("social".to_owned(), EVENT_A.to_owned()));
    tokio::time::sleep(Duration::from_secs(3)).await;
    harness.settled(IngestFamily::Social, 5).await?;
    assert_eq!(search_documents(&harness).await?, 0);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// The process stopped after the inbox commit and before the analysis: the redelivery resumes
/// the analysis for the current head instead of skipping it.
#[tokio::test]
async fn crash_between_inbox_commit_and_analysis_resumes_social_and_archive() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let inbox = SourceInbox::new(&harness.database.database);

    let snapshot: SocialSourceSnapshot =
        serde_json::from_value(social_payload("2026-08-17T10:00:00Z")?["source"].clone())?;
    inbox
        .accept_social(uuid::Uuid::parse_str(EVENT_A)?, CAPTURED, &snapshot)
        .await?;
    let added: serde_json::Value = serde_json::from_str(CONTRACT_CONVERSATION)?;
    let provenance: AiArchiveProvenance =
        serde_json::from_value(added["import_provenance"].clone())?;
    let conversation = conversation_from_json(&added["conversation"].to_string())?;
    inbox
        .accept_ai_conversation(
            uuid::Uuid::parse_str(EVENT_B)?,
            "ai_archive.conversation.added.v1",
            &provenance,
            &conversation,
        )
        .await?;
    assert_eq!(search_documents(&harness).await?, 0);

    let worker = harness.spawn(routed(&[
        ("social_prompt_v1", SOCIAL_ANALYSIS),
        ("archive_prompt_v1", ARCHIVE_ANALYSIS),
    ]));
    harness
        .publish(
            "evt.social.source.captured.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                CAPTURED,
                "ratatoskr-x",
                Some(OWNER),
                &social_payload("2026-08-17T10:00:00Z")?,
            ),
        )
        .await?;
    harness
        .publish(
            "evt.ai_archive.conversation.added.v1",
            EVENT_B,
            envelope(
                EVENT_B,
                "ai_archive.conversation.added.v1",
                "ratatoskr-chatgpt",
                Some(OWNER),
                &added,
            ),
        )
        .await?;
    eventually("both resumed analyses are searchable", || async {
        Ok(search_documents(&harness).await? == 2)
    })
    .await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

async fn completion_messages(
    harness: &Harness,
) -> Result<Vec<(EventEnvelope, Option<String>)>, Box<dyn std::error::Error>> {
    let stream = harness.context.get_stream(EVENTS_STREAM).await?;
    let consumer = stream
        .create_consumer(pull::Config {
            filter_subject: "evt.knowledge.analysis.completed.v1".to_owned(),
            ack_policy: AckPolicy::None,
            deliver_policy: DeliverPolicy::All,
            ..pull::Config::default()
        })
        .await?;
    let mut batch = consumer
        .fetch()
        .max_messages(10)
        .expires(Duration::from_secs(1))
        .messages()
        .await?;
    let mut messages = Vec::new();
    while let Some(message) = batch.next().await {
        let message = message.map_err(|error| error.to_string())?;
        let id = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get("Nats-Msg-Id"))
            .map(|value| value.as_str().to_owned());
        messages.push((EventEnvelope::from_json(&message.payload)?, id));
    }
    Ok(messages)
}

/// The fact is published once with a typed envelope, and a worker restart after a crash between
/// the broker's acknowledgement and the `published_at` update does not publish it twice.
#[tokio::test]
async fn completion_facts_reach_the_event_stream_once() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(routed(&[("social_prompt_v1", SOCIAL_ANALYSIS)]));
    harness
        .publish(
            "evt.social.source.captured.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                CAPTURED,
                "ratatoskr-x",
                Some(OWNER),
                &social_payload("2026-08-17T10:00:00Z")?,
            ),
        )
        .await?;

    eventually("the completion fact is published", || async {
        Ok(harness
            .count("select count(*) from knowledge.event_outbox where published_at is not null")
            .await?
            == 1)
    })
    .await?;
    let messages = completion_messages(&harness).await?;
    assert_eq!(messages.len(), 1, "exactly one broker message");
    let (published, message_id) = messages.first().ok_or("no completion message")?;
    assert_eq!(published.producer.as_str(), "ratatoskr-knowledge");
    assert_eq!(
        published
            .tenant_id
            .map(|tenant| tenant.to_string())
            .as_deref(),
        Some(OWNER)
    );
    assert_eq!(
        published
            .causation_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some(format!("event:{EVENT_A}").as_str())
    );
    assert_eq!(
        published.correlation_id.to_string(),
        "operation:018f0000-0000-7000-8000-000000000b01"
    );
    assert_eq!(
        message_id.as_deref(),
        Some(published.event_id.0.to_string().as_str())
    );
    let completed = published.payload_as::<SocialSourceAnalysisCompleted>()?;
    assert_eq!(completed.owner.to_string(), OWNER);
    worker.stop().await?;

    // Crash between the PubAck and the published_at update, then restart.
    sqlx::query("update knowledge.event_outbox set published_at = null")
        .execute(harness.database.database.pool())
        .await?;
    let restarted = harness.spawn(routed(&[]));
    eventually(
        "the restarted worker marks the fact published again",
        || async {
            Ok(harness
                .count("select count(*) from knowledge.event_outbox where published_at is not null")
                .await?
                == 1)
        },
    )
    .await?;
    assert_eq!(
        completion_messages(&harness).await?.len(),
        1,
        "the broker drops the republish by message id"
    );
    restarted.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[test]
fn publisher_failure_only_matters_after_the_grace_period() {
    let start = Instant::now();
    let mut health = PublishHealth::default();
    assert!(health.healthy(start, PUBLISH_READINESS_GRACE));
    health.record(false, start);
    assert!(health.healthy(start + PUBLISH_READINESS_GRACE, PUBLISH_READINESS_GRACE));
    assert!(!health.healthy(
        start + PUBLISH_READINESS_GRACE + Duration::from_secs(1),
        PUBLISH_READINESS_GRACE
    ));
    health.record(false, start + Duration::from_secs(40));
    assert!(
        !health.healthy(start + Duration::from_secs(41), PUBLISH_READINESS_GRACE),
        "a repeated failure does not restart the clock"
    );
    health.record(true, start + Duration::from_secs(42));
    assert!(health.healthy(start + Duration::from_secs(43), PUBLISH_READINESS_GRACE));
}
