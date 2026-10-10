//! A disposable database, blob root and the shared broker, plus a running ingest worker.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull};
use futures_util::StreamExt as _;
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{BlobStore, Config, LlmProvider};
use ratatoskr_knowledge_service::{
    EVENTS_STREAM, IngestFamily, Lifecycle, spawn_ingest_workers_with_provider,
};
use tokio::sync::watch;

use crate::ingest_support::{TestResult, config, eventually, provision};

/// One broker, one test at a time: the stream and the durables are shared by name.
pub(crate) static BROKER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn nats_url() -> String {
    std::env::var("KNOWLEDGE_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:14223".to_owned())
}

pub(crate) struct Harness {
    pub(crate) database: TestDatabase,
    blobs: BlobStore,
    _root: TemporaryBlobRoot,
    context: jetstream::Context,
    url: String,
}

impl Harness {
    pub(crate) async fn start() -> Result<Self, Box<dyn std::error::Error>> {
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

    /// The worker configuration for this broker, for a test that adjusts it before spawning.
    pub(crate) fn config(&self) -> Config {
        config(&self.url)
    }

    pub(crate) fn spawn<P: LlmProvider + 'static>(&self, provider: Arc<P>) -> Worker {
        self.spawn_with(self.config(), provider)
    }

    pub(crate) fn spawn_with<P: LlmProvider + 'static>(
        &self,
        config: Config,
        provider: Arc<P>,
    ) -> Worker {
        let lifecycle = Lifecycle::starting_with_ingest();
        let (drain, receiver) = watch::channel(false);
        let handle = spawn_ingest_workers_with_provider(
            config,
            self.database.database.clone(),
            self.blobs.clone(),
            lifecycle,
            receiver,
            provider,
        );
        Worker { drain, handle }
    }

    pub(crate) async fn publish(
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

    pub(crate) async fn count(&self, sql: &str) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(sql)
            .fetch_one(self.database.database.pool())
            .await
    }

    pub(crate) async fn search_documents(&self) -> Result<i64, sqlx::Error> {
        self.count("select count(*) from knowledge.search_documents")
            .await
    }

    /// The number of rows across `tables`, each a `knowledge` table name.
    pub(crate) async fn rows_in(&self, tables: &[&str]) -> Result<i64, sqlx::Error> {
        let mut total = 0;
        for table in tables {
            total += self
                .count(&format!("select count(*) from knowledge.{table}"))
                .await?;
        }
        Ok(total)
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
    pub(crate) async fn settled(&self, family: IngestFamily, deliveries: u64) -> TestResult {
        eventually("the durable settles", || async {
            let (ack_pending, pending, delivered) = self.durable(family).await?;
            Ok(ack_pending == 0 && pending == 0 && delivered == deliveries)
        })
        .await
    }

    /// The Knowledge facts on `subject` that reached the events stream, with their message ids.
    pub(crate) async fn facts_on(
        &self,
        subject: &str,
    ) -> Result<Vec<(EventEnvelope, Option<String>)>, Box<dyn std::error::Error>> {
        let stream = self.context.get_stream(EVENTS_STREAM).await?;
        let consumer = stream
            .create_consumer(pull::Config {
                filter_subject: subject.to_owned(),
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
}

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
