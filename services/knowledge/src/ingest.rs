//! Supervised `JetStream` intake of the four ingest durables and publication of Knowledge's
//! completion facts (XR-021 CONTRACTS.md S02, S04 and S09).
//!
//! One consumer loop per durable, each verifying its Edge-provisioned durable and never creating
//! one, plus one publisher loop for the transactional outbox. A loop that loses its broker
//! reconnects and keeps readiness false meanwhile; a task that ends for any other reason before an
//! orderly shutdown fails the worker, which the process turns into a non-zero exit.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_nats::connection::State;
use async_nats::jetstream::{self, consumer::PullConsumer};
use futures_util::StreamExt as _;
use ratatoskr_knowledge::{
    BlobStore, Config, Database, GithubReadmeClient, GithubReadmeClientSettings, LlmProvider,
    RateLimiter,
};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::bus::{self, BusFailure, DurableSpec};
use crate::ingest_handlers::Outcome;
use crate::ingest_publish::publish_event_outbox;
use crate::provider_runtime::IngestProvider;
use crate::redelivery::{Redelivery, RedeliverySchedule};
use crate::{Lifecycle, Metrics};

/// The one events stream every ingest durable belongs to.
pub const EVENTS_STREAM: &str = "ratatoskr_events";

/// The delivery on which the repository family settles a failed fact instead of retrying a plain
/// transient fault.
///
/// Only the repository family has a delivery budget: its terminal delivery ends in a
/// `knowledge.repository_analysis.failed.v1` fact that the requester waits for. The budget covers
/// plain transient faults only (a transient provider failure, a timeout, a held run, an invalid
/// response, an unavailable README). A rejected credential, an empty balance, a withdrawn model
/// and a spend-budget refusal are redelivered without a budget, like every other family, which is
/// redelivered until it succeeds or turns out never to be able to (XR-021 CONTRACTS.md R4-05 and
/// R6-01).
pub const TERMINAL_DELIVERY: i64 = 5;

/// How long the completion publisher may keep failing before the worker stops being ready.
pub const PUBLISH_READINESS_GRACE: Duration = Duration::from_secs(30);

/// One of the four ingest durables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestFamily {
    /// Extracted documents.
    Documents,
    /// Social source captures, updates and removals.
    Social,
    /// AI archive lifecycle facts and tombstones.
    AiArchive,
    /// Repository analysis requests.
    Repository,
}

impl IngestFamily {
    /// The four families, in durable-table order.
    pub const ALL: [Self; 4] = [
        Self::Documents,
        Self::Social,
        Self::AiArchive,
        Self::Repository,
    ];

    /// The Edge-provisioned durable name.
    #[must_use]
    pub const fn durable(self) -> &'static str {
        match self {
            Self::Documents => "ratatoskr_knowledge_documents",
            Self::Social => "ratatoskr_knowledge_social_sources",
            Self::AiArchive => "ratatoskr_knowledge_ai_archive",
            Self::Repository => "ratatoskr_knowledge_repository_requests",
        }
    }

    /// The durable's filter subject.
    #[must_use]
    pub const fn filter(self) -> &'static str {
        match self {
            Self::Documents => "evt.content.document.extracted.v1",
            Self::Social => "evt.social.source.>",
            Self::AiArchive => "evt.ai_archive.>",
            Self::Repository => "evt.knowledge.repository_analysis.requested.v1",
        }
    }

    /// The family spelling logged beside a rejected or redelivered delivery.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Documents => "documents",
            Self::Social => "social",
            Self::AiArchive => "ai_archive",
            Self::Repository => "repository",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Documents => 0,
            Self::Social => 1,
            Self::AiArchive => 2,
            Self::Repository => 3,
        }
    }
}

/// Tracks how long the completion publisher has been failing.
///
/// Pure bookkeeping over explicit instants, so the readiness rule is testable without a clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct PublishHealth {
    failing_since: Option<Instant>,
}

impl PublishHealth {
    /// Records the outcome of one publish attempt at `now`.
    pub fn record(&mut self, succeeded: bool, now: Instant) {
        if succeeded {
            self.failing_since = None;
        } else if self.failing_since.is_none() {
            self.failing_since = Some(now);
        }
    }

    /// Whether the publisher has not been failing for longer than `grace` at `now`.
    #[must_use]
    pub fn healthy(&self, now: Instant, grace: Duration) -> bool {
        self.failing_since
            .is_none_or(|since| now.saturating_duration_since(since) <= grace)
    }
}

/// Readiness of the whole ingest worker.
#[derive(Debug)]
struct IngestHealth {
    lifecycle: Lifecycle,
    families: [AtomicBool; 4],
    publisher_healthy: AtomicBool,
    failed: AtomicBool,
    guard: Mutex<()>,
}

impl IngestHealth {
    fn new(lifecycle: Lifecycle) -> Self {
        Self {
            lifecycle,
            families: [
                AtomicBool::new(false),
                AtomicBool::new(false),
                AtomicBool::new(false),
                AtomicBool::new(false),
            ],
            publisher_healthy: AtomicBool::new(true),
            failed: AtomicBool::new(false),
            guard: Mutex::new(()),
        }
    }

    fn set_family(&self, family: IngestFamily, ready: bool) {
        if let Some(flag) = self.families.get(family.index()) {
            flag.store(ready, Ordering::Release);
        }
        self.refresh();
    }

    fn set_publisher(&self, healthy: bool) {
        self.publisher_healthy.store(healthy, Ordering::Release);
        self.refresh();
    }

    fn fail(&self) {
        self.failed.store(true, Ordering::Release);
        self.refresh();
    }

    fn refresh(&self) {
        let _serialized = self.guard.lock();
        let ready = !self.failed.load(Ordering::Acquire)
            && self.publisher_healthy.load(Ordering::Acquire)
            && self
                .families
                .iter()
                .all(|family| family.load(Ordering::Acquire));
        self.lifecycle.set_ingest_ready(ready);
    }
}

/// Everything the handlers need, shared by the four consumer loops and the publisher.
#[derive(Debug)]
pub(crate) struct IngestContext<P> {
    pub(crate) config: Config,
    pub(crate) database: Database,
    pub(crate) blobs: BlobStore,
    pub(crate) provider: Arc<P>,
    pub(crate) readme: GithubReadmeClient,
    pub(crate) redelivery: Redelivery,
}

/// Spawns the ingest supervisor with the provider the configuration selects.
///
/// `limiter` is the process-wide inference limiter, shared with the recap worker. The caller must
/// signal drain and join the returned handle. A handle that finishes before drain means the worker
/// failed and the process must exit non-zero.
#[must_use]
pub fn spawn_ingest_workers(
    config: Config,
    database: Database,
    blobs: BlobStore,
    lifecycle: Lifecycle,
    drain: watch::Receiver<bool>,
    limiter: Arc<RateLimiter>,
    metrics: Arc<Metrics>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match IngestProvider::from_config(&config, &database, limiter) {
            Ok(provider) => {
                supervise(
                    config,
                    database,
                    blobs,
                    lifecycle,
                    drain,
                    Arc::new(provider),
                    Redelivery::standard(metrics),
                )
                .await;
            }
            Err(error) => tracing::error!(class = "provider_configuration", %error),
        }
    })
}

/// Spawns the ingest supervisor with an explicit provider and the production redelivery schedule.
///
/// This is the seam the tests use to run the real consumers against a scripted provider.
#[must_use]
pub fn spawn_ingest_workers_with_provider<P: LlmProvider + 'static>(
    config: Config,
    database: Database,
    blobs: BlobStore,
    lifecycle: Lifecycle,
    drain: watch::Receiver<bool>,
    provider: Arc<P>,
    metrics: Arc<Metrics>,
) -> tokio::task::JoinHandle<()> {
    spawn_ingest_workers_with_schedule(
        config,
        database,
        blobs,
        lifecycle,
        drain,
        provider,
        Redelivery::standard(metrics),
    )
}

/// Spawns the ingest supervisor with an explicit provider and an explicit redelivery schedule.
///
/// The schedule is an argument and never a configuration key, so a test can redeliver eight times
/// inside its deadline while a deployment cannot lower the production delay.
#[must_use]
pub fn spawn_ingest_workers_with_schedule<P: LlmProvider + 'static>(
    config: Config,
    database: Database,
    blobs: BlobStore,
    lifecycle: Lifecycle,
    drain: watch::Receiver<bool>,
    provider: Arc<P>,
    redelivery: Redelivery,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(supervise(
        config, database, blobs, lifecycle, drain, provider, redelivery,
    ))
}

/// The README source cannot be built from the configuration.
#[derive(Debug, Clone, Copy)]
struct ReadmeUnavailable;

fn readme_client(config: &Config) -> Result<GithubReadmeClient, ReadmeUnavailable> {
    let secret = config
        .ingest
        .github_readme_service_secret
        .clone()
        .ok_or(ReadmeUnavailable)?;
    let deadline = Duration::from_millis(config.limits.provider_timeout_ms);
    GithubReadmeClient::new(GithubReadmeClientSettings {
        base_url: config.ingest.github_readme_base_url.clone(),
        service_secret: secret,
        connect_timeout: deadline.min(Duration::from_secs(5)),
        request_deadline: deadline,
    })
    .map_err(|_| ReadmeUnavailable)
}

async fn supervise<P: LlmProvider + 'static>(
    config: Config,
    database: Database,
    blobs: BlobStore,
    lifecycle: Lifecycle,
    mut drain: watch::Receiver<bool>,
    provider: Arc<P>,
    redelivery: Redelivery,
) {
    let Ok(readme) = readme_client(&config) else {
        tracing::error!(class = "readme_configuration");
        return;
    };
    let health = Arc::new(IngestHealth::new(lifecycle));
    let context = Arc::new(IngestContext {
        config,
        database,
        blobs,
        provider,
        readme,
        redelivery,
    });
    let mut tasks = JoinSet::new();
    for family in IngestFamily::ALL {
        tasks.spawn(consume_family(
            Arc::clone(&context),
            family,
            Arc::clone(&health),
            drain.clone(),
        ));
    }
    tasks.spawn(publish_loop(
        Arc::clone(&context),
        Arc::clone(&health),
        drain.clone(),
    ));
    // cancel-safe: watch::Receiver::changed and JoinSet::join_next hold no partial work.
    tokio::select! {
        biased;
        _ = drain.changed() => {}
        finished = tasks.join_next() => {
            if finished.is_some() {
                tracing::error!(class = "ingest_task_exited");
                health.fail();
                tasks.abort_all();
                return;
            }
        }
    }
    while tasks.join_next().await.is_some() {}
}

async fn consume_family<P: LlmProvider>(
    context: Arc<IngestContext<P>>,
    family: IngestFamily,
    health: Arc<IngestHealth>,
    mut drain: watch::Receiver<bool>,
) {
    while !*drain.borrow() {
        let result = consume_once(&context, family, &health, &mut drain).await;
        health.set_family(family, false);
        if *drain.borrow() {
            break;
        }
        tracing::warn!(
            family = family.label(),
            class = match result {
                Ok(()) => "consumer_stopped",
                Err(BusFailure::Unavailable) => "bus_unavailable",
                Err(BusFailure::Durable) => "durable_mismatch",
            },
            "ingest consumer is not ready"
        );
        // cancel-safe: watch::Receiver::changed and sleep retain no partial work.
        tokio::select! {
            biased;
            _ = drain.changed() => {}
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn consume_once<P: LlmProvider>(
    context: &IngestContext<P>,
    family: IngestFamily,
    health: &IngestHealth,
    drain: &mut watch::Receiver<bool>,
) -> Result<(), BusFailure> {
    let ingest = &context.config.ingest;
    let client = bus::connect(&ingest.bus_endpoint, ingest.bus_credentials_file.as_deref()).await?;
    let jetstream = jetstream::new(client.clone());
    let consumer: PullConsumer = jetstream
        .get_consumer_from_stream(family.durable(), EVENTS_STREAM)
        .await
        .map_err(|_| BusFailure::Unavailable)?;
    bus::verify_consumer(
        &consumer,
        &DurableSpec {
            name: family.durable(),
            filter: family.filter(),
            ack_wait: Duration::from_secs(ingest.ack_wait_seconds),
            max_deliver: Some(-1),
        },
    )?;
    let mut messages = consumer
        .stream()
        .max_messages_per_batch(usize::try_from(ingest.fetch_batch).unwrap_or(16))
        .messages()
        .await
        .map_err(|_| BusFailure::Unavailable)?;
    health.set_family(family, true);
    let mut watchdog = tokio::time::interval(Duration::from_secs(1));
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        // cancel-safe: watch::changed, Interval::tick and StreamExt::next do not commit work; a
        // handler runs to completion inside the arm before the next iteration observes drain.
        tokio::select! {
            biased;
            _ = drain.changed() => return Ok(()),
            _ = watchdog.tick() => {
                // A subscription can sit silent for a long while after the broker is gone, so
                // readiness follows the connection itself.
                if client.connection_state() != State::Connected {
                    return Err(BusFailure::Unavailable);
                }
            }
            next = messages.next() => {
                let Some(next) = next else {
                    return Err(BusFailure::Unavailable);
                };
                let message = next.map_err(|_| BusFailure::Unavailable)?;
                let delivered = message.info().map_or(1, |info| info.delivered);
                let outcome = context.handle(family, message.payload.as_ref(), delivered).await;
                let ack = context.disposition(family, &outcome, delivered);
                message.ack_with(ack).await.map_err(|_| BusFailure::Unavailable)?;
            }
        }
    }
}

impl<P: LlmProvider> IngestContext<P> {
    async fn handle(&self, family: IngestFamily, bytes: &[u8], delivered: i64) -> Outcome {
        match family {
            IngestFamily::Documents => self.documents(bytes).await,
            IngestFamily::Social => self.social(bytes).await,
            IngestFamily::AiArchive => self.ai_archive(bytes).await,
            IngestFamily::Repository => self.repository(bytes, delivered).await,
        }
    }

    /// Maps a handler outcome to the broker disposition and makes a redelivery visible.
    ///
    /// A redelivery is counted on the operator plane, and from the second delivery on it is logged
    /// with the family, the class and the delivery count, never content (XR-021 CONTRACTS.md
    /// R4-05 rule 3).
    fn disposition(
        &self,
        family: IngestFamily,
        outcome: &Outcome,
        delivered: i64,
    ) -> jetstream::AckKind {
        match outcome {
            Outcome::Done => {}
            Outcome::Reject(class) => {
                tracing::warn!(family = family.label(), class, "ingest delivery rejected");
            }
            Outcome::Retry(class) => {
                self.redelivery.metrics.record_ingest_redelivery();
                if delivered >= 2 {
                    tracing::warn!(
                        family = family.label(),
                        class,
                        delivered,
                        "ingest delivery failed and will be redelivered"
                    );
                } else {
                    tracing::debug!(
                        family = family.label(),
                        class,
                        delivered,
                        "ingest delivery failed and will be redelivered"
                    );
                }
            }
        }
        ack_kind(outcome, delivered, &self.redelivery.schedule)
    }
}

/// The broker disposition of a handler outcome.
///
/// Pure: a retryable outcome is negative-acknowledged with the delay the schedule gives for its
/// delivery count and is never terminal, however many deliveries it has had. A delivery that can
/// never succeed arrives as [`Outcome::Reject`], and the repository family ends its own budget by
/// settling a fact (XR-021 CONTRACTS.md R4-05).
pub(crate) fn ack_kind(
    outcome: &Outcome,
    delivered: i64,
    schedule: &RedeliverySchedule,
) -> jetstream::AckKind {
    match outcome {
        Outcome::Done => jetstream::AckKind::Ack,
        Outcome::Reject(_) => jetstream::AckKind::Term,
        Outcome::Retry(_) => jetstream::AckKind::Nak(Some(schedule.delay(delivered))),
    }
}

async fn publish_loop<P: LlmProvider>(
    context: Arc<IngestContext<P>>,
    health: Arc<IngestHealth>,
    mut drain: watch::Receiver<bool>,
) {
    let ingest = &context.config.ingest;
    let mut tracker = PublishHealth::default();
    let mut connection: Option<jetstream::Context> = None;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !*drain.borrow() {
        // cancel-safe: watch::changed and Interval::tick commit nothing.
        tokio::select! {
            biased;
            _ = drain.changed() => break,
            _ = ticker.tick() => {}
        }
        if connection.is_none() {
            connection = bus::connect(&ingest.bus_endpoint, ingest.bus_credentials_file.as_deref())
                .await
                .ok()
                .map(jetstream::new);
        }
        let succeeded = match &connection {
            Some(jetstream) => match publish_event_outbox(&context.database, jetstream).await {
                Ok(_) => true,
                Err(error) => {
                    tracing::warn!(class = %error, "completion publisher is failing");
                    connection = None;
                    false
                }
            },
            None => false,
        };
        let now = Instant::now();
        tracker.record(succeeded, now);
        health.set_publisher(tracker.healthy(now, PUBLISH_READINESS_GRACE));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_nats::jetstream::AckKind;

    use super::ack_kind;
    use crate::ingest_handlers::Outcome;
    use crate::redelivery::RedeliverySchedule;

    /// A retryable failure is naked at every delivery count and never terminated: the delivery
    /// budget that used to end the fifth delivery is gone.
    #[test]
    fn a_retryable_outcome_is_never_terminal() {
        for delivered in [1, 2, 4, 5, 6, 500] {
            let kind = ack_kind(
                &Outcome::Retry("social_analysis"),
                delivered,
                &RedeliverySchedule::STANDARD,
            );
            assert!(
                matches!(kind, AckKind::Nak(Some(_))),
                "delivery {delivered} was {kind:?}"
            );
        }
    }

    #[test]
    fn a_retryable_outcome_waits_as_long_as_the_schedule_says() {
        let schedule = RedeliverySchedule::STANDARD;
        for delivered in [1, 3, 6, 500] {
            assert!(
                matches!(
                    ack_kind(&Outcome::Retry("document_analysis"), delivered, &schedule),
                    AckKind::Nak(Some(delay)) if delay == schedule.delay(delivered)
                ),
                "delivery {delivered}"
            );
        }
        assert!(
            matches!(
                ack_kind(&Outcome::Retry("document_analysis"), 7, &schedule),
                AckKind::Nak(Some(delay)) if delay == Duration::from_mins(1)
            ),
            "the delay stops growing at one minute"
        );
    }

    #[test]
    fn done_is_acknowledged_and_reject_is_terminal_at_any_delivery() {
        for delivered in [1, 5, 500] {
            assert!(matches!(
                ack_kind(&Outcome::Done, delivered, &RedeliverySchedule::STANDARD),
                AckKind::Ack
            ));
            assert!(matches!(
                ack_kind(
                    &Outcome::Reject("undecodable"),
                    delivered,
                    &RedeliverySchedule::STANDARD
                ),
                AckKind::Term
            ));
        }
    }
}
