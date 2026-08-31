use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use futures_util::StreamExt as _;
use ratatoskr_knowledge::{
    AdmissionDisposition, Config, Database, PRIMARY_EVENT_SUBJECTS, PrimaryAdmissionStore,
    TerminalOutbox,
};
use tokio::sync::watch;

use crate::{Lifecycle, Metrics};

#[must_use]
pub(super) fn spawn_intake_supervisor(
    config: Config,
    database: Database,
    lifecycle: Lifecycle,
    metrics: Arc<Metrics>,
    drain: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        supervise_intake(&config, &database, &lifecycle, &metrics, drain).await;
        lifecycle.set_primary_bus_ready(false);
    })
}

#[must_use]
pub(super) fn spawn_outbox_supervisor(
    config: Config,
    database: Database,
    lifecycle: Lifecycle,
    metrics: Arc<Metrics>,
    drain: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        supervise_outbox(&config, &database, &lifecycle, &metrics, drain).await;
        lifecycle.set_primary_outbox_ready(false);
    })
}

async fn supervise_intake(
    config: &Config,
    database: &Database,
    lifecycle: &Lifecycle,
    metrics: &Metrics,
    mut drain: watch::Receiver<bool>,
) {
    while !*drain.borrow() {
        let _result = consume_once(config, database, lifecycle, metrics, &mut drain).await;
        lifecycle.set_primary_bus_ready(false);
        if *drain.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = drain.changed() => {}
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn consume_once(
    config: &Config,
    database: &Database,
    lifecycle: &Lifecycle,
    metrics: &Metrics,
    drain: &mut watch::Receiver<bool>,
) -> Result<(), ()> {
    let client = connect(config).await?;
    let context = jetstream::new(client);
    let consumer: jetstream::consumer::PullConsumer = context
        .get_consumer_from_stream(&config.primary.bus_durable, &config.primary.bus_stream)
        .await
        .map_err(|_| ())?;
    verify_consumer(&consumer, config)?;
    let mut messages = consumer
        .stream()
        .max_messages_per_batch(usize::try_from(config.primary.fetch_batch).unwrap_or(32))
        .messages()
        .await
        .map_err(|_| ())?;
    lifecycle.set_primary_bus_ready(true);
    loop {
        tokio::select! {
            biased;
            _ = drain.changed() => return Ok(()),
            next = messages.next() => {
                let Some(next) = next else { return Err(()); };
                let message = next.map_err(|_| ())?;
                let result = PrimaryAdmissionStore::new(database)
                    .admit(message.subject.as_str(), message.payload.as_ref())
                    .await;
                let ack = match result {
                    Ok(AdmissionDisposition::Accepted) => {
                        metrics.record_primary_admitted();
                        jetstream::AckKind::Ack
                    }
                    Ok(AdmissionDisposition::Duplicate | AdmissionDisposition::Suppressed) => {
                        jetstream::AckKind::Ack
                    }
                    Ok(AdmissionDisposition::Rejected) => {
                        metrics.record_primary_rejected();
                        jetstream::AckKind::Term
                    }
                    Ok(AdmissionDisposition::Collision) => {
                        metrics.record_primary_collision();
                        jetstream::AckKind::Term
                    }
                    Err(_) => jetstream::AckKind::Nak(Some(Duration::from_secs(2))),
                };
                message.ack_with(ack).await.map_err(|_| ())?;
            }
        }
    }
}

async fn supervise_outbox(
    config: &Config,
    database: &Database,
    lifecycle: &Lifecycle,
    metrics: &Metrics,
    mut drain: watch::Receiver<bool>,
) {
    loop {
        let result = publish_once(config, database, lifecycle, metrics, &mut drain).await;
        lifecycle.set_primary_outbox_ready(false);
        if *drain.borrow() {
            break;
        }
        if result.is_err() {
            metrics.record_outbox_retry();
        }
        tokio::select! {
            biased;
            _ = drain.changed() => {}
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn publish_once(
    config: &Config,
    database: &Database,
    lifecycle: &Lifecycle,
    metrics: &Metrics,
    drain: &mut watch::Receiver<bool>,
) -> Result<(), ()> {
    let client = connect(config).await?;
    let context = jetstream::new(client);
    lifecycle.set_primary_outbox_ready(true);
    let outbox = TerminalOutbox::new(database);
    loop {
        let Some(entry) = outbox.next_pending().await.map_err(|_| ())? else {
            if *drain.borrow() {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = drain.changed() => return Ok(()),
                () = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
            continue;
        };
        let payload = serde_json::to_vec(&entry.envelope).map_err(|_| ())?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", entry.message_id.to_string());
        let acknowledgement = context
            .publish_with_headers(entry.subject.clone(), headers, payload.into())
            .await
            .map_err(|_| ())?;
        acknowledgement.await.map_err(|_| ())?;
        outbox
            .mark_published(entry.outbox_id)
            .await
            .map_err(|_| ())?;
        metrics.record_outbox_published();
    }
}

async fn connect(config: &Config) -> Result<async_nats::Client, ()> {
    if let Some(path) = &config.primary.bus_credentials_file {
        let metadata = tokio::fs::metadata(path).await.map_err(|_| ())?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 4_096 {
            return Err(());
        }
        let seed = tokio::fs::read_to_string(path).await.map_err(|_| ())?;
        async_nats::ConnectOptions::with_nkey(seed.trim().to_owned())
            .connect(&config.primary.bus_endpoint)
            .await
            .map_err(|_| ())
    } else {
        async_nats::connect(&config.primary.bus_endpoint)
            .await
            .map_err(|_| ())
    }
}

fn verify_consumer(
    consumer: &jetstream::consumer::PullConsumer,
    config: &Config,
) -> Result<(), ()> {
    let actual = &consumer.cached_info().config;
    let expected = PRIMARY_EVENT_SUBJECTS
        .iter()
        .map(|subject| (*subject).to_owned())
        .collect::<Vec<_>>();
    if actual.durable_name.as_deref() != Some(config.primary.bus_durable.as_str())
        || !actual.filter_subject.is_empty()
        || actual.filter_subjects != expected
        || actual.ack_policy != jetstream::consumer::AckPolicy::Explicit
        || actual.ack_wait != Duration::from_secs(config.primary.ack_wait_seconds)
        || actual.deliver_subject.is_some()
        || actual.deliver_policy != jetstream::consumer::DeliverPolicy::All
        || actual.replay_policy != jetstream::consumer::ReplayPolicy::Instant
    {
        return Err(());
    }
    Ok(())
}
