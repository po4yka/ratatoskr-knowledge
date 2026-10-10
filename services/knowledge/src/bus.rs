//! Broker connection and durable verification shared by the recap and ingest workers.
//!
//! Workers verify, never create: the durable is provisioned by the platform, and a worker refuses
//! to consume from one whose filter, acknowledgement policy or deadlines differ from the fixed
//! table (XR-021 CONTRACTS.md S02 and S04).

use std::path::Path;
use std::time::Duration;

use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PullConsumer};

/// Why a bus dependency is not usable. Carries no endpoint, credential or message content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BusFailure {
    /// The broker cannot be reached, refuses the credential, or a stream operation failed.
    #[error("the bus dependency is unavailable")]
    Unavailable,
    /// The pre-provisioned durable does not match the fixed contract.
    #[error("the durable does not match the fixed contract")]
    Durable,
}

/// The configuration a durable must carry for a worker to consume from it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DurableSpec<'a> {
    pub(crate) name: &'a str,
    pub(crate) filter: &'a str,
    pub(crate) ack_wait: Duration,
    /// `Some(-1)` for durables that redeliver without limit; `None` leaves it unchecked.
    pub(crate) max_deliver: Option<i64>,
}

/// Connects with the `NKey` seed read from `credentials`, or anonymously when it is absent.
///
/// The seed is read once and never logged.
pub(crate) async fn connect(
    endpoint: &str,
    credentials: Option<&Path>,
) -> Result<async_nats::Client, BusFailure> {
    if let Some(path) = credentials {
        let seed = tokio::fs::read_to_string(path)
            .await
            .map_err(|_| BusFailure::Unavailable)?;
        async_nats::ConnectOptions::with_nkey(seed.trim().to_owned())
            .connect(endpoint)
            .await
            .map_err(|_| BusFailure::Unavailable)
    } else {
        async_nats::connect(endpoint)
            .await
            .map_err(|_| BusFailure::Unavailable)
    }
}

/// Checks the durable's cached configuration against the spec.
pub(crate) fn verify_consumer(
    consumer: &PullConsumer,
    spec: &DurableSpec<'_>,
) -> Result<(), BusFailure> {
    let actual = &consumer.cached_info().config;
    if actual.durable_name.as_deref() != Some(spec.name)
        || actual.filter_subject != spec.filter
        || actual.ack_policy != AckPolicy::Explicit
        || actual.ack_wait != spec.ack_wait
        || actual.deliver_subject.is_some()
        || actual.deliver_policy != DeliverPolicy::All
        || spec
            .max_deliver
            .is_some_and(|expected| actual.max_deliver != expected)
    {
        return Err(BusFailure::Durable);
    }
    Ok(())
}
