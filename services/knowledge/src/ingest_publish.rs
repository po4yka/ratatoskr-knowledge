//! Publication of Knowledge's committed completion facts (XR-021 CONTRACTS.md S09).

use std::time::Duration;

use async_nats::jetstream;
use ratatoskr_knowledge::{Database, EventOutbox};

const BATCH: i64 = 32;
const PUBLISH_DEADLINE: Duration = Duration::from_secs(5);

/// Why a batch of completion facts could not be published. Carries no payload content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PublishFailure {
    /// The broker did not accept or acknowledge the message in time.
    ///
    /// A publish that the broker's permissions refuse is indistinguishable from a timeout to the
    /// client: check the NATS server log for a Publish Violation.
    #[error("the completion fact was not acknowledged by the broker")]
    Unacknowledged,
    /// The outbox could not be read or updated.
    #[error("the completion outbox is unavailable")]
    Outbox,
    /// A stored fact does not form a valid envelope.
    #[error("a stored completion fact is not a valid envelope")]
    Encode,
}

/// Publishes up to a batch of unpublished facts and marks each after its acknowledgement.
///
/// Returns the number published.
///
/// # Errors
///
/// Returns the first failure; the facts that follow it stay unpublished and are retried by the
/// next call.
pub async fn publish_event_outbox(
    database: &Database,
    context: &jetstream::Context,
) -> Result<usize, PublishFailure> {
    let outbox = EventOutbox::new(database);
    let rows = outbox
        .claim_unpublished(BATCH)
        .await
        .map_err(|_| PublishFailure::Outbox)?;
    let mut published = 0;
    for row in rows {
        let envelope = row.envelope().map_err(|_| PublishFailure::Encode)?;
        let bytes = serde_json::to_vec(&envelope).map_err(|_| PublishFailure::Encode)?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", row.outbox_id.to_string());
        let accepted = tokio::time::timeout(
            PUBLISH_DEADLINE,
            context.publish_with_headers(
                format!("evt.{}", row.subject.as_str()),
                headers,
                bytes.into(),
            ),
        )
        .await
        .map_err(|_| PublishFailure::Unacknowledged)?
        .map_err(|_| PublishFailure::Unacknowledged)?;
        tokio::time::timeout(PUBLISH_DEADLINE, accepted)
            .await
            .map_err(|_| PublishFailure::Unacknowledged)?
            .map_err(|_| PublishFailure::Unacknowledged)?;
        outbox
            .mark_published(row.outbox_id)
            .await
            .map_err(|_| PublishFailure::Outbox)?;
        published += 1;
    }
    Ok(published)
}
