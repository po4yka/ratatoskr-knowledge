//! The acknowledgement Knowledge queues for a deletion fact (XR-021 CONTRACTS.md R5-11).
//!
//! The events stream drops its oldest message at 7 days or 1 GiB, and a producer marks a deletion
//! fact published at the broker acknowledgement. Knowledge therefore tells the producer, through
//! its transactional outbox, that an `ai_archive.subject.tombstoned.v1` or a
//! `social.source.removed.v1` was applied or refused, and the producer re-sends the deletion fact
//! until the acknowledgement arrives. A repeat of an applied fact re-arms the outbox row, so an
//! acknowledgement the stream dropped is produced again.

use std::time::Duration;

use ratatoskr_identifiers::{EntityRef, EventId, Extensions, TenantRef, WireTimestamp};
use ratatoskr_operation_contracts::{
    DeletionAckFamily, DeletionAckOutcome, DeletionAckRefusal, KnowledgeDeletionAcknowledged,
};

use crate::{EnqueueOutcome, EventCause, EventOutbox, EventOutboxError, OutboxFact, OutboxSubject};

/// How long an acknowledgement stays published before a repeat of its deletion fact produces it
/// again: a producer that re-sends a fact it already had acknowledged lost the acknowledgement.
pub const DELETION_ACK_REARM_AFTER: Duration = Duration::from_mins(1);

/// What Knowledge decided about one deletion fact.
#[derive(Debug, Clone)]
pub struct DeletionAcknowledgement {
    /// The deletion fact being answered: its event id and its correlation.
    pub acknowledged: EventCause,
    /// Which deletion fact family it is.
    pub family: DeletionAckFamily,
    /// The owner of the deleted data.
    pub owner: TenantRef,
    /// The subject the fact named.
    pub subject: EntityRef,
    /// Whether the fact was applied or refused.
    pub outcome: DeletionAckOutcome,
    /// Why it was refused, present exactly when `outcome` is `refused`.
    pub refusal: Option<DeletionAckRefusal>,
}

impl DeletionAcknowledgement {
    /// An acknowledgement that the deletion was applied (or repeats one that was).
    #[must_use]
    pub const fn applied(
        acknowledged: EventCause,
        family: DeletionAckFamily,
        owner: TenantRef,
        subject: EntityRef,
    ) -> Self {
        Self {
            acknowledged,
            family,
            owner,
            subject,
            outcome: DeletionAckOutcome::Applied,
            refusal: None,
        }
    }

    /// An acknowledgement that the deletion was refused because its subject belongs to another
    /// owner.
    #[must_use]
    pub const fn foreign_subject(
        acknowledged: EventCause,
        family: DeletionAckFamily,
        owner: TenantRef,
        subject: EntityRef,
    ) -> Self {
        Self {
            acknowledged,
            family,
            owner,
            subject,
            outcome: DeletionAckOutcome::Refused,
            refusal: Some(DeletionAckRefusal::ForeignSubject),
        }
    }

    /// The outbox fact that carries this acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`EventOutboxError::InvalidRow`] when the payload violates the contract.
    pub fn into_fact(self) -> Result<OutboxFact, EventOutboxError> {
        let payload = KnowledgeDeletionAcknowledged {
            acknowledged_event_id: EventId(self.acknowledged.event_id),
            family: self.family,
            owner: self.owner,
            subject: self.subject.clone(),
            outcome: self.outcome,
            refusal: self.refusal,
            acknowledged_at: WireTimestamp::now(),
            extensions: Extensions::new(),
        };
        payload
            .validate()
            .map_err(|_| EventOutboxError::InvalidRow)?;
        Ok(OutboxFact {
            subject: OutboxSubject::DeletionAcknowledged,
            tenant: self.owner,
            aggregate_id: self.subject,
            correlation_id: self.acknowledged.correlation_id.clone(),
            causation_id: Some(self.acknowledged.causation_ref()?),
            payload: serde_json::to_value(&payload).map_err(|_| EventOutboxError::InvalidRow)?,
            dedupe_key: format!("deletion_ack:{}", self.acknowledged.event_id),
        })
    }
}

impl EventOutbox<'_> {
    /// Queues `acknowledgement`, or produces it again when it was published more than
    /// `rearm_after` ago.
    ///
    /// # Errors
    ///
    /// Returns [`EventOutboxError`] when the acknowledgement is not representable or the write
    /// fails.
    pub async fn acknowledge_deletion(
        &self,
        acknowledgement: DeletionAcknowledgement,
        rearm_after: Duration,
    ) -> Result<EnqueueOutcome, EventOutboxError> {
        let fact = acknowledgement.into_fact()?;
        let mut transaction = self.begin().await?;
        let outcome = Self::enqueue_rearm_in(&mut transaction, &fact, rearm_after).await?;
        self.commit(transaction).await?;
        Ok(outcome)
    }
}
