//! Acknowledgement of deletion facts by the ingest worker (XR-021 CONTRACTS.md R5-11 rule 2).
//!
//! A deletion fact is acknowledged only after its rows are deleted and its blobs are collected,
//! so an acknowledgement always means the content is gone. The acknowledgement is queued in
//! Knowledge's outbox; a repeat of the fact re-arms it, so a producer that re-sends a fact it
//! had acknowledged gets the acknowledgement again.

use ratatoskr_ai_archive_contracts::{AiArchiveTombstone, AiArchiveTombstoneSubject};
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_identifiers::EntityRef;
use ratatoskr_knowledge::{
    DeletionAcknowledgement, EventCause, EventOutbox, EventOutboxError, LlmProvider,
};
use ratatoskr_operation_contracts::DeletionAckFamily;
use ratatoskr_social_contracts::SocialSourceRemoved;

use crate::ingest::IngestContext;
use crate::ingest_handlers::{Outcome, QueueFailure};

/// The reference to the subject an AI-archive tombstone names.
fn tombstone_subject(tombstone: &AiArchiveTombstone) -> Result<EntityRef, EventOutboxError> {
    let reference = match &tombstone.subject {
        AiArchiveTombstoneSubject::Archive => format!("ai_archive:{}", tombstone.ai_archive_id),
        AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => {
            format!("ai_conversation:{ai_conversation_id}")
        }
        AiArchiveTombstoneSubject::Project { ai_project_id } => {
            format!("ai_project:{ai_project_id}")
        }
        AiArchiveTombstoneSubject::Artifact {
            external_artifact_id,
        } => format!("ai_artifact:{external_artifact_id}"),
    };
    EntityRef::parse(&reference).map_err(|_| EventOutboxError::InvalidRow)
}

/// The acknowledgement of an AI-archive tombstone: applied, or refused because its subject
/// belongs to another owner.
pub(crate) fn archive_acknowledgement(
    envelope: &EventEnvelope,
    refused: bool,
) -> Result<DeletionAcknowledgement, EventOutboxError> {
    let tombstone = envelope
        .payload_as::<AiArchiveTombstone>()
        .map_err(|_| EventOutboxError::InvalidRow)?;
    let cause = EventCause::of(envelope);
    let subject = tombstone_subject(&tombstone)?;
    Ok(if refused {
        DeletionAcknowledgement::foreign_subject(
            cause,
            DeletionAckFamily::AiArchive,
            tombstone.owner,
            subject,
        )
    } else {
        DeletionAcknowledgement::applied(
            cause,
            DeletionAckFamily::AiArchive,
            tombstone.owner,
            subject,
        )
    })
}

/// The acknowledgement of an applied social removal.
pub(crate) fn social_acknowledgement(
    envelope: &EventEnvelope,
    removed: &SocialSourceRemoved,
) -> Result<DeletionAcknowledgement, EventOutboxError> {
    Ok(DeletionAcknowledgement::applied(
        EventCause::of(envelope),
        DeletionAckFamily::Social,
        removed.owner,
        EntityRef::parse(&format!("social_source:{}", removed.social_source_id))
            .map_err(|_| EventOutboxError::InvalidRow)?,
    ))
}

impl<P: LlmProvider> IngestContext<P> {
    /// Queues `acknowledgement`, or produces it again when it was published longer ago than the
    /// re-arm window. [`Outcome::Done`] means the acknowledgement is queued; a failure to queue it
    /// is a retry, never a silent success, because the producer re-sends until it is acknowledged.
    pub(crate) async fn acknowledge(
        &self,
        acknowledgement: Result<DeletionAcknowledgement, EventOutboxError>,
        retry_class: &'static str,
    ) -> Outcome {
        let queued = match acknowledgement {
            Ok(acknowledgement) => {
                EventOutbox::new(&self.database)
                    .acknowledge_deletion(acknowledgement, self.redelivery.ack_rearm_after)
                    .await
            }
            Err(error) => Err(error),
        };
        match queued {
            Ok(_) => Outcome::Done,
            Err(error) => QueueFailure::from(error).outcome(retry_class),
        }
    }
}
