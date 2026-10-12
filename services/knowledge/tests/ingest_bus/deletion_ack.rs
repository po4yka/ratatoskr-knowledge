//! Knowledge acknowledges every deletion fact it applies, repeats or refuses (XR-021
//! CONTRACTS.md R5-11 rules 1 and 2).
//!
//! The events stream drops its oldest message at 7 days or 1 GiB, and a producer marks a deletion
//! fact published at the broker acknowledgement, so Knowledge tells the producer through its
//! outbox, after the rows and the blobs are gone, and again whenever the producer re-sends a fact
//! it had acknowledged. These tests run the real consumers against a real `JetStream` and
//! database.

use std::time::Duration;

use ratatoskr_knowledge_service::{EVENTS_STREAM, IngestFamily};
use ratatoskr_operation_contracts::{
    DeletionAckFamily, DeletionAckOutcome, DeletionAckRefusal, KnowledgeDeletionAcknowledged,
};

use crate::fixtures::{
    CAPTURED, EVENT_A, EVENT_B, EVENT_C, OTHER_OWNER, OWNER, REMOVED, SOCIAL_ANALYSIS, envelope,
    removal_payload, routed, social_payload,
};
use crate::harness::{BROKER, Harness};
use crate::ingest_support::{TestResult, eventually};
use crate::tombstone::{
    TOMBSTONE_SUBJECT, archive_deliveries, archive_is_quiet, blob_directories, blob_files,
    publish_added, publish_tombstone, scripted, set_mode,
};

const ACK_SUBJECT: &str = "evt.knowledge.deletion.acknowledged.v1";
const SOCIAL_SUBJECT: &str = "evt.social.source.removed.v1";
const CONVERSATION: &str = "ai_conversation:018f0000-0000-7000-8000-000000000403";
const SOCIAL_SOURCE: &str = "social_source:018f0000-0000-7000-8000-000000000201";

/// The acknowledgements on the stream, with their envelopes.
async fn acknowledgements(
    harness: &Harness,
) -> Result<
    Vec<(
        ratatoskr_event_envelope::EventEnvelope,
        KnowledgeDeletionAcknowledged,
    )>,
    Box<dyn std::error::Error>,
> {
    let mut found = Vec::new();
    for (envelope, _id) in harness.facts_on(ACK_SUBJECT).await? {
        let payload = envelope.payload_as::<KnowledgeDeletionAcknowledged>()?;
        payload.validate()?;
        found.push((envelope, payload));
    }
    Ok(found)
}

async fn ack_rows(harness: &Harness) -> Result<i64, sqlx::Error> {
    harness
        .count(
            "select count(*) from knowledge.event_outbox
             where subject = 'knowledge.deletion.acknowledged.v1'",
        )
        .await
}

async fn last_ack_sequence(harness: &Harness) -> Result<u64, Box<dyn std::error::Error>> {
    let stream = harness.context().get_stream(EVENTS_STREAM).await?;
    Ok(stream
        .get_last_raw_message_by_subject(ACK_SUBJECT)
        .await?
        .sequence)
}

/// Waits for an event on the stream to be quiet: nothing more arrives for a moment.
async fn nothing_more_for(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// (a) An AI-archive tombstone is applied, its blobs are erased, and exactly one acknowledgement
/// follows, naming the tombstone's event id and carrying no content.
#[tokio::test]
async fn an_applied_tombstone_is_acknowledged_once_after_its_blobs_are_erased() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    publish_added(&harness, EVENT_A, OWNER).await?;
    eventually("the conversation is searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    archive_is_quiet(&harness).await?;
    assert!(!blob_files(harness.blob_root()).is_empty());

    publish_tombstone(&harness, EVENT_B, OWNER).await?;
    eventually("one acknowledgement is published", || async {
        Ok(acknowledgements(&harness).await?.len() == 1)
    })
    .await?;
    assert!(
        blob_files(harness.blob_root()).is_empty(),
        "the acknowledgement follows the erasure of the blobs"
    );
    assert_eq!(harness.search_documents().await?, 0);
    nothing_more_for(Duration::from_secs(2)).await;
    let all = acknowledgements(&harness).await?;
    assert_eq!(all.len(), 1, "exactly one acknowledgement");
    let (envelope, payload) = &all[0];
    assert_eq!(payload.acknowledged_event_id.0.to_string(), EVENT_B);
    assert_eq!(payload.family, DeletionAckFamily::AiArchive);
    assert_eq!(payload.owner.to_string(), OWNER);
    assert_eq!(payload.subject.to_wire(), CONVERSATION);
    assert_eq!(payload.outcome, DeletionAckOutcome::Applied);
    assert_eq!(payload.refusal, None);
    assert_eq!(envelope.producer.as_str(), "ratatoskr-knowledge");
    assert_eq!(envelope.aggregate_id.to_wire(), CONVERSATION);
    assert_eq!(
        envelope
            .causation_id
            .as_ref()
            .map(ratatoskr_identifiers::EntityRef::to_wire),
        Some(format!("event:{EVENT_B}"))
    );
    assert_eq!(
        envelope.tenant_id.map(|tenant| tenant.to_string()),
        Some(OWNER.to_owned())
    );
    // The transcript of one acknowledgement: identifiers, an outcome and no content.
    eprintln!(
        "acknowledgement subject={ACK_SUBJECT} event_id={} acknowledged_event_id={} family=ai_archive outcome=applied subject_ref={CONVERSATION}",
        envelope.event_id.0, payload.acknowledged_event_id.0
    );
    archive_is_quiet(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// The blobs cannot be removed yet: the delivery is redelivered and nothing is acknowledged
/// until the erasure is done.
#[tokio::test]
async fn nothing_is_acknowledged_while_the_blob_erasure_is_failing() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    publish_added(&harness, EVENT_A, OWNER).await?;
    eventually("the conversation is searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    archive_is_quiet(&harness).await?;
    let directories = blob_directories(harness.blob_root());
    set_mode(&directories, 0o555)?;

    publish_tombstone(&harness, EVENT_B, OWNER).await?;
    let outcome = async {
        eventually("the tombstone was delivered three times", || async {
            Ok(archive_deliveries(&harness).await? >= 4)
        })
        .await?;
        assert!(acknowledgements(&harness).await?.is_empty());
        assert_eq!(ack_rows(&harness).await?, 0);
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    set_mode(&directories, 0o755)?;
    outcome?;

    eventually(
        "the erasure finishes and the acknowledgement follows",
        || async {
            Ok(acknowledgements(&harness).await?.len() == 1
                && blob_files(harness.blob_root()).is_empty())
        },
    )
    .await?;
    archive_is_quiet(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// (b) A social removal is acknowledged the same way, with the family `social`.
#[tokio::test]
async fn an_applied_social_removal_is_acknowledged_once() -> TestResult {
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
        Ok(harness.search_documents().await? == 1)
    })
    .await?;

    harness
        .publish(
            SOCIAL_SUBJECT,
            EVENT_B,
            envelope(
                EVENT_B,
                REMOVED,
                "ratatoskr-x",
                Some(OWNER),
                &removal_payload("2026-08-18T10:00:00Z"),
            ),
        )
        .await?;
    eventually("one acknowledgement is published", || async {
        Ok(acknowledgements(&harness).await?.len() == 1)
    })
    .await?;
    nothing_more_for(Duration::from_secs(2)).await;
    let all = acknowledgements(&harness).await?;
    assert_eq!(all.len(), 1);
    let (_, payload) = &all[0];
    assert_eq!(payload.acknowledged_event_id.0.to_string(), EVENT_B);
    assert_eq!(payload.family, DeletionAckFamily::Social);
    assert_eq!(payload.owner.to_string(), OWNER);
    assert_eq!(payload.subject.to_wire(), SOCIAL_SOURCE);
    assert_eq!(payload.outcome, DeletionAckOutcome::Applied);
    assert_eq!(harness.search_documents().await?, 0);
    harness.settled(IngestFamily::Social, 2).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// (c) The producer re-sends a fact whose acknowledgement the stream dropped: a repeat after the
/// re-arm window produces the acknowledgement again, under the same outbox id.
#[tokio::test]
async fn a_redelivered_tombstone_produces_a_lost_acknowledgement_again() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_with_duplicate_window(Duration::from_secs(1))
        .await?
        .with_ack_rearm_after(Duration::from_secs(1));
    let worker = harness.spawn(scripted());
    publish_added(&harness, EVENT_A, OWNER).await?;
    eventually("the conversation is searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    publish_tombstone(&harness, EVENT_B, OWNER).await?;
    eventually("the first acknowledgement is published", || async {
        Ok(acknowledgements(&harness).await?.len() == 1)
    })
    .await?;
    let first_sequence = last_ack_sequence(&harness).await?;
    let first_id = acknowledgements(&harness).await?[0].0.event_id.0;

    // The stream loses the acknowledgement, and the producer, which never saw it, re-sends.
    harness
        .context()
        .get_stream(EVENTS_STREAM)
        .await?
        .delete_message(first_sequence)
        .await?;
    assert!(acknowledgements(&harness).await?.is_empty());
    nothing_more_for(Duration::from_secs(2)).await;
    publish_tombstone(&harness, EVENT_B, OWNER).await?;

    eventually("the acknowledgement is produced again", || async {
        Ok(acknowledgements(&harness).await?.len() == 1
            && last_ack_sequence(&harness).await? > first_sequence)
    })
    .await?;
    assert_eq!(
        acknowledgements(&harness).await?[0].0.event_id.0,
        first_id,
        "the same outbox row, so the same message id"
    );
    assert_eq!(ack_rows(&harness).await?, 1);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A repeat inside the re-arm window is not produced again.
#[tokio::test]
async fn a_redelivery_inside_the_re_arm_window_produces_nothing() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    publish_added(&harness, EVENT_A, OWNER).await?;
    eventually("the conversation is searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    publish_tombstone(&harness, EVENT_B, OWNER).await?;
    eventually("the acknowledgement is published", || async {
        Ok(acknowledgements(&harness).await?.len() == 1
            && harness
                .count(
                    "select count(*) from knowledge.event_outbox
                     where subject = 'knowledge.deletion.acknowledged.v1'
                       and published_at is not null",
                )
                .await?
                == 1)
    })
    .await?;
    let published_at: String = sqlx::query_scalar(
        "select published_at::text from knowledge.event_outbox
         where subject = 'knowledge.deletion.acknowledged.v1'",
    )
    .fetch_one(harness.database.database.pool())
    .await?;

    // The same tombstone again, under another message id so the broker stores it.
    let repeat = envelope(
        EVENT_B,
        "ai_archive.subject.tombstoned.v1",
        "ratatoskr-chatgpt",
        Some(OWNER),
        &serde_json::from_str(crate::fixtures::CONTRACT_TOMBSTONE)?,
    );
    harness
        .publish(TOMBSTONE_SUBJECT, "repeat-1", repeat)
        .await?;
    harness.settled(IngestFamily::AiArchive, 3).await?;
    nothing_more_for(Duration::from_secs(2)).await;

    let after: String = sqlx::query_scalar(
        "select published_at::text from knowledge.event_outbox
         where subject = 'knowledge.deletion.acknowledged.v1'",
    )
    .fetch_one(harness.database.database.pool())
    .await?;
    assert_eq!(
        after, published_at,
        "the row was not re-armed and published again"
    );
    assert_eq!(ack_rows(&harness).await?, 1);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// (d) A tombstone for a subject that belongs only to another tenant is acknowledged as refused
/// and terminated.
#[tokio::test]
async fn a_foreign_subject_is_acknowledged_as_refused_and_terminated() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    publish_added(&harness, EVENT_A, OTHER_OWNER).await?;
    eventually("the other tenant's conversation is searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    archive_is_quiet(&harness).await?;

    publish_tombstone(&harness, EVENT_B, OWNER).await?;
    eventually("one acknowledgement is published", || async {
        Ok(acknowledgements(&harness).await?.len() == 1)
    })
    .await?;
    let all = acknowledgements(&harness).await?;
    let (_, payload) = &all[0];
    assert_eq!(payload.acknowledged_event_id.0.to_string(), EVENT_B);
    assert_eq!(payload.owner.to_string(), OWNER);
    assert_eq!(payload.outcome, DeletionAckOutcome::Refused);
    assert_eq!(payload.refusal, Some(DeletionAckRefusal::ForeignSubject));
    harness.settled(IngestFamily::AiArchive, 2).await?;
    nothing_more_for(Duration::from_secs(3)).await;
    harness.settled(IngestFamily::AiArchive, 2).await?;
    assert_eq!(
        harness.search_documents().await?,
        1,
        "the other tenant's data stays"
    );
    assert_eq!(
        harness
            .count("select count(*) from knowledge.ai_archive_tombstones")
            .await?,
        0
    );
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// (e) A message that cannot be attributed is terminated and never acknowledged.
#[tokio::test]
async fn an_unattributable_deletion_fact_is_terminated_without_an_acknowledgement() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    let tombstone: serde_json::Value = serde_json::from_str(crate::fixtures::CONTRACT_TOMBSTONE)?;

    harness
        .publish(TOMBSTONE_SUBJECT, EVENT_A, b"not an envelope".to_vec())
        .await?;
    harness
        .publish(
            TOMBSTONE_SUBJECT,
            EVENT_B,
            envelope(
                EVENT_B,
                "ai_archive.subject.tombstoned.v1",
                "ratatoskr-x",
                Some(OWNER),
                &tombstone,
            ),
        )
        .await?;
    harness
        .publish(
            TOMBSTONE_SUBJECT,
            EVENT_C,
            envelope(
                EVENT_C,
                "ai_archive.subject.tombstoned.v1",
                "ratatoskr-chatgpt",
                None,
                &tombstone,
            ),
        )
        .await?;
    harness
        .publish(
            SOCIAL_SUBJECT,
            "018f0000-0000-7000-8000-000000000b21",
            envelope(
                "018f0000-0000-7000-8000-000000000b21",
                REMOVED,
                "ratatoskr-chatgpt",
                Some(OWNER),
                &removal_payload("2026-08-18T10:00:00Z"),
            ),
        )
        .await?;
    harness
        .publish(
            SOCIAL_SUBJECT,
            "018f0000-0000-7000-8000-000000000b22",
            envelope(
                "018f0000-0000-7000-8000-000000000b22",
                REMOVED,
                "ratatoskr-x",
                None,
                &removal_payload("2026-08-18T10:00:00Z"),
            ),
        )
        .await?;

    harness.settled(IngestFamily::AiArchive, 3).await?;
    harness.settled(IngestFamily::Social, 2).await?;
    nothing_more_for(Duration::from_secs(2)).await;
    harness.settled(IngestFamily::AiArchive, 3).await?;
    harness.settled(IngestFamily::Social, 2).await?;
    assert!(acknowledgements(&harness).await?.is_empty());
    assert_eq!(ack_rows(&harness).await?, 0);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// (f) A tombstone for a subject Knowledge never held has nothing to delete and is applied: the
/// tombstone is still recorded so a late older delivery cannot bring the subject back.
#[tokio::test]
async fn a_tombstone_for_a_subject_never_held_is_applied() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());

    publish_tombstone(&harness, EVENT_B, OWNER).await?;
    eventually("one acknowledgement is published", || async {
        Ok(acknowledgements(&harness).await?.len() == 1)
    })
    .await?;
    let all = acknowledgements(&harness).await?;
    assert_eq!(all[0].1.outcome, DeletionAckOutcome::Applied);
    assert_eq!(all[0].1.refusal, None);
    assert_eq!(
        harness
            .count("select count(*) from knowledge.ai_archive_tombstones")
            .await?,
        1
    );
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}
