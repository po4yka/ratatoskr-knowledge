//! The ingest worker against a real `JetStream`: four durable consumers, their dispositions, and
//! the completion publisher (XR-021 CONTRACTS.md S02, S04 and S09).
//!
//! The durable and stream names are compiled constants of the service, so these tests share one
//! disposable broker and run one at a time.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ratatoskr_ai_archive_contracts::AiArchiveProvenance;
use ratatoskr_knowledge::test_support::archive::conversation_from_json;
use ratatoskr_knowledge::{ScriptedProvider, SourceInbox};
use ratatoskr_knowledge_service::{IngestFamily, PUBLISH_READINESS_GRACE, PublishHealth};
use ratatoskr_social_contracts::{SocialSourceAnalysisCompleted, SocialSourceSnapshot};

use crate::fixtures::{
    ARCHIVE_ANALYSIS, ARTICLE_ANALYSIS, CAPTURED, CONTRACT_CONVERSATION, CONTRACT_TOMBSTONE,
    EVENT_A, EVENT_B, OWNER, SOCIAL_ANALYSIS, envelope, extracted_document, removal_payload,
    response, routed, social_payload,
};
use crate::harness::{BROKER, Harness};
use crate::ingest_support::{TestResult, eventually};

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
        Ok(harness.search_documents().await? == 1)
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
        Ok(harness.search_documents().await? == 1)
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
        Ok(harness.search_documents().await? == 0)
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
        Ok(harness.search_documents().await? == 1)
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
        Ok(harness.search_documents().await? == 0)
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
    assert_eq!(harness.search_documents().await?, 0);

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
        Ok(harness.search_documents().await? == 2)
    })
    .await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
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
    let messages = harness
        .facts_on("evt.knowledge.analysis.completed.v1")
        .await?;
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
        harness
            .facts_on("evt.knowledge.analysis.completed.v1")
            .await?
            .len(),
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
