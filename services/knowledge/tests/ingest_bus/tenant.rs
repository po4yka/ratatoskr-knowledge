//! Tenant binding of the ingest handlers: a delivery whose payload names an owner other than its
//! envelope's `tenant_id` is terminated and writes nothing (XR-021 CONTRACTS.md S09).

use std::time::Duration;

use ratatoskr_knowledge_service::IngestFamily;

use crate::fixtures::{
    ARCHIVE_ANALYSIS, CAPTURED, CONTRACT_CONVERSATION, CONTRACT_TOMBSTONE, CONVERSATION_ADDED,
    EVENT_A, EVENT_B, EVENT_C, EVENT_D, OTHER_OWNER, OWNER, REMOVED, REPOSITORY_ANALYSIS_ABSENT,
    REPOSITORY_REQUESTED, SOCIAL_ANALYSIS, envelope, removal_payload, repository_request, routed,
    social_payload,
};
use crate::harness::{BROKER, Harness};
use crate::ingest_support::TestResult;

/// Every table a delivery could write to before it is acknowledged.
const WRITTEN_BY_ADMISSION: &[&str] = &[
    "source_refs",
    "analysis_runs",
    "source_analysis_inbox",
    "source_analysis_heads",
    "repository_analysis_requests",
    "ai_archive_tombstones",
    "ai_archive_object_inbox",
    "social_tombstones",
    "search_documents",
    "event_outbox",
];

/// Publishes the five mismatched deliveries: the payload owner is `OWNER`, the envelope names
/// `OTHER_OWNER`.
async fn publish_mismatched(harness: &Harness) -> TestResult {
    harness
        .publish(
            "evt.social.source.captured.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                CAPTURED,
                "ratatoskr-x",
                Some(OTHER_OWNER),
                &social_payload("2026-08-17T10:00:00Z")?,
            ),
        )
        .await?;
    harness
        .publish(
            "evt.social.source.removed.v1",
            EVENT_B,
            envelope(
                EVENT_B,
                REMOVED,
                "ratatoskr-x",
                Some(OTHER_OWNER),
                &removal_payload("2026-08-18T10:00:00Z"),
            ),
        )
        .await?;
    harness
        .publish(
            "evt.ai_archive.conversation.added.v1",
            EVENT_C,
            envelope(
                EVENT_C,
                CONVERSATION_ADDED,
                "ratatoskr-chatgpt",
                Some(OTHER_OWNER),
                &serde_json::from_str(CONTRACT_CONVERSATION)?,
            ),
        )
        .await?;
    harness
        .publish(
            "evt.ai_archive.subject.tombstoned.v1",
            EVENT_D,
            envelope(
                EVENT_D,
                "ai_archive.subject.tombstoned.v1",
                "ratatoskr-chatgpt",
                Some(OTHER_OWNER),
                &serde_json::from_str(CONTRACT_TOMBSTONE)?,
            ),
        )
        .await?;
    harness
        .publish(
            "evt.knowledge.repository_analysis.requested.v1",
            "018f0000-0000-7000-8000-000000000b15",
            envelope(
                "018f0000-0000-7000-8000-000000000b15",
                REPOSITORY_REQUESTED,
                "ratatoskr-github",
                Some(OTHER_OWNER),
                &serde_json::to_value(repository_request(OWNER, None)?)?,
            ),
        )
        .await
}

/// Without the binding a producer could inject a snapshot, a removal, an archive object, a
/// tombstone or a repository request for a tenant its envelope does not name. The provider can
/// answer every family, so a delivery that got past the check would leave rows behind.
#[tokio::test]
async fn payload_owner_that_differs_from_the_envelope_tenant_is_terminated_and_writes_nothing()
-> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(routed(&[
        ("social_prompt_v1", SOCIAL_ANALYSIS),
        ("archive_prompt_v1", ARCHIVE_ANALYSIS),
        ("repository_prompt_v1", REPOSITORY_ANALYSIS_ABSENT),
    ]));
    publish_mismatched(&harness).await?;

    harness.settled(IngestFamily::Social, 2).await?;
    harness.settled(IngestFamily::AiArchive, 2).await?;
    harness.settled(IngestFamily::Repository, 1).await?;
    // A redelivery would arrive after the 2 s nak delay; none may.
    tokio::time::sleep(Duration::from_secs(4)).await;
    harness.settled(IngestFamily::Social, 2).await?;
    harness.settled(IngestFamily::AiArchive, 2).await?;
    harness.settled(IngestFamily::Repository, 1).await?;
    assert_eq!(harness.rows_in(WRITTEN_BY_ADMISSION).await?, 0);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}
