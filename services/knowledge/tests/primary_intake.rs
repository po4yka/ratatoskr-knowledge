//! Primary delivery admission and poison-message regression tests.

use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{AdmissionDisposition, PRIMARY_EVENT_SUBJECTS, PrimaryAdmissionStore};
use uuid::Uuid;

const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";
const ARCHIVE: &str = "018f0000-0000-7000-8000-000000000402";

#[tokio::test]
async fn delivery_is_acked_only_after_collision_checked_atomic_admission()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        PRIMARY_EVENT_SUBJECTS,
        [
            "evt.content.document.extracted.v1",
            "evt.social.source.captured.v1",
            "evt.social.source.updated.v1",
            "evt.social.source.removed.v1",
            "evt.ai_archive.archive.imported.v1",
            "evt.ai_archive.conversation.added.v1",
            "evt.ai_archive.conversation.updated.v1",
            "evt.ai_archive.project.added.v1",
            "evt.ai_archive.project.updated.v1",
            "evt.ai_archive.artifact.added.v1",
            "evt.ai_archive.artifact.updated.v1",
            "evt.ai_archive.subject.tombstoned.v1",
            "evt.knowledge.repository_analysis.requested.v1",
        ]
    );

    let database = TestDatabase::create().await?;
    let intake = PrimaryAdmissionStore::new(&database.database);

    assert_eq!(
        intake
            .admit("evt.social.source.captured.v1", b"not-json")
            .await?,
        AdmissionDisposition::Rejected
    );
    assert_counts(&database, 0, 0, 1).await?;

    let event_id = Uuid::now_v7();
    let valid = archive_conversation_envelope(event_id);
    let bytes = serde_json::to_vec(&valid)?;
    assert_eq!(
        intake
            .admit("evt.ai_archive.conversation.added.v1", &bytes)
            .await?,
        AdmissionDisposition::Accepted
    );
    assert_counts(&database, 1, 1, 1).await?;
    assert_eq!(
        intake
            .admit("evt.ai_archive.conversation.added.v1", &bytes)
            .await?,
        AdmissionDisposition::Duplicate
    );
    assert_counts(&database, 1, 1, 1).await?;

    let mut collision = valid.clone();
    collision["occurred_at"] = serde_json::json!("2026-08-18T11:31:01Z");
    assert_eq!(
        intake
            .admit(
                "evt.ai_archive.conversation.added.v1",
                &serde_json::to_vec(&collision)?,
            )
            .await?,
        AdmissionDisposition::Collision
    );
    assert_counts(&database, 1, 1, 2).await?;

    for (field, value) in [
        ("producer", serde_json::json!("ratatoskr-github")),
        (
            "tenant_id",
            serde_json::json!("user:018f0000-0000-7000-8000-000000000999"),
        ),
        (
            "aggregate_id",
            serde_json::json!("ai_archive:018f0000-0000-7000-8000-000000000999"),
        ),
    ] {
        let mut invalid = archive_conversation_envelope(Uuid::now_v7());
        invalid[field] = value;
        assert_eq!(
            intake
                .admit(
                    "evt.ai_archive.conversation.added.v1",
                    &serde_json::to_vec(&invalid)?,
                )
                .await?,
            AdmissionDisposition::Rejected
        );
    }

    let valid_after_poison = archive_conversation_envelope(Uuid::now_v7());
    assert_eq!(
        intake
            .admit(
                "evt.ai_archive.conversation.added.v1",
                &serde_json::to_vec(&valid_after_poison)?,
            )
            .await?,
        AdmissionDisposition::Suppressed,
        "same immutable source revision converges through the source head without another work row"
    );
    let receipts: i64 = sqlx::query_scalar("select count(*) from knowledge.primary_event_receipts")
        .fetch_one(database.database.pool())
        .await?;
    let work: i64 = sqlx::query_scalar("select count(*) from knowledge.analysis_work")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(receipts, 2, "valid delivery after poison was not admitted");
    assert_eq!(work, 1, "equal source revision created duplicate work");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn artifact_must_match_its_import_provenance() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let intake = PrimaryAdmissionStore::new(&database.database);
    let cross_owner_artifact = artifact_envelope(Uuid::now_v7());

    assert_eq!(
        intake
            .admit(
                "evt.ai_archive.artifact.added.v1",
                &serde_json::to_vec(&cross_owner_artifact)?,
            )
            .await?,
        AdmissionDisposition::Rejected,
        "artifact identity escaped its import provenance"
    );
    let mut digest_mismatch = artifact_envelope(Uuid::now_v7());
    normalize_artifact_identity(&mut digest_mismatch)?;
    set_json_pointer(
        &mut digest_mismatch,
        "/payload/artifact/content_digest/hex",
        serde_json::json!("6".repeat(64)),
    )?;
    assert_eq!(
        intake
            .admit(
                "evt.ai_archive.artifact.added.v1",
                &serde_json::to_vec(&digest_mismatch)?,
            )
            .await?,
        AdmissionDisposition::Rejected,
        "artifact content digest disagreed with its immutable blob"
    );
    let mut foreign_export = artifact_envelope(Uuid::now_v7());
    normalize_artifact_identity(&mut foreign_export)?;
    set_json_pointer(
        &mut foreign_export,
        "/payload/import_provenance/source_export/owner_service",
        serde_json::json!("ratatoskr-claude"),
    )?;
    assert_eq!(
        intake
            .admit(
                "evt.ai_archive.artifact.added.v1",
                &serde_json::to_vec(&foreign_export)?,
            )
            .await?,
        AdmissionDisposition::Rejected,
        "archive provenance escaped the authenticated producer"
    );
    assert_counts(&database, 0, 0, 3).await?;
    database.cleanup().await?;
    Ok(())
}

fn normalize_artifact_identity(
    envelope: &mut serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    set_json_pointer(
        envelope,
        "/payload/artifact/provider",
        serde_json::json!("chatgpt"),
    )?;
    set_json_pointer(
        envelope,
        "/payload/artifact/owner",
        serde_json::json!(OWNER),
    )
}

fn set_json_pointer(
    envelope: &mut serde_json::Value,
    pointer: &str,
    value: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let field = envelope
        .pointer_mut(pointer)
        .ok_or("test envelope is missing an expected field")?;
    *field = value;
    Ok(())
}

async fn assert_counts(
    database: &TestDatabase,
    receipts: i64,
    work: i64,
    rejections: i64,
) -> Result<(), sqlx::Error> {
    let actual: (i64, i64, i64) = sqlx::query_as(
        "select
             (select count(*) from knowledge.primary_event_receipts),
             (select count(*) from knowledge.analysis_work),
             (select count(*) from knowledge.primary_event_rejections)",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(actual, (receipts, work, rejections));
    Ok(())
}

fn archive_conversation_envelope(event_id: Uuid) -> serde_json::Value {
    serde_json::json!({
        "event_id": event_id,
        "event_type": "ai_archive.conversation.added.v1",
        "occurred_at": "2026-08-18T11:31:00Z",
        "producer": "ratatoskr-chatgpt",
        "aggregate_id": format!("ai_archive:{ARCHIVE}"),
        "correlation_id": format!("event:{event_id}"),
        "tenant_id": OWNER,
        "schema_version": 1,
        "payload": {
            "import_provenance": {
                "ai_archive_id": ARCHIVE,
                "provider": "chatgpt",
                "owner": OWNER,
                "source_export": {
                    "owner_service": "ratatoskr-chatgpt",
                    "digest": {
                        "algorithm": "sha256",
                        "hex": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    },
                    "media_type": "application/zip",
                    "length_bytes": 2_097_152
                },
                "imported_at": "2026-08-18T11:30:00Z",
                "parser_name": "chatgpt_export",
                "parser_version": "2026.08.1"
            },
            "conversation": {
                "ai_conversation_id": "018f0000-0000-7000-8000-000000000403",
                "provider": "chatgpt",
                "owner": OWNER,
                "messages": [{
                    "external_message_id": "msg-0001",
                    "author_role": "user",
                    "parts": [{"part_kind": "text", "text": "Explain E0597."}],
                    "parser_name": "chatgpt_export",
                    "parser_version": "2026.08.1"
                }],
                "content_digest": {
                    "algorithm": "sha256",
                    "hex": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                },
                "parser_name": "chatgpt_export",
                "parser_version": "2026.08.1"
            }
        }
    })
}

fn artifact_envelope(event_id: Uuid) -> serde_json::Value {
    serde_json::json!({
        "event_id": event_id,
        "event_type": "ai_archive.artifact.added.v1",
        "occurred_at": "2026-08-18T11:31:00Z",
        "producer": "ratatoskr-chatgpt",
        "aggregate_id": format!("ai_archive:{ARCHIVE}"),
        "correlation_id": format!("event:{event_id}"),
        "tenant_id": OWNER,
        "schema_version": 1,
        "payload": {
            "import_provenance": {
                "ai_archive_id": ARCHIVE,
                "provider": "chatgpt",
                "owner": OWNER,
                "source_export": {
                    "owner_service": "ratatoskr-chatgpt",
                    "digest": {"algorithm": "sha256", "hex": "4".repeat(64)},
                    "media_type": "application/zip",
                    "length_bytes": 1024
                },
                "imported_at": "2026-08-18T11:30:00Z",
                "parser_name": "chatgpt_export",
                "parser_version": "2026.08.1"
            },
            "artifact": {
                "external_artifact_id": "artifact-1",
                "provider": "claude",
                "owner": "user:018f0000-0000-7000-8000-000000000999",
                "artifact_kind": "artifact",
                "content_blob": {
                    "owner_service": "ratatoskr-chatgpt",
                    "digest": {"algorithm": "sha256", "hex": "5".repeat(64)},
                    "media_type": "application/octet-stream",
                    "length_bytes": 8
                },
                "content_digest": {"algorithm": "sha256", "hex": "5".repeat(64)},
                "parser_name": "chatgpt_export",
                "parser_version": "2026.08.1"
            }
        }
    })
}
