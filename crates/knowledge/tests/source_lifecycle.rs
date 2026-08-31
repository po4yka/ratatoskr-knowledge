//! Owner-scoped social lifecycle and stale-replay regression tests.

use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{AdmissionDisposition, PrimaryAdmissionStore};
use uuid::Uuid;

const SOURCE: &str = "018f0000-0000-7000-8000-000000000201";
const OWNER_A: &str = "user:018f0000-0000-7000-8000-000000000005";
const OWNER_B: &str = "user:018f0000-0000-7000-8000-000000000006";
const ARCHIVE: &str = "018f0000-0000-7000-8000-000000000402";
const CONVERSATION: &str = "018f0000-0000-7000-8000-000000000403";

#[tokio::test]
async fn source_update_and_removal_retire_search_and_block_stale_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let intake = PrimaryAdmissionStore::new(&database.database);

    assert_eq!(
        admit(&intake, capture(OWNER_A, "2026-08-17T10:00:00Z", 'a')).await?,
        AdmissionDisposition::Accepted
    );
    seed_search_projection(&database, OWNER_A).await?;
    let mut newer_payload = capture(OWNER_A, "2026-08-17T10:02:00Z", 'b');
    newer_payload["occurred_at"] = serde_json::json!("2026-08-17T09:00:00Z");
    assert_eq!(
        admit(&intake, newer_payload).await?,
        AdmissionDisposition::Accepted
    );
    let searchable_after_update: i64 =
        sqlx::query_scalar("select count(*) from knowledge.search_documents where tenant_ref = $1")
            .bind(OWNER_A)
            .fetch_one(database.database.pool())
            .await?;
    let historical_outputs: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.analysis_outputs output
         join knowledge.analysis_runs run on run.run_id = output.run_id
         join knowledge.source_refs source on source.source_ref_id = run.source_ref_id
         where source.tenant_ref = $1",
    )
    .bind(OWNER_A)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(
        searchable_after_update, 0,
        "superseded revision stayed searchable"
    );
    assert_eq!(
        historical_outputs, 1,
        "source update deleted analysis history"
    );
    let mut older_payload = capture(OWNER_A, "2026-08-17T10:01:00Z", 'c');
    older_payload["occurred_at"] = serde_json::json!("2026-08-17T12:00:00Z");
    assert_eq!(
        admit(&intake, older_payload).await?,
        AdmissionDisposition::Suppressed,
        "envelope time overrode the authoritative captured_at"
    );
    assert_eq!(
        admit(&intake, capture(OWNER_B, "2026-08-17T09:00:00Z", 'd')).await?,
        AdmissionDisposition::Accepted
    );
    assert_eq!(
        admit(&intake, removal(OWNER_A, "2026-08-17T10:03:00Z")).await?,
        AdmissionDisposition::Suppressed
    );
    let owner_a_states: Vec<String> = sqlx::query_scalar(
        "select work.state from knowledge.analysis_work work
         join knowledge.primary_event_receipts receipt on receipt.event_id = work.event_id
         where receipt.tenant_ref = $1 order by work.created_at",
    )
    .bind(OWNER_A)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(owner_a_states, ["suppressed", "suppressed"]);
    let owner_b_state: String = sqlx::query_scalar(
        "select work.state from knowledge.analysis_work work
         join knowledge.primary_event_receipts receipt on receipt.event_id = work.event_id
         where receipt.tenant_ref = $1",
    )
    .bind(OWNER_B)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(owner_b_state, "admitted", "another owner was suppressed");
    let derived: i64 =
        sqlx::query_scalar("select count(*) from knowledge.search_documents where tenant_ref = $1")
            .bind(OWNER_A)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(derived, 0, "removed source retained a search projection");

    assert_eq!(
        admit(&intake, capture(OWNER_A, "2026-08-17T10:02:30Z", 'e')).await?,
        AdmissionDisposition::Suppressed,
        "stale replay resurrected removed source work"
    );
    let head: (String, String) = sqlx::query_as(
        "select lifecycle, revision from knowledge.primary_source_heads
         where family = 'social' and tenant_ref = $1 and source_key = $2",
    )
    .bind(OWNER_A)
    .bind(SOURCE)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(head.0, "removed");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn archive_tombstone_blocks_stale_child_replay() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let intake = PrimaryAdmissionStore::new(&database.database);

    assert_eq!(
        admit(&intake, archive_conversation("2026-08-17T11:00:00Z")).await?,
        AdmissionDisposition::Accepted
    );
    assert_eq!(
        admit(&intake, archive_tombstone("2026-08-17T11:02:00Z")).await?,
        AdmissionDisposition::Suppressed
    );
    let archive_state: String = sqlx::query_scalar(
        "select state from knowledge.analysis_work
         where tenant_ref = $1 and parent_source_key = $2",
    )
    .bind(OWNER_A)
    .bind(format!("archive:{ARCHIVE}"))
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(archive_state, "suppressed");
    assert_eq!(
        admit(&intake, archive_conversation("2026-08-17T11:01:00Z")).await?,
        AdmissionDisposition::Suppressed,
        "an archive-wide tombstone allowed stale child replay"
    );
    let retained_tombstone: (String, String) = sqlx::query_as(
        "select lifecycle, source_key from knowledge.primary_source_state
         where family = 'ai_archive' and tenant_ref = $1 and source_key = $2",
    )
    .bind(OWNER_A)
    .bind(format!("archive:{ARCHIVE}"))
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(
        retained_tombstone,
        ("removed".to_owned(), format!("archive:{ARCHIVE}"))
    );

    database.cleanup().await?;
    Ok(())
}

fn archive_conversation(occurred_at: &str) -> serde_json::Value {
    let event_id = Uuid::now_v7();
    serde_json::json!({
        "event_id": event_id,
        "event_type": "ai_archive.conversation.added.v1",
        "occurred_at": occurred_at,
        "producer": "ratatoskr-chatgpt",
        "aggregate_id": format!("ai_archive:{ARCHIVE}"),
        "correlation_id": format!("event:{event_id}"),
        "tenant_id": OWNER_A,
        "schema_version": 1,
        "payload": {
            "import_provenance": {
                "ai_archive_id": ARCHIVE,
                "provider": "chatgpt",
                "owner": OWNER_A,
                "source_export": {
                    "owner_service": "ratatoskr-chatgpt",
                    "digest": {"algorithm":"sha256", "hex":"1".repeat(64)},
                    "media_type":"application/json", "length_bytes":512
                },
                "imported_at":"2026-08-17T10:59:00Z",
                "parser_name":"chatgpt_export", "parser_version":"2026.08.1"
            },
            "conversation": {
                "ai_conversation_id": CONVERSATION,
                "provider":"chatgpt", "owner":OWNER_A,
                "messages":[{
                    "external_message_id":"msg-1", "author_role":"user",
                    "parts":[{"part_kind":"text", "text":"durable archive"}],
                    "parser_name":"chatgpt_export", "parser_version":"2026.08.1"
                }],
                "content_digest":{"algorithm":"sha256", "hex":"2".repeat(64)},
                "parser_name":"chatgpt_export", "parser_version":"2026.08.1"
            }
        }
    })
}

fn archive_tombstone(occurred_at: &str) -> serde_json::Value {
    let event_id = Uuid::now_v7();
    serde_json::json!({
        "event_id": event_id,
        "event_type": "ai_archive.subject.tombstoned.v1",
        "occurred_at": occurred_at,
        "producer": "ratatoskr-chatgpt",
        "aggregate_id": format!("ai_archive:{ARCHIVE}"),
        "correlation_id": format!("event:{event_id}"),
        "tenant_id": OWNER_A,
        "schema_version": 1,
        "payload": {
            "ai_archive_id": ARCHIVE,
            "provider":"chatgpt", "owner":OWNER_A,
            "subject":{"subject_kind":"archive"},
            "reason":"provider_deletion_event",
            "evidence_ref":{
                "owner_service":"ratatoskr-chatgpt",
                "digest":{"algorithm":"sha256", "hex":"3".repeat(64)},
                "media_type":"application/json", "length_bytes":512
            },
            "observed_at":occurred_at
        }
    })
}

async fn admit(
    intake: &PrimaryAdmissionStore<'_>,
    envelope: serde_json::Value,
) -> Result<AdmissionDisposition, Box<dyn std::error::Error>> {
    let event_type = envelope
        .get("event_type")
        .ok_or("event type missing")?
        .as_str()
        .ok_or("event type missing")?;
    Ok(intake
        .admit(
            &format!("evt.{event_type}"),
            &serde_json::to_vec(&envelope)?,
        )
        .await?)
}

fn capture(owner: &str, occurred_at: &str, digest_char: char) -> serde_json::Value {
    let event_id = Uuid::now_v7();
    serde_json::json!({
        "event_id": event_id,
        "event_type": "social.source.captured.v1",
        "occurred_at": occurred_at,
        "producer": "ratatoskr-x",
        "aggregate_id": format!("social_source:{SOURCE}"),
        "correlation_id": format!("event:{event_id}"),
        "tenant_id": owner,
        "schema_version": 1,
        "payload": {"source": {
            "social_source_id": SOURCE,
            "platform": "x",
            "external_post_id": "1234567890",
            "owner": owner,
            "captured_at": occurred_at,
            "text": "durable source",
            "content_digest": {
                "algorithm": "sha256",
                "hex": digest_char.to_string().repeat(64)
            },
            "acquisition": "official_api",
            "saved_authority": "authoritative_platform_state",
            "completeness": "complete",
            "upstream_availability": "available"
        }}
    })
}

fn removal(owner: &str, occurred_at: &str) -> serde_json::Value {
    let event_id = Uuid::now_v7();
    serde_json::json!({
        "event_id": event_id,
        "event_type": "social.source.removed.v1",
        "occurred_at": occurred_at,
        "producer": "ratatoskr-x",
        "aggregate_id": format!("social_source:{SOURCE}"),
        "correlation_id": format!("event:{event_id}"),
        "tenant_id": owner,
        "schema_version": 1,
        "payload": {
            "social_source_id": SOURCE,
            "owner": owner,
            "reason": "user_requested",
            "removed_at": occurred_at
        }
    })
}

async fn seed_search_projection(
    database: &TestDatabase,
    owner: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_ref = Uuid::now_v7();
    let run_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.source_refs (
             source_ref_id, tenant_ref, owner_context, source_document_id,
             content_digest_algorithm, content_digest_hex, source_blob
         ) values ($1, $2, 'ratatoskr-knowledge', $3, 'sha256', $4, $5)",
    )
    .bind(source_ref)
    .bind(owner)
    .bind(SOURCE)
    .bind("f".repeat(64))
    .bind(serde_json::json!({
        "owner_service": "ratatoskr-knowledge",
        "digest": {"algorithm": "sha256", "hex": "f".repeat(64)},
        "media_type": "application/json",
        "length_bytes": 1
    }))
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         ) values ($1, $2, 'social_analysis_v1', 'social_prompt_v1',
             'social_context_v1', 'family_default_v1', 'persisted')",
    )
    .bind(run_id)
    .bind(source_ref)
    .execute(database.database.pool())
    .await?;
    let output_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_outputs (output_id, run_id, result, raw_response)
         values ($1, $2, '{}'::jsonb, '{}'::jsonb)",
    )
    .bind(output_id)
    .bind(run_id)
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref, owner_context,
             document_id, title, lead, body, updated_at
         ) values ($1, $2, $3, $4, 'ratatoskr-knowledge', $5, 'title', '', 'body', now())",
    )
    .bind(Uuid::now_v7())
    .bind(source_ref)
    .bind(output_id)
    .bind(owner)
    .bind(Uuid::parse_str(SOURCE)?)
    .execute(database.database.pool())
    .await?;
    Ok(())
}
