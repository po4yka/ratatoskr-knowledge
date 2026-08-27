//! Social and archive source-delivery replay tests.

#![allow(clippy::expect_used, clippy::panic, reason = "fixture assertions")]

use ratatoskr_identifiers::DigestHex;
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    FamilyValidationError, SourceInbox, SourceInboxAdmission, validate_archive_analysis,
};

const SOCIAL: &str = r#"{
  "social_source_id":"018f0000-0000-7000-8000-000000000201",
  "platform":"x", "external_post_id":"1234567890123456789",
  "owner":"user:018f0000-0000-7000-8000-000000000005",
  "captured_at":"2026-08-17T10:00:00Z", "text":"A useful post.",
  "content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
  "acquisition":"official_api", "saved_authority":"authoritative_platform_state",
  "completeness":"complete", "upstream_availability":"available"
}"#;

const ARCHIVE_CONVERSATION: &str = r#"{
  "ai_conversation_id":"018f0000-0000-7000-8000-000000000403",
  "provider":"chatgpt", "owner":"user:018f0000-0000-7000-8000-000000000005",
  "messages":[{"external_message_id":"msg-0001","author_role":"user",
    "parts":[{"part_kind":"text","text":"Explain a borrow error."}],
    "parser_name":"chatgpt_export","parser_version":"2026.08.1"}],
  "content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
  "parser_name":"chatgpt_export","parser_version":"2026.08.1"
}"#;

#[test]
fn archive_analysis_rejects_a_decision_citing_an_absent_message()
-> Result<(), Box<dyn std::error::Error>> {
    let conversation: ratatoskr_ai_archive_contracts::AiConversation =
        serde_json::from_str(ARCHIVE_CONVERSATION)?;
    let response = serde_json::json!({
        "summary": "A short answer was requested.", "summary_message_ids": ["msg-0001"],
        "decisions": [{"text": "Use a concise explanation.", "message_id": "absent"}]
    });
    assert_eq!(
        validate_archive_analysis(&response, &conversation),
        Err(FamilyValidationError::Citation)
    );
    Ok(())
}

#[tokio::test]
async fn social_delivery_is_idempotent_and_a_delayed_fact_cannot_regress_the_head()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let inbox = SourceInbox::new(&database.database);
    let social: ratatoskr_social_contracts::SocialSourceSnapshot = serde_json::from_str(SOCIAL)?;
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000801")?;
    assert_eq!(
        inbox
            .accept_social(event_id, "social.source.captured.v1", &social)
            .await?,
        SourceInboxAdmission::AcceptedCurrent
    );
    assert_eq!(
        inbox
            .accept_social(event_id, "social.source.captured.v1", &social)
            .await?,
        SourceInboxAdmission::Duplicate
    );
    let mut delayed = social.clone();
    delayed.captured_at = "2026-08-16T10:00:00Z".parse()?;
    delayed.content_digest.hex =
        DigestHex::parse("1111111111111111111111111111111111111111111111111111111111111111")?;
    assert_eq!(
        inbox
            .accept_social(
                uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000802")?,
                "social.source.updated.v1",
                &delayed,
            )
            .await?,
        SourceInboxAdmission::AcceptedHistorical
    );
    let receipts: i64 = sqlx::query_scalar("select count(*) from knowledge.source_analysis_inbox")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(receipts, 2);
    let head_digest: String = sqlx::query_scalar(
        "select content_digest_hex from knowledge.source_analysis_heads where family = 'social'",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(head_digest, social.content_digest.hex.as_str());
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn archive_conversation_redelivery_creates_one_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let inbox = SourceInbox::new(&database.database);
    let conversation: ratatoskr_ai_archive_contracts::AiConversation =
        serde_json::from_str(ARCHIVE_CONVERSATION)?;
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000803")?;
    let observed_at = "2026-08-17T10:00:00Z".parse()?;
    assert_eq!(
        inbox
            .accept_ai_conversation(
                event_id,
                "ai_archive.conversation.added.v1",
                observed_at,
                &conversation,
            )
            .await?,
        SourceInboxAdmission::AcceptedCurrent
    );
    assert_eq!(
        inbox
            .accept_ai_conversation(
                event_id,
                "ai_archive.conversation.added.v1",
                observed_at,
                &conversation,
            )
            .await?,
        SourceInboxAdmission::Duplicate
    );
    database.cleanup().await?;
    Ok(())
}
