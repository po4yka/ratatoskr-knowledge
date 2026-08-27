//! Social and archive source-delivery replay tests.

#![allow(clippy::expect_used, clippy::panic, reason = "fixture assertions")]

use ratatoskr_ai_archive_contracts::{AiArchiveProvenance, AiArchiveTombstone};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType, TenantRef,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    FamilyValidationError, SourceInbox, SourceInboxAdmission, SourceInboxError, SourceReference,
    validate_archive_analysis,
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

const ARCHIVE_PROVENANCE: &str = r#"{
  "ai_archive_id":"018f0000-0000-7000-8000-000000000402",
  "provider":"chatgpt", "owner":"user:018f0000-0000-7000-8000-000000000005",
  "source_export":{"owner_service":"ratatoskr-chatgpt","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/json","length_bytes":512},
  "imported_at":"2026-08-17T10:00:00Z", "parser_name":"chatgpt_export", "parser_version":"2026.08.1"
}"#;

async fn seed_archive_derived_state(
    database: &TestDatabase,
    owner: TenantRef,
    archive_id: &str,
    document_id: &str,
) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")?,
    };
    let source = database
        .database
        .register_source(&SourceReference {
            tenant: owner,
            owner_context: "ratatoskr-knowledge".to_owned(),
            ai_archive_id: archive_id.to_owned(),
            document_id: DocumentId::parse(document_id)?,
            content_digest: digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse("ratatoskr-knowledge")?,
                digest,
                media_type: MediaType::parse("application/json")?,
                length_bytes: 512,
            },
        })
        .await?;
    let run_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_runs
             (run_id, source_ref_id, contract_version, prompt_version,
              context_builder_version, model_policy, state)
         values ($1, $2, 'archive.v1', 'archive.prompt', 'archive.context',
                 'archive.model', 'completed')",
    )
    .bind(run_id)
    .bind(source.id)
    .execute(database.database.pool())
    .await?;
    let output_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_outputs
             (output_id, run_id, result, raw_response, accepted)
         values ($1, $2, '{}'::jsonb, '{}'::jsonb, true)",
    )
    .bind(output_id)
    .bind(run_id)
    .execute(database.database.pool())
    .await?;
    let document_uuid = uuid::Uuid::parse_str(document_id)?;
    sqlx::query(
        "insert into knowledge.search_projection_inputs
             (source_ref_id, latest_output_id, tenant_ref, owner_context,
              document_id, title, lead, body, updated_at)
         values ($1, $2, $3, 'ratatoskr-knowledge', $4,
                 'Synthetic title', 'Synthetic lead', 'Synthetic body', now())",
    )
    .bind(source.id)
    .bind(output_id)
    .bind(owner.to_string())
    .bind(document_uuid)
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.search_documents
             (search_document_id, source_ref_id, latest_output_id, tenant_ref,
              owner_context, document_id, title, lead, body, updated_at)
         values ($1, $2, $3, $4, 'ratatoskr-knowledge', $5,
                 'Synthetic title', 'Synthetic lead', 'Synthetic body', now())",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(source.id)
    .bind(output_id)
    .bind(owner.to_string())
    .bind(document_uuid)
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.embedding_chunks
             (embedding_chunk_id, source_ref_id, output_id, tenant_ref, owner_context,
              document_id, ordinal, chunk_text, chunk_digest_hex, chunking_version,
              provider, model, dimensions, prompt_version, embedding)
         values ($1, $2, $3, $4, 'ratatoskr-knowledge', $5, 0, 'Synthetic chunk',
                 $6, 'archive.chunking', 'synthetic', 'synthetic-embedding', 1536,
                 'archive.embedding', $7)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(source.id)
    .bind(output_id)
    .bind(owner.to_string())
    .bind(document_uuid)
    .bind("1".repeat(64))
    .bind(pgvector::Vector::from(vec![0.0_f32; 1_536]))
    .execute(database.database.pool())
    .await?;
    Ok(source.id)
}

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
    let provenance: AiArchiveProvenance = serde_json::from_str(ARCHIVE_PROVENANCE)?;
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000803")?;
    assert_eq!(
        inbox
            .accept_ai_conversation(
                event_id,
                "ai_archive.conversation.added.v1",
                &provenance,
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
                &provenance,
                &conversation,
            )
            .await?,
        SourceInboxAdmission::Duplicate
    );
    database.cleanup().await?;
    Ok(())
}

/// An authoritative archive tombstone is a distinct, idempotent inbox receipt that atomically
/// removes only the derived source revisions carrying that archive provenance.
#[tokio::test]
async fn archive_tombstone_is_deduplicated_and_scoped() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let inbox = SourceInbox::new(&database.database);
    let owner: TenantRef = "user:018f0000-0000-7000-8000-000000000005".parse()?;
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")?,
    };
    let archived = database
        .database
        .register_source(&SourceReference {
            tenant: owner,
            owner_context: "ratatoskr-knowledge".to_owned(),
            ai_archive_id: "018f0000-0000-7000-8000-000000000402".to_owned(),
            document_id: DocumentId::parse("018f0000-0000-7000-8000-000000000403")?,
            content_digest: digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse("ratatoskr-knowledge")?,
                digest: digest.clone(),
                media_type: MediaType::parse("application/json")?,
                length_bytes: 512,
            },
        })
        .await?;
    let retained = database
        .database
        .register_source(&SourceReference {
            tenant: owner,
            owner_context: "ratatoskr-knowledge".to_owned(),
            ai_archive_id: "018f0000-0000-7000-8000-000000000499".to_owned(),
            document_id: DocumentId::new_v7(),
            content_digest: digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse("ratatoskr-knowledge")?,
                digest: digest.clone(),
                media_type: MediaType::parse("application/json")?,
                length_bytes: 512,
            },
        })
        .await?;
    let tombstone: AiArchiveTombstone = serde_json::from_str(
        r#"{
          "ai_archive_id":"018f0000-0000-7000-8000-000000000402",
          "provider":"chatgpt",
          "owner":"user:018f0000-0000-7000-8000-000000000005",
          "subject":{"subject_kind":"archive"},
          "reason":"provider_deletion_event",
          "evidence_ref":{"owner_service":"ratatoskr-chatgpt","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/json","length_bytes":512},
          "observed_at":"2026-08-27T06:00:00Z"
        }"#,
    )?;
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000804")?;

    assert_eq!(
        inbox
            .accept_ai_tombstone(event_id, "ai_archive.subject.tombstoned.v1", &tombstone)
            .await?,
        SourceInboxAdmission::AcceptedCurrent
    );
    let removed: i64 =
        sqlx::query_scalar("select count(*) from knowledge.source_refs where source_ref_id = $1")
            .bind(archived.id)
            .fetch_one(database.database.pool())
            .await?;
    let still_present: i64 =
        sqlx::query_scalar("select count(*) from knowledge.source_refs where source_ref_id = $1")
            .bind(retained.id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(removed, 0);
    assert_eq!(still_present, 1);
    assert_eq!(
        inbox
            .accept_ai_tombstone(event_id, "ai_archive.subject.tombstoned.v1", &tombstone)
            .await?,
        SourceInboxAdmission::Duplicate
    );
    let receipt: (String, String) = sqlx::query_as(
        "select tenant_ref, source_id from knowledge.source_analysis_inbox where event_id = $1",
    )
    .bind(event_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(
        receipt,
        (
            "user:018f0000-0000-7000-8000-000000000005".to_owned(),
            "018f0000-0000-7000-8000-000000000402".to_owned(),
        )
    );
    database.cleanup().await?;
    Ok(())
}

/// The additive owner-requested authority token reaches the same complete, tenant-scoped
/// deletion transaction and remains replay-safe.
#[tokio::test]
async fn user_requested_archive_tombstone_is_deduplicated_and_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let inbox = SourceInbox::new(&database.database);
    let owner: TenantRef = "user:018f0000-0000-7000-8000-000000000005".parse()?;
    let target = seed_archive_derived_state(
        &database,
        owner,
        "018f0000-0000-7000-8000-000000000402",
        "018f0000-0000-7000-8000-000000000403",
    )
    .await?;
    let sibling = seed_archive_derived_state(
        &database,
        owner,
        "018f0000-0000-7000-8000-000000000499",
        "018f0000-0000-7000-8000-000000000404",
    )
    .await?;
    let parsed = serde_json::from_str::<AiArchiveTombstone>(
        r#"{
          "ai_archive_id":"018f0000-0000-7000-8000-000000000402",
          "provider":"chatgpt",
          "owner":"user:018f0000-0000-7000-8000-000000000005",
          "subject":{"subject_kind":"conversation","ai_conversation_id":"018f0000-0000-7000-8000-000000000403"},
          "reason":"user_requested",
          "evidence_ref":{"owner_service":"ratatoskr-chatgpt","digest":{"algorithm":"sha256","hex":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},"media_type":"application/json","length_bytes":256},
          "observed_at":"2026-08-27T07:00:00Z"
        }"#,
    );
    assert!(
        parsed.is_ok(),
        "the consumer contract must accept user_requested: {parsed:?}"
    );
    let tombstone = parsed.expect("the preceding assertion proves the tombstone parsed");
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000805")?;

    assert_eq!(
        inbox
            .accept_ai_tombstone(event_id, "ai_archive.subject.tombstoned.v1", &tombstone)
            .await?,
        SourceInboxAdmission::AcceptedCurrent
    );
    assert_eq!(
        inbox
            .accept_ai_tombstone(event_id, "ai_archive.subject.tombstoned.v1", &tombstone)
            .await?,
        SourceInboxAdmission::Duplicate
    );

    for table in [
        "source_refs",
        "analysis_runs",
        "search_projection_inputs",
        "search_documents",
        "embedding_chunks",
    ] {
        let target_count: i64 = sqlx::query_scalar(&format!(
            "select count(*) from knowledge.{table} where source_ref_id = $1"
        ))
        .bind(target)
        .fetch_one(database.database.pool())
        .await?;
        let sibling_count: i64 = sqlx::query_scalar(&format!(
            "select count(*) from knowledge.{table} where source_ref_id = $1"
        ))
        .bind(sibling)
        .fetch_one(database.database.pool())
        .await?;
        assert_eq!(target_count, 0, "target rows remain in {table}");
        assert_eq!(sibling_count, 1, "sibling rows were removed from {table}");
    }
    let target_outputs: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.analysis_outputs o
         join knowledge.analysis_runs r on r.run_id = o.run_id
         where r.source_ref_id = $1",
    )
    .bind(target)
    .fetch_one(database.database.pool())
    .await?;
    let sibling_outputs: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.analysis_outputs o
         join knowledge.analysis_runs r on r.run_id = o.run_id
         where r.source_ref_id = $1",
    )
    .bind(sibling)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(target_outputs, 0, "target analysis output remains");
    assert_eq!(sibling_outputs, 1, "sibling analysis output was removed");
    let deletion_records: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.deletion_records
         where tenant_ref = $1 and source_document_id = $2",
    )
    .bind(owner.to_string())
    .bind("018f0000-0000-7000-8000-000000000403")
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(deletion_records, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn user_requested_tombstone_refuses_a_cross_tenant_subject()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let inbox = SourceInbox::new(&database.database);
    let actual_owner: TenantRef = "user:018f0000-0000-7000-8000-000000000005".parse()?;
    let source_id = seed_archive_derived_state(
        &database,
        actual_owner,
        "018f0000-0000-7000-8000-000000000402",
        "018f0000-0000-7000-8000-000000000403",
    )
    .await?;
    let tombstone: AiArchiveTombstone = serde_json::from_str(
        r#"{
          "ai_archive_id":"018f0000-0000-7000-8000-000000000402",
          "provider":"chatgpt",
          "owner":"user:018f0000-0000-7000-8000-000000000006",
          "subject":{"subject_kind":"conversation","ai_conversation_id":"018f0000-0000-7000-8000-000000000403"},
          "reason":"user_requested",
          "evidence_ref":{"owner_service":"ratatoskr-chatgpt","digest":{"algorithm":"sha256","hex":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},"media_type":"application/json","length_bytes":256},
          "observed_at":"2026-08-27T07:00:00Z"
        }"#,
    )?;
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000806")?;

    let admission = inbox
        .accept_ai_tombstone(event_id, "ai_archive.subject.tombstoned.v1", &tombstone)
        .await;
    assert!(
        matches!(admission, Err(SourceInboxError::InvalidArchiveFact)),
        "a known subject owned only by another tenant must be refused: {admission:?}"
    );
    let source_count: i64 =
        sqlx::query_scalar("select count(*) from knowledge.source_refs where source_ref_id = $1")
            .bind(source_id)
            .fetch_one(database.database.pool())
            .await?;
    let receipt_count: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.source_analysis_inbox where event_id = $1",
    )
    .bind(event_id)
    .fetch_one(database.database.pool())
    .await?;
    let deletion_count: i64 = sqlx::query_scalar("select count(*) from knowledge.deletion_records")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!((source_count, receipt_count, deletion_count), (1, 0, 0));

    database.cleanup().await?;
    Ok(())
}
