//! Fixture-event to analysis and search-projection integration tests for every source family.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "synthetic fixtures"
)]

use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ratatoskr_ai_archive_contracts::{AiArchiveProvenance, AiArchiveSubject};
use ratatoskr_github_contracts::{
    ReadmeRevision, RepositoryAnalysisAttributes, RepositoryAnalysisContract,
    RepositoryAnalysisRequested, RepositoryAnalysisRevision, RepositoryFullName,
};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, EntityRef, Extensions,
    MediaType, RepositoryAnalysisRequestId, RepositoryId, TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::archive::conversation_from_json;
use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport, TestDatabase};
use ratatoskr_knowledge::{
    BlobError, BlobStore, BudgetLedger, BudgetLimits, Config, ControlledProvider, EventCause,
    FamilyPipeline, FamilyPipelineError, OpenRouterProvider, OpenRouterSettings, ProviderError,
    ProviderResponse, ProviderSecret, ProviderUsage, RateLimiter, RepositoryReadmeError,
    RepositoryReadmeResolver, RetryPolicy, ScriptedProvider, SearchQuery, SourceInbox,
    SpendControls, TokenPrices, search_page,
};
use sha2::{Digest as _, Sha256};

const SOCIAL: &str = r#"{
  "social_source_id":"018f0000-0000-7000-8000-000000000201",
  "platform":"x", "external_post_id":"1234567890123456789",
  "owner":"user:018f0000-0000-7000-8000-000000000005",
  "captured_at":"2026-08-17T10:00:00Z", "text":"A useful post.",
  "content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
  "acquisition":"official_api", "saved_authority":"authoritative_platform_state",
  "completeness":"complete", "upstream_availability":"available"
}"#;

const ARCHIVE: &str = r#"{
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

const ARCHIVE_PROJECT: &str = r#"{
  "ai_project_id":"018f0000-0000-7000-8000-000000000404",
  "provider":"chatgpt", "title":"Rust learning",
  "description":"Notes about borrow checking.", "instructions":"Keep examples concise.",
  "parser_name":"chatgpt_export", "parser_version":"2026.08.1"
}"#;

#[tokio::test]
async fn social_event_replay_produces_one_analysis_and_search_document()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let snapshot: ratatoskr_social_contracts::SocialSourceSnapshot = serde_json::from_str(SOCIAL)?;
    let inbox = SourceInbox::new(&database.database);
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000801")?;
    inbox
        .accept_social(event_id, "social.source.captured.v1", &snapshot)
        .await?;
    let store = BlobStore::new(temp_root("social"), 32_768);
    let provider = ScriptedProvider::new([Ok(response(
        r#"{"summary":"Useful post.","topics":["rust"],"evidence_excerpt":"useful post","confidence":"grounded"}"#,
    ))]);
    let pipeline = FamilyPipeline::new(
        &database.database,
        &provider,
        &store,
        Duration::from_secs(1),
    );

    assert_eq!(
        pipeline
            .execute_social_event(&inbox, event_id)
            .await?
            .summary,
        "Useful post."
    );
    assert_eq!(
        pipeline
            .execute_social_event(&inbox, event_id)
            .await?
            .summary,
        "Useful post."
    );
    assert_eq!(provider.call_count()?, 1);
    assert_eq!(search_document_count(&database).await?, 1);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn archive_event_produces_grounded_analysis_and_search_document()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let conversation = conversation_from_json(ARCHIVE)?;
    let provenance: AiArchiveProvenance = serde_json::from_str(ARCHIVE_PROVENANCE)?;
    let inbox = SourceInbox::new(&database.database);
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000802")?;
    inbox
        .accept_ai_conversation(
            event_id,
            "ai_archive.conversation.added.v1",
            &provenance,
            &conversation,
        )
        .await?;
    let store = BlobStore::new(temp_root("archive"), 32_768);
    let provider = ScriptedProvider::new([Ok(response(
        r#"{"summary":"The user requested an explanation.","summary_message_ids":["msg-0001"],"decisions":[{"text":"Keep the explanation concise.","message_id":"msg-0001"}]}"#,
    ))]);
    let pipeline = FamilyPipeline::new(
        &database.database,
        &provider,
        &store,
        Duration::from_secs(1),
    );

    let execution = pipeline.execute_archive_event(&inbox, event_id).await?;
    assert_eq!(execution.analysis.decisions.len(), 1);
    assert_eq!(
        execution.completion.subject,
        AiArchiveSubject::Conversation {
            ai_conversation_id: conversation.ai_conversation_id
        }
    );
    assert_eq!(
        execution.completion.content_digest,
        conversation.content_digest
    );
    assert_eq!(execution.completion.ai_archive_id, provenance.ai_archive_id);
    assert_eq!(search_document_count(&database).await?, 1);
    database.cleanup().await?;
    Ok(())
}

/// XR-021 CONTRACTS.md R3-09: the search document of a conversation is its text. The prompt
/// scaffold (`family:`, `conversation_id:`, `message_id:`, `role:`) is evidence for the model, not
/// something a user searched for or should see in a snippet.
#[tokio::test]
async fn a_conversation_is_indexed_without_prompt_scaffolding()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let first = format!(
        "Explain zanzibar borrow errors. {}",
        "Then keep going. ".repeat(30)
    );
    let reply = "A borrow error means a reference outlives its owner.";
    let conversation = conversation_from_json(
        &serde_json::json!({
            "ai_conversation_id": "018f0000-0000-7000-8000-000000000403",
            "provider": "chatgpt",
            "owner": "user:018f0000-0000-7000-8000-000000000005",
            "title": "Borrow checker notes",
            "messages": [
                {"external_message_id": "msg-0001", "author_role": "user",
                 "parts": [{"part_kind": "text", "text": first}],
                 "parser_name": "chatgpt_export", "parser_version": "2026.08.1"},
                {"external_message_id": "msg-0002", "author_role": "assistant",
                 "parts": [{"part_kind": "markdown", "markdown": reply}],
                 "parser_name": "chatgpt_export", "parser_version": "2026.08.1"}
            ],
            "content_digest": {"algorithm": "sha256", "hex": "0".repeat(64)},
            "parser_name": "chatgpt_export", "parser_version": "2026.08.1"
        })
        .to_string(),
    )?;
    let provenance: AiArchiveProvenance = serde_json::from_str(ARCHIVE_PROVENANCE)?;
    let inbox = SourceInbox::new(&database.database);
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000812")?;
    inbox
        .accept_ai_conversation(
            event_id,
            "ai_archive.conversation.added.v1",
            &provenance,
            &conversation,
        )
        .await?;
    let store = BlobStore::new(temp_root("archive-text"), 32_768);
    let provider = ScriptedProvider::new([Ok(response(
        r#"{"summary":"The user requested an explanation.","summary_message_ids":["msg-0001"],"decisions":[{"text":"Keep the explanation concise.","message_id":"msg-0001"}]}"#,
    ))]);
    FamilyPipeline::new(
        &database.database,
        &provider,
        &store,
        Duration::from_secs(1),
    )
    .execute_archive_event(&inbox, event_id)
    .await?;

    let tenant = conversation.owner.to_string();
    for scaffold in ["conversation_id", "message_id", "family", "role"] {
        let page = search_page(
            database.database.pool(),
            &SearchQuery::new(tenant.clone(), scaffold, 10, 0)?,
        )
        .await?;
        assert!(
            page.results.is_empty(),
            "`{scaffold}` is prompt scaffolding and must not find the conversation"
        );
    }
    let page = search_page(
        database.database.pool(),
        &SearchQuery::new(tenant, "zanzibar", 10, 0)?,
    )
    .await?;
    assert_eq!(page.results.len(), 1, "a word from a message finds it");
    let snippet = page.results[0].snippet.clone().unwrap_or_default();
    assert!(snippet.contains("zanzibar"), "{snippet}");
    for scaffold in ["family:", "message_id:", "role:", "conversation_id"] {
        assert!(!snippet.contains(scaffold), "{snippet}");
    }

    let (title, lead, body): (String, String, String) =
        sqlx::query_as("select title, lead, body from knowledge.search_documents")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(title, "Borrow checker notes");
    assert_eq!(lead, first.chars().take(280).collect::<String>());
    assert_eq!(body, format!("{first}\n{reply}"));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn archive_project_event_produces_grounded_analysis_and_search_document()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let project: ratatoskr_ai_archive_contracts::AiProject = serde_json::from_str(ARCHIVE_PROJECT)?;
    let provenance: AiArchiveProvenance = serde_json::from_str(ARCHIVE_PROVENANCE)?;
    let digest = ratatoskr_identifiers::ContentDigest {
        algorithm: ratatoskr_identifiers::DigestAlgorithm::Sha256,
        hex: ratatoskr_identifiers::DigestHex::parse(
            "1111111111111111111111111111111111111111111111111111111111111111",
        )?,
    };
    let inbox = SourceInbox::new(&database.database);
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000803")?;
    inbox
        .accept_ai_project(
            event_id,
            "ai_archive.project.added.v1",
            &provenance,
            &project,
            &digest,
        )
        .await?;
    let store = BlobStore::new(temp_root("archive-project"), 32_768);
    let provider = ScriptedProvider::new([Ok(response(
        r#"{"summary":"Rust learning notes.","topics":["rust"],"evidence_excerpt":"borrow checking"}"#,
    ))]);
    let pipeline = FamilyPipeline::new(
        &database.database,
        &provider,
        &store,
        Duration::from_secs(1),
    );

    assert_eq!(
        pipeline
            .execute_archive_project_event(&inbox, event_id)
            .await?
            .summary,
        "Rust learning notes."
    );
    assert_eq!(search_document_count(&database).await?, 1);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn repository_request_acquires_readme_then_projects_search_document()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let readme = b"# Project\nA Rust library.".to_vec();
    let request = repository_request(&readme)?;
    let store = BlobStore::new(temp_root("repository"), 32_768);
    let provider = ScriptedProvider::new([Ok(response(
        r#"{"summary":"A Rust library.","topics":["rust"],"evidence_excerpt":"Rust library","readme_evidence":"present"}"#,
    ))]);
    let pipeline = FamilyPipeline::new(
        &database.database,
        &provider,
        &store,
        Duration::from_secs(1),
    );

    let execution = pipeline
        .execute_repository(&request, &cause(), &StaticReadme { bytes: readme })
        .await?;
    assert_eq!(execution.analysis.summary, "A Rust library.");
    assert!(execution.completion.is_some());
    assert_eq!(search_document_count(&database).await?, 1);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn one_shared_ledger_blocks_archive_after_social_usage()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let social: ratatoskr_social_contracts::SocialSourceSnapshot = serde_json::from_str(SOCIAL)?;
    let archive = conversation_from_json(ARCHIVE)?;
    let controls = SpendControls {
        limits: BudgetLimits {
            daily_tokens: 300,
            monthly_tokens: 300,
            daily_cost_micro_usd: 1,
            monthly_cost_micro_usd: 1,
        },
        prices: TokenPrices {
            input_micro_usd_per_mtoken: 0,
            output_micro_usd_per_mtoken: 0,
        },
        max_output_tokens: 1,
    };
    let ledger = BudgetLedger::new(database.database.pool().clone());
    let social_inner = ScriptedProvider::new([Ok(ProviderResponse {
        bytes: br#"{"summary":"Useful post.","topics":[],"evidence_excerpt":"useful post","confidence":"grounded"}"#.to_vec(),
        request_id: Some("social-budget".to_owned()), usage: ProviderUsage { input_tokens: 250, output_tokens: 0 },
    })]);
    let social_provider = ControlledProvider::new(
        social_inner,
        Arc::new(RateLimiter::new(Duration::ZERO)),
        ledger.clone(),
        controls,
    );
    let store = BlobStore::new(temp_root("budget"), 32_768);
    FamilyPipeline::new(
        &database.database,
        &social_provider,
        &store,
        Duration::from_secs(1),
    )
    .execute_social(&social)
    .await?;

    let archive_inner = ScriptedProvider::new([Ok(response(
        r#"{"summary":"unused","summary_message_ids":["msg-0001"],"decisions":[]}"#,
    ))]);
    let archive_provider = ControlledProvider::new(
        archive_inner.clone(),
        Arc::new(RateLimiter::new(Duration::ZERO)),
        ledger,
        controls,
    );
    let failure = FamilyPipeline::new(
        &database.database,
        &archive_provider,
        &store,
        Duration::from_secs(1),
    )
    .execute_archive(&archive)
    .await
    .expect_err("shared budget must refuse the second family");
    assert!(matches!(
        failure,
        FamilyPipelineError::Provider(ProviderError::BudgetExhausted)
    ));
    assert_eq!(archive_inner.call_count()?, 0);

    let repository_inner = ScriptedProvider::new([Ok(response(
        r#"{"summary":"unused","topics":[],"evidence_excerpt":"Project","readme_evidence":"present"}"#,
    ))]);
    let repository_provider = ControlledProvider::new(
        repository_inner.clone(),
        Arc::new(RateLimiter::new(Duration::ZERO)),
        BudgetLedger::new(database.database.pool().clone()),
        controls,
    );
    let request = repository_request(b"# Project\nA Rust library.")?;
    let repository_failure = FamilyPipeline::new(
        &database.database,
        &repository_provider,
        &store,
        Duration::from_secs(1),
    )
    .execute_repository(
        &request,
        &cause(),
        &StaticReadme {
            bytes: b"# Project\nA Rust library.".to_vec(),
        },
    )
    .await
    .expect_err("shared budget must also refuse the repository family");
    assert!(matches!(
        repository_failure,
        FamilyPipelineError::Provider(ProviderError::BudgetExhausted)
    ));
    assert_eq!(repository_inner.call_count()?, 0);
    database.cleanup().await?;
    Ok(())
}

/// A conversation of `messages` messages whose text parts are `part_bytes` long and made of
/// distinct words, so the search projection is as wide as a real export.
fn large_conversation(
    messages: usize,
    part_bytes: usize,
) -> Result<ratatoskr_ai_archive_contracts::AiConversation, Box<dyn std::error::Error>> {
    use std::fmt::Write as _;
    let mut counter = 0_u64;
    let mut messages_json = Vec::new();
    for index in 0..messages {
        let mut text = String::with_capacity(part_bytes + 16);
        while text.len() < part_bytes {
            counter += 1;
            let _ignored = write!(text, "w{counter:x} ");
        }
        text.truncate(part_bytes);
        messages_json.push(serde_json::json!({
            "external_message_id": format!("msg-{:04}", index + 1),
            "author_role": if index % 2 == 0 { "user" } else { "assistant" },
            "parts": [{"part_kind": "text", "text": text}],
            "parser_name": "chatgpt_export", "parser_version": "2026.08.1"
        }));
    }
    conversation_from_json(
        &serde_json::json!({
            "ai_conversation_id": "018f0000-0000-7000-8000-000000000403",
            "provider": "chatgpt",
            "owner": "user:018f0000-0000-7000-8000-000000000005",
            "title": "A very long conversation",
            "messages": messages_json,
            "content_digest": {"algorithm": "sha256", "hex": "0".repeat(64)},
            "parser_name": "chatgpt_export", "parser_version": "2026.08.1"
        })
        .to_string(),
    )
}

/// Stores, analyses and searches one conversation through the store the service builds from the
/// default configuration, whose provider response cap is 1 MiB (XR-021 CONTRACTS.md R5-03).
async fn analyse_large_conversation(
    label: &str,
    messages: usize,
    part_bytes: usize,
) -> Result<u64, Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let conversation = large_conversation(messages, part_bytes)?;
    let provenance: AiArchiveProvenance = serde_json::from_str(ARCHIVE_PROVENANCE)?;
    let inbox = SourceInbox::new(&database.database);
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000821")?;
    inbox
        .accept_ai_conversation(
            event_id,
            "ai_archive.conversation.added.v1",
            &provenance,
            &conversation,
        )
        .await?;
    let mut config = Config::default();
    assert_eq!(config.limits.raw_response_bytes, 1_048_576);
    config.storage.blob_root = temp_root(label);
    let store = BlobStore::for_config(&config);
    let provider = ScriptedProvider::new([Ok(response(
        r#"{"summary":"A long conversation.","summary_message_ids":["msg-0001"],"decisions":[]}"#,
    ))]);
    let execution = FamilyPipeline::new(
        &database.database,
        &provider,
        &store,
        Duration::from_secs(5),
    )
    .execute_archive_event(&inbox, event_id)
    .await;
    assert!(
        execution.is_ok(),
        "a snapshot of {messages} x {part_bytes} bytes must be analysed: {execution:?}"
    );
    assert_eq!(search_document_count(&database).await?, 1);
    let stored: i64 = sqlx::query_scalar(
        "select (source_blob->>'length_bytes')::bigint from knowledge.source_refs \
         where owner_context = 'ratatoskr-knowledge'",
    )
    .fetch_one(database.database.pool())
    .await?;
    let page = search_page(
        database.database.pool(),
        &SearchQuery::new(conversation.owner.to_string(), "w1", 10, 0)?,
    )
    .await?;
    assert_eq!(page.results.len(), 1, "the conversation is searchable");
    database.cleanup().await?;
    Ok(u64::try_from(stored)?)
}

#[tokio::test]
async fn a_two_mebibyte_conversation_is_stored_analysed_and_searchable()
-> Result<(), Box<dyn std::error::Error>> {
    let stored = analyse_large_conversation("two-mib", 36, 60_000).await?;
    assert!(stored > 2 * 1_048_576, "{stored}");
    Ok(())
}

#[tokio::test]
async fn a_conversation_of_7_9_million_bytes_is_stored_analysed_and_searchable()
-> Result<(), Box<dyn std::error::Error>> {
    let stored = analyse_large_conversation("seven-nine", 132, 59_800).await?;
    assert!(stored > 7_800_000 && stored <= 8_388_608, "{stored}");
    Ok(())
}

/// The cap on the indexed text is what keeps a long conversation from failing with `PostgreSQL`'s
/// "string is too long for tsvector" error, so it is measured here against the worst case: the
/// widest vector per input byte that English text can make is a run of distinct short words.
#[tokio::test]
async fn the_search_text_cap_keeps_the_worst_case_vector_under_the_postgres_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    // Every two-, three- and four-letter word, shortest first: the shorter the words, the wider
    // the vector per input byte.
    let cap = ratatoskr_knowledge::search::SEARCH_TEXT_MAX_BYTES;
    let mut text = String::new();
    'fill: for length in 2_u32..=4 {
        for index in 0..26_u32.pow(length) {
            let word: String = (0..length)
                .map(|place| {
                    char::from(b'a' + u8::try_from(index / 26_u32.pow(place) % 26).unwrap_or(0))
                })
                .collect();
            text.push_str(&word);
            text.push(' ');
            if text.len() >= cap {
                break 'fill;
            }
        }
    }
    text.truncate(ratatoskr_knowledge::search::SEARCH_TEXT_MAX_BYTES);
    let size: i32 = sqlx::query_scalar("select pg_column_size(to_tsvector('english', $1::text))")
        .bind(&text)
        .fetch_one(database.database.pool())
        .await?;
    assert!(
        size < 1_000_000,
        "the vector of the widest capped text is {size} bytes"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_snapshot_above_blob_bytes_is_rejected_with_a_class_and_no_partial_row()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let conversation = large_conversation(34, 60_000)?;
    let provenance: AiArchiveProvenance = serde_json::from_str(ARCHIVE_PROVENANCE)?;
    let inbox = SourceInbox::new(&database.database);
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000822")?;
    inbox
        .accept_ai_conversation(
            event_id,
            "ai_archive.conversation.added.v1",
            &provenance,
            &conversation,
        )
        .await?;
    let mut config = Config::default();
    config.storage.blob_root = temp_root("above-ceiling");
    config.limits.blob_bytes = 1_048_576;
    let store = BlobStore::for_config(&config);
    let provider = ScriptedProvider::new([]);
    let failure = FamilyPipeline::new(
        &database.database,
        &provider,
        &store,
        Duration::from_secs(5),
    )
    .execute_archive_event(&inbox, event_id)
    .await
    .expect_err("a snapshot above the ceiling must be rejected");
    assert!(
        matches!(failure, FamilyPipelineError::Blob(BlobError::TooLarge)),
        "{failure:?}"
    );
    assert!(failure.is_permanent());
    assert_eq!(provider.call_count()?, 0);
    for table in ["source_refs", "analysis_runs", "search_documents"] {
        let rows: i64 = sqlx::query_scalar(&format!("select count(*) from knowledge.{table}"))
            .fetch_one(database.database.pool())
            .await?;
        assert_eq!(rows, 0, "{table} must hold no partial row");
    }
    database.cleanup().await?;
    Ok(())
}

/// XR-021 CONTRACTS.md R5-03 rule 1: the blob store is no longer capped at `raw_response_bytes`,
/// so the cap on a provider response must hold in the adapter. A response above it is cut while
/// it is read, before anything is stored, whatever the store would have accepted.
#[tokio::test]
async fn a_provider_response_above_the_response_cap_is_cut_before_it_is_stored()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let snapshot: ratatoskr_social_contracts::SocialSourceSnapshot = serde_json::from_str(SOCIAL)?;
    let inbox = SourceInbox::new(&database.database);
    let event_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000823")?;
    inbox
        .accept_social(event_id, "social.source.captured.v1", &snapshot)
        .await?;
    let transport = FakeTransport::start(vec![FakeReply::oversized(200_000)]).await?;
    let provider = OpenRouterProvider::new(OpenRouterSettings {
        base_url: format!("http://{}/api/v1", transport.local_addr()),
        model: "openai/gpt-oss-20b".to_owned(),
        credential: ProviderSecret::new("sk-or-v1-LEAKME".to_owned()),
        max_output_tokens: 2_048,
        response_byte_cap: 4_096,
        call_deadline: Duration::from_secs(2),
        connect_timeout: Duration::from_secs(2),
        retry: RetryPolicy::new(1, 0, 0),
    })?;
    let root = temp_root("response-cap");
    let mut config = Config::default();
    config.storage.blob_root.clone_from(&root);
    let store = BlobStore::for_config(&config);

    let failure = FamilyPipeline::new(
        &database.database,
        &provider,
        &store,
        Duration::from_secs(2),
    )
    .execute_social_event(&inbox, event_id)
    .await
    .expect_err("a response above the cap must fail");

    assert!(
        matches!(
            &failure,
            FamilyPipelineError::Provider(ProviderError::Permanent)
        ),
        "{failure:?}"
    );
    let stored_raw_responses: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.analysis_attempts where raw_response is not null",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(stored_raw_responses, 0, "no raw response was stored");
    let mut largest = 0_u64;
    let mut pending = vec![root];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                largest = largest.max(metadata.len());
            }
        }
    }
    assert!(
        largest <= 4_096,
        "a stored file of {largest} bytes is above the provider response cap"
    );
    database.cleanup().await?;
    Ok(())
}

fn cause() -> EventCause {
    EventCause {
        event_id: uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000901")
            .expect("cause event identity"),
        correlation_id: EntityRef::parse("operation:018f0000-0000-7000-8000-000000000902")
            .expect("cause correlation"),
    }
}

#[derive(Debug)]
struct StaticReadme {
    bytes: Vec<u8>,
}

impl RepositoryReadmeResolver for StaticReadme {
    fn read_readme<'a>(
        &'a self,
        _reference: &'a BlobRef,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, RepositoryReadmeError>> + Send + 'a>> {
        Box::pin(ready(Ok(self.bytes.clone())))
    }
}

fn response(json: &str) -> ProviderResponse {
    ProviderResponse {
        bytes: json.as_bytes().to_vec(),
        request_id: Some("fixture-request".to_owned()),
        usage: ProviderUsage {
            input_tokens: 5,
            output_tokens: 5,
        },
    }
}

async fn search_document_count(database: &TestDatabase) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("select count(*) from knowledge.search_documents")
        .fetch_one(database.database.pool())
        .await
}

fn repository_request(
    readme: &[u8],
) -> Result<RepositoryAnalysisRequested, Box<dyn std::error::Error>> {
    let hex = format!("{:x}", Sha256::digest(readme));
    Ok(RepositoryAnalysisRequested {
        owner: TenantRef::of_user(UserId::new_v7()),
        repository_id: RepositoryId::parse("018f0000-0000-7000-8000-000000000701")?,
        github_repository_numeric_id: 42,
        request_id: RepositoryAnalysisRequestId::parse("018f0000-0000-7000-8000-000000000702")?,
        source_revision: RepositoryAnalysisRevision {
            attributes_digest: digest('a')?,
            readme: ReadmeRevision::Present {
                content_ref: BlobRef {
                    owner_service: BlobOwner::parse("ratatoskr-github")?,
                    digest: ContentDigest {
                        algorithm: DigestAlgorithm::Sha256,
                        hex: DigestHex::parse(&hex)?,
                    },
                    media_type: MediaType::parse("text/markdown")?,
                    length_bytes: u64::try_from(readme.len())?,
                },
            },
        },
        repository_attributes: RepositoryAnalysisAttributes {
            repository_full_name: RepositoryFullName::parse("owner/project")?,
            description: None,
            primary_language: None,
        },
        requested_contract: RepositoryAnalysisContract::RepositoryAnalysis,
        idempotency_key: digest('b')?,
        extensions: Extensions::new(),
    })
}

fn digest(digit: char) -> Result<ContentDigest, Box<dyn std::error::Error>> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    })
}

fn temp_root(family: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ratatoskr-family-{family}-{}",
        uuid::Uuid::now_v7()
    ))
}
