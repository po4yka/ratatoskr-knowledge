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
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, Extensions, MediaType,
    RepositoryAnalysisRequestId, RepositoryId, TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    BlobStore, BudgetLedger, BudgetLimits, ControlledProvider, FamilyPipeline, FamilyPipelineError,
    ProviderError, ProviderResponse, ProviderUsage, RateLimiter, RepositoryReadmeError,
    RepositoryReadmeResolver, ScriptedProvider, SourceInbox, SpendControls, TokenPrices,
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
    let conversation: ratatoskr_ai_archive_contracts::AiConversation =
        serde_json::from_str(ARCHIVE)?;
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
        .execute_repository(&request, &StaticReadme { bytes: readme })
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
    let archive: ratatoskr_ai_archive_contracts::AiConversation = serde_json::from_str(ARCHIVE)?;
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
