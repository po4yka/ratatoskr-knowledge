//! Helpers shared by the durable pipeline integration-test binaries.

use std::sync::Arc;

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::{FakeTransport, TestDatabase};
use ratatoskr_knowledge::{
    AnalysisIdentity, BudgetLedger, BudgetLimits, ControlledProvider, OpenRouterProvider,
    OpenRouterSettings, ProviderResponse, ProviderSecret, ProviderUsage, RateLimiter, RetryPolicy,
    SourceReference, SpendControls, TokenPrices, prepare_context,
};

pub(crate) async fn run_and_context(
    database: &TestDatabase,
) -> Result<(uuid::Uuid, ratatoskr_knowledge::PreparedContext, Document), Box<dyn std::error::Error>>
{
    let document = Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:pipeline")?,
        content_digest: digest('a')?,
        title: Some("Pipeline".to_owned()),
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            text: "Evidence.".to_owned(),
        }],
        provenance: Vec::new(),
    };
    let source = database
        .database
        .register_source(&SourceReference {
            tenant: TenantRef::of_user(UserId::new_v7()),
            owner_context: "ratatoskr-extractor".to_owned(),
            document_id: document.document_id,
            content_digest: document.content_digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse("ratatoskr-extractor")?,
                digest: document.content_digest.clone(),
                media_type: MediaType::parse("application/json")?,
                length_bytes: 128,
            },
        })
        .await?;
    let run = database
        .database
        .create_run(&AnalysisIdentity {
            source_revision_id: source.id,
            contract_version: "article_v1".to_owned(),
            prompt_version: "article_prompt_v1".to_owned(),
            context_builder_version: "document_context_v1".to_owned(),
            model_policy: "fake_default_v1".to_owned(),
        })
        .await?;
    Ok((run.id, prepare_context(&document, 1_000)?, document))
}

pub(crate) fn digest(digit: char) -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    })
}

pub(crate) fn valid_response(request_id: &str) -> ProviderResponse {
    ProviderResponse {
        bytes: br#"{
            "summary": "A grounded summary.",
            "key_points": [{"text": "Evidence exists.", "source_block_indexes": [0]}]
        }"#
        .to_vec(),
        request_id: Some(request_id.to_owned()),
        usage: ProviderUsage {
            input_tokens: 20,
            output_tokens: 10,
        },
    }
}

pub(crate) fn controlled_openrouter(
    database: &TestDatabase,
    transport: &FakeTransport,
    max_tries: u32,
) -> Result<ControlledProvider<OpenRouterProvider>, Box<dyn std::error::Error>> {
    let inner = OpenRouterProvider::new(OpenRouterSettings {
        base_url: format!("http://{}/api/v1", transport.local_addr()),
        model: "openai/gpt-oss-20b".to_owned(),
        credential: ProviderSecret::new("sk-or-v1-LEAKME".to_owned()),
        max_output_tokens: 16,
        response_byte_cap: 4_096,
        call_deadline: std::time::Duration::from_secs(2),
        connect_timeout: std::time::Duration::from_secs(2),
        retry: RetryPolicy::new(max_tries, 0, 0),
    })?;
    Ok(ControlledProvider::new(
        inner,
        Arc::new(RateLimiter::new(std::time::Duration::ZERO)),
        BudgetLedger::new(database.database.pool().clone()),
        SpendControls {
            limits: BudgetLimits {
                daily_tokens: u64::MAX - 1,
                monthly_tokens: u64::MAX - 1,
                daily_cost_micro_usd: u64::MAX - 1,
                monthly_cost_micro_usd: u64::MAX - 1,
            },
            prices: TokenPrices {
                input_micro_usd_per_mtoken: 0,
                output_micro_usd_per_mtoken: 0,
            },
            max_output_tokens: 16,
        },
    ))
}
