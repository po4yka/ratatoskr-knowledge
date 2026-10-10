//! Helpers shared by the durable pipeline integration-test binaries.

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    AnalysisIdentity, ProviderResponse, ProviderUsage, SourceReference, prepare_context,
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
            block_id: BlockId::new_v7(),
            text: "Evidence.".to_owned(),
        }],
        provenance: Vec::new(),
    };
    let source = database
        .database
        .register_source(&SourceReference {
            tenant: TenantRef::of_user(UserId::new_v7()),
            owner_context: "ratatoskr-extractor".to_owned(),
            ai_archive_id: String::new(),
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
