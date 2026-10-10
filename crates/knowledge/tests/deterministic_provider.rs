//! Fixture-mode ingestion: the deterministic first-paragraph provider (XR-021 CONTRACTS.md S09).

use ratatoskr_document_contracts::{
    ContentDocumentExtracted, Document, DocumentAddress, DocumentBlock,
};
use ratatoskr_event_envelope::{EventEnvelope, EventPayload};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, Extensions,
    MediaType,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, DocumentIntake, DocumentIntakeOutcome, ExtractiveArticleProvider, GenerationRequest,
    LlmProvider, ProviderError,
};

const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";

fn digest() -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&"a".repeat(64))?,
    })
}

fn extracted_envelope() -> Result<EventEnvelope, Box<dyn std::error::Error>> {
    let document = Document {
        document_id: DocumentId::parse("018f0000-0000-7000-8000-000000000a01")?,
        source_address: DocumentAddress::parse("document:quokka-notes")?,
        content_digest: digest()?,
        title: Some("Quokka migration notes".to_owned()),
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            block_id: BlockId::new_v7(),
            text: "Quokkas were observed crossing the island in March.".to_owned(),
        }],
        provenance: Vec::new(),
    };
    let payload = ContentDocumentExtracted {
        document,
        document_blob: BlobRef {
            owner_service: BlobOwner::parse("ratatoskr-extractor")?,
            digest: digest()?,
            media_type: MediaType::parse("application/json")?,
            length_bytes: 256,
        },
        extensions: Extensions::new(),
    };
    Ok(serde_json::from_value(serde_json::json!({
        "event_id": "018f0000-0000-7000-8000-000000000a02",
        "event_type": ContentDocumentExtracted::EVENT_TYPE,
        "occurred_at": "2026-08-27T06:00:00Z",
        "producer": "ratatoskr-extractor",
        "aggregate_id": "document:018f0000-0000-7000-8000-000000000a01",
        "correlation_id": "operation:018f0000-0000-7000-8000-000000000a03",
        "tenant_id": OWNER,
        "schema_version": 1,
        "payload": serde_json::to_value(&payload)?
    }))?)
}

/// The deterministic first-paragraph provider carries a document all the way to search without an
/// inference credential, and answers no other analysis family.
#[tokio::test]
async fn extractive_fixture_provider_makes_a_document_searchable()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let accepted = extracted_envelope()?;
    let intake = DocumentIntake::new(&database.database, 4_000, std::time::Duration::from_secs(5));

    assert_eq!(
        intake
            .accept(&accepted, &ExtractiveArticleProvider, &blobs)
            .await?,
        DocumentIntakeOutcome::Analysed
    );

    let summary: String = sqlx::query_scalar(
        "select result ->> 'summary' from knowledge.analysis_outputs where accepted",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(
        summary,
        "Quokkas were observed crossing the island in March."
    );
    let found: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.search_documents where tenant_ref = $1 and title = $2",
    )
    .bind(OWNER)
    .bind("Quokka migration notes")
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(found, 1);

    let social = ExtractiveArticleProvider
        .generate_json(GenerationRequest {
            prompt_version: "social_prompt_v1".to_owned(),
            system_policy: String::new(),
            task_instruction: String::new(),
            output_schema: serde_json::json!({}),
            source_content: "block 0 paragraph: \"A post.\"\n".to_owned(),
        })
        .await;
    assert!(matches!(
        social,
        Err(failure) if failure.error == ProviderError::Permanent
    ));
    database.cleanup().await?;
    Ok(())
}
