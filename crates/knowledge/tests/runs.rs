//! Durable analysis identity and state tests.

use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{AnalysisIdentity, SourceReference};

#[tokio::test]
async fn changed_source_digest_creates_an_immutable_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let tenant = TenantRef::of_user(UserId::new_v7());
    let document_id = DocumentId::new_v7();

    let first = database
        .database
        .register_source(&source(tenant, document_id, 'a')?)
        .await?;
    let second = database
        .database
        .register_source(&source(tenant, document_id, 'b')?)
        .await?;

    assert_ne!(first.id, second.id);
    let count: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.source_refs where source_document_id = $1",
    )
    .bind(document_id.to_string())
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(count, 2);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn complete_analysis_identity_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let source = database
        .database
        .register_source(&source(
            TenantRef::of_user(UserId::new_v7()),
            DocumentId::new_v7(),
            'c',
        )?)
        .await?;
    let identity = AnalysisIdentity {
        source_revision_id: source.id,
        contract_version: "article_v1".to_owned(),
        prompt_version: "article_prompt_v1".to_owned(),
        context_builder_version: "document_context_v1".to_owned(),
        model_policy: "fake_default_v1".to_owned(),
    };

    let first = database.database.create_run(&identity).await?;
    let second = database.database.create_run(&identity).await?;
    assert_eq!(first.id, second.id);

    let count: i64 = sqlx::query_scalar("select count(*) from knowledge.analysis_runs")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(count, 1);
    database.cleanup().await?;
    Ok(())
}

fn source(
    tenant: TenantRef,
    document_id: DocumentId,
    digit: char,
) -> Result<SourceReference, ratatoskr_identifiers::IdentifierError> {
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    };
    Ok(SourceReference {
        tenant,
        owner_context: "ratatoskr-extractor".to_owned(),
        document_id,
        content_digest: digest.clone(),
        source_blob: BlobRef {
            owner_service: BlobOwner::parse("ratatoskr-extractor")?,
            digest,
            media_type: MediaType::parse("application/json")?,
            length_bytes: 128,
        },
    })
}
