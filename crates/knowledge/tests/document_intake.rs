//! Extracted-document intake: one event becomes one registered source, one analysis and one
//! searchable document (XR-021 CONTRACTS.md S09).

use ratatoskr_document_contracts::{
    ContentDocumentExtracted, Document, DocumentAddress, DocumentBlock,
};
use ratatoskr_event_envelope::{EventEnvelope, EventPayload};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, Extensions,
    MediaType, TenantRef,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, DocumentIntake, DocumentIntakeError, DocumentIntakeOutcome, HybridRetriever,
    ProviderError, ProviderResponse, ProviderUsage, RankingPath, ScriptedEmbeddingProvider,
    ScriptedProvider, SearchQuery, SourceReference,
};

const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";

fn digest(digit: char) -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    })
}

const FIRST_DOCUMENT_ID: &str = "018f0000-0000-7000-8000-000000000a01";
const SECOND_DOCUMENT_ID: &str = "018f0000-0000-7000-8000-000000000a09";

fn document() -> Result<Document, Box<dyn std::error::Error>> {
    document_with_id(FIRST_DOCUMENT_ID)
}

fn document_with_id(document_id: &str) -> Result<Document, Box<dyn std::error::Error>> {
    Ok(Document {
        document_id: DocumentId::parse(document_id)?,
        source_address: DocumentAddress::parse("document:quokka-notes")?,
        content_digest: digest('a')?,
        title: Some("Quokka migration notes".to_owned()),
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            block_id: BlockId::new_v7(),
            text: "Quokkas were observed crossing the island in March.".to_owned(),
        }],
        provenance: Vec::new(),
    })
}

fn document_blob(owner_service: &str) -> Result<BlobRef, Box<dyn std::error::Error>> {
    Ok(BlobRef {
        owner_service: BlobOwner::parse(owner_service)?,
        digest: digest('a')?,
        media_type: MediaType::parse("application/json")?,
        length_bytes: 256,
    })
}

fn envelope(
    document: &Document,
    blob: &BlobRef,
    producer: &str,
    tenant: Option<&str>,
) -> Result<EventEnvelope, Box<dyn std::error::Error>> {
    let payload = ContentDocumentExtracted {
        document: document.clone(),
        document_blob: blob.clone(),
        extensions: Extensions::new(),
    };
    let mut value = serde_json::json!({
        "event_id": "018f0000-0000-7000-8000-000000000a02",
        "event_type": ContentDocumentExtracted::EVENT_TYPE,
        "occurred_at": "2026-08-27T06:00:00Z",
        "producer": producer,
        "aggregate_id": format!("document:{}", document.document_id),
        "correlation_id": "operation:018f0000-0000-7000-8000-000000000a03",
        "causation_id": "command:018f0000-0000-7000-8000-000000000a04",
        "schema_version": 1,
        "payload": serde_json::to_value(&payload)?
    });
    if let (Some(tenant), Some(object)) = (tenant, value.as_object_mut()) {
        object.insert("tenant_id".to_owned(), serde_json::json!(tenant));
    }
    Ok(serde_json::from_value(value)?)
}

fn article_response() -> ProviderResponse {
    ProviderResponse {
        bytes: br#"{
            "summary": "Quokkas crossed the island in March.",
            "key_points": [{"text": "Quokkas were observed.", "source_block_indexes": [0]}]
        }"#
        .to_vec(),
        request_id: Some("scripted-intake".to_owned()),
        usage: ProviderUsage {
            input_tokens: 20,
            output_tokens: 10,
        },
    }
}

async fn count(database: &TestDatabase, sql: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(sql)
        .fetch_one(database.database.pool())
        .await
}

#[tokio::test]
async fn extracted_document_is_analysed_and_searchable() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let document = document()?;
    let blob = document_blob("ratatoskr-extractor")?;
    let accepted = envelope(&document, &blob, "ratatoskr-extractor", Some(OWNER))?;
    let provider = ScriptedProvider::new([Ok(article_response())]);
    let intake = DocumentIntake::new(&database.database, 4_000, std::time::Duration::from_secs(5));

    assert_eq!(
        intake.accept(&accepted, &provider, &blobs).await?,
        DocumentIntakeOutcome::Analysed
    );

    let source: (String, String, serde_json::Value) =
        sqlx::query_as("select tenant_ref, owner_context, source_blob from knowledge.source_refs")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(source.0, OWNER);
    assert_eq!(source.1, "ratatoskr-extractor");
    assert_eq!(source.2, serde_json::to_value(&blob)?);
    let runs: Vec<(String, String)> =
        sqlx::query_as("select state, model_policy from knowledge.analysis_runs")
            .fetch_all(database.database.pool())
            .await?;
    assert_eq!(
        runs,
        [("persisted".to_owned(), "family_default_v1".to_owned())]
    );

    let retriever = HybridRetriever::new(ScriptedEmbeddingProvider::new(
        8,
        [Err(ProviderError::Transient)],
    ));
    let owner: TenantRef = OWNER.parse()?;
    let (page, path) = retriever
        .page(
            database.database.pool(),
            &SearchQuery::new(owner.to_string(), "quokka", 5, 0)?,
        )
        .await?;
    assert_eq!(path, RankingPath::LexicalOnly);
    assert_eq!(
        page.results
            .iter()
            .map(|hit| hit.title.as_str())
            .collect::<Vec<_>>(),
        ["Quokka migration notes"]
    );

    // A redelivery of the same fact creates no second run and makes no second provider call.
    assert_eq!(
        intake.accept(&accepted, &provider, &blobs).await?,
        DocumentIntakeOutcome::AlreadyAnalysed
    );
    assert_eq!(provider.call_count()?, 1);
    assert_eq!(
        count(&database, "select count(*) from knowledge.analysis_runs").await?,
        1
    );
    assert_eq!(
        count(&database, "select count(*) from knowledge.source_refs").await?,
        1
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refused_extracted_document_events_leave_no_state() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let document = document()?;
    let blob = document_blob("ratatoskr-extractor")?;
    let provider = ScriptedProvider::new([Ok(article_response())]);
    let intake = DocumentIntake::new(&database.database, 4_000, std::time::Duration::from_secs(5));

    let without_tenant = envelope(&document, &blob, "ratatoskr-extractor", None)?;
    assert!(matches!(
        intake.accept(&without_tenant, &provider, &blobs).await,
        Err(DocumentIntakeError::MissingTenant)
    ));
    let wrong_producer = envelope(&document, &blob, "ratatoskr-x", Some(OWNER))?;
    assert!(matches!(
        intake.accept(&wrong_producer, &provider, &blobs).await,
        Err(DocumentIntakeError::WrongProducer)
    ));
    let foreign_blob = envelope(
        &document,
        &document_blob("ratatoskr-github")?,
        "ratatoskr-extractor",
        Some(OWNER),
    )?;
    assert!(matches!(
        intake.accept(&foreign_blob, &provider, &blobs).await,
        Err(DocumentIntakeError::InvalidPayload)
    ));

    assert_eq!(provider.call_count()?, 0);
    assert_eq!(
        count(&database, "select count(*) from knowledge.source_refs").await?,
        0
    );
    database.cleanup().await?;
    Ok(())
}

/// Stores one extracted document through the intake and returns the outcome.
async fn accept_document(
    database: &TestDatabase,
    blobs: &BlobStore,
    provider: &ScriptedProvider,
    document_id: &str,
    tenant: &str,
) -> Result<DocumentIntakeOutcome, Box<dyn std::error::Error>> {
    let document = document_with_id(document_id)?;
    let accepted = envelope(
        &document,
        &document_blob("ratatoskr-extractor")?,
        "ratatoskr-extractor",
        Some(tenant),
    )?;
    let intake = DocumentIntake::new(&database.database, 4_000, std::time::Duration::from_secs(5));
    Ok(intake.accept(&accepted, provider, blobs).await?)
}

/// XR-021 CONTRACTS.md R5-02 rule 4: the same document captured twice makes one row, one run
/// and one search document, whatever `document_id` the second event carries.
#[tokio::test]
async fn equal_digest_and_equal_document_id_make_one_row_run_and_search_document()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let provider = ScriptedProvider::new([Ok(article_response())]);

    assert_eq!(
        accept_document(&database, &blobs, &provider, FIRST_DOCUMENT_ID, OWNER).await?,
        DocumentIntakeOutcome::Analysed
    );
    assert_eq!(
        accept_document(&database, &blobs, &provider, FIRST_DOCUMENT_ID, OWNER).await?,
        DocumentIntakeOutcome::AlreadyAnalysed
    );

    assert_eq!(provider.call_count()?, 1);
    for table in ["source_refs", "analysis_runs", "search_documents"] {
        assert_eq!(
            count(
                &database,
                &format!("select count(*) from knowledge.{table}")
            )
            .await?,
            1,
            "{table}"
        );
    }
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn equal_digest_under_another_document_id_reuses_the_first_row()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let provider = ScriptedProvider::new([Ok(article_response())]);

    assert_eq!(
        accept_document(&database, &blobs, &provider, FIRST_DOCUMENT_ID, OWNER).await?,
        DocumentIntakeOutcome::Analysed
    );
    assert_eq!(
        accept_document(&database, &blobs, &provider, SECOND_DOCUMENT_ID, OWNER).await?,
        DocumentIntakeOutcome::AlreadyAnalysed
    );

    assert_eq!(provider.call_count()?, 1);
    for table in ["source_refs", "analysis_runs", "search_documents"] {
        assert_eq!(
            count(
                &database,
                &format!("select count(*) from knowledge.{table}")
            )
            .await?,
            1,
            "{table}"
        );
    }
    let stored: String = sqlx::query_scalar("select source_document_id from knowledge.source_refs")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(stored, FIRST_DOCUMENT_ID);
    let searchable: uuid::Uuid =
        sqlx::query_scalar("select document_id from knowledge.search_documents")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(searchable.to_string(), FIRST_DOCUMENT_ID);
    database.cleanup().await?;
    Ok(())
}

/// The first analysis did not finish (the provider failed), so the second event with another
/// document id continues the same run, and the search document keeps the first id.
#[tokio::test]
async fn an_unfinished_first_analysis_is_continued_under_the_first_document_id()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let provider = ScriptedProvider::new([
        Err(ProviderError::Transient),
        Err(ProviderError::Transient),
        Ok(article_response()),
    ]);

    assert!(
        accept_document(&database, &blobs, &provider, FIRST_DOCUMENT_ID, OWNER)
            .await
            .is_err()
    );
    assert_eq!(
        accept_document(&database, &blobs, &provider, SECOND_DOCUMENT_ID, OWNER).await?,
        DocumentIntakeOutcome::Analysed
    );

    assert_eq!(
        count(&database, "select count(*) from knowledge.source_refs").await?,
        1
    );
    assert_eq!(
        count(&database, "select count(*) from knowledge.analysis_runs").await?,
        1
    );
    let searchable: uuid::Uuid =
        sqlx::query_scalar("select document_id from knowledge.search_documents")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(searchable.to_string(), FIRST_DOCUMENT_ID);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn the_same_digest_under_two_tenants_keeps_two_rows() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let provider = ScriptedProvider::new([Ok(article_response()), Ok(article_response())]);

    for tenant in [OWNER, "user:018f0000-0000-7000-8000-000000000006"] {
        assert_eq!(
            accept_document(&database, &blobs, &provider, FIRST_DOCUMENT_ID, tenant).await?,
            DocumentIntakeOutcome::Analysed
        );
    }

    assert_eq!(
        count(&database, "select count(*) from knowledge.source_refs").await?,
        2
    );
    assert_eq!(
        count(&database, "select count(*) from knowledge.search_documents").await?,
        2
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn the_database_keeps_one_extractor_source_per_tenant_and_digest()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let index_definition: String = sqlx::query_scalar(
        "select indexdef from pg_indexes
         where schemaname = 'knowledge' and indexname = 'source_refs_document_digest_key'",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert!(index_definition.contains("UNIQUE"), "{index_definition}");
    assert!(
        index_definition.contains("ratatoskr-extractor"),
        "{index_definition}"
    );

    let tenant: TenantRef = OWNER.parse()?;
    let reference = |document_id: &str| -> Result<SourceReference, Box<dyn std::error::Error>> {
        Ok(SourceReference {
            tenant,
            owner_context: "ratatoskr-extractor".to_owned(),
            ai_archive_id: String::new(),
            document_id: DocumentId::parse(document_id)?,
            content_digest: digest('a')?,
            source_blob: document_blob("ratatoskr-extractor")?,
        })
    };
    let first = database
        .database
        .register_source(&reference(FIRST_DOCUMENT_ID)?)
        .await?;
    let second = database
        .database
        .register_source(&reference(SECOND_DOCUMENT_ID)?)
        .await?;
    assert_eq!(
        first.id, second.id,
        "a second document id resolves to the first row"
    );
    assert_eq!(
        count(&database, "select count(*) from knowledge.source_refs").await?,
        1
    );

    // The rule is scoped to the extractor: Knowledge's own snapshots of two archives with the
    // same digest stay two rows.
    let snapshot = |archive: &str| -> Result<SourceReference, Box<dyn std::error::Error>> {
        Ok(SourceReference {
            tenant,
            owner_context: "ratatoskr-knowledge".to_owned(),
            ai_archive_id: archive.to_owned(),
            document_id: DocumentId::parse(FIRST_DOCUMENT_ID)?,
            content_digest: digest('b')?,
            source_blob: document_blob("ratatoskr-knowledge")?,
        })
    };
    let one = database
        .database
        .register_source(&snapshot("018f0000-0000-7000-8000-000000000c01")?)
        .await?;
    let other = database
        .database
        .register_source(&snapshot("018f0000-0000-7000-8000-000000000c02")?)
        .await?;
    assert_ne!(one.id, other.id);
    assert_eq!(
        count(&database, "select count(*) from knowledge.source_refs").await?,
        3
    );
    database.cleanup().await?;
    Ok(())
}
