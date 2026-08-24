//! Reader-side ranked retrieval checks over a disposable database.

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{SearchError, SearchQuery, SourceReference, search_page};

fn digest(digit: char) -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    })
}

/// Registers one source revision under `tenant` and projects its accepted
/// search row directly, simulating a completed analysis whose output landed
/// `age_seconds` ago. Returns the tenant's canonical text form.
async fn project_row(
    database: &TestDatabase,
    tenant: &TenantRef,
    owner_context: &str,
    title: &str,
    lead: &str,
    body: &str,
    age_seconds: i64,
) -> Result<String, Box<dyn std::error::Error>> {
    let document = Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:search")?,
        content_digest: digest('a')?,
        title: Some(title.to_owned()),
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            text: lead.to_owned(),
        }],
        provenance: Vec::new(),
    };
    let source = database
        .database
        .register_source(&SourceReference {
            tenant: *tenant,
            owner_context: owner_context.to_owned(),
            document_id: document.document_id,
            content_digest: document.content_digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse(owner_context)?,
                digest: document.content_digest.clone(),
                media_type: MediaType::parse("application/json")?,
                length_bytes: 128,
            },
        })
        .await?;
    let (tenant_ref,): (String,) =
        sqlx::query_as("select tenant_ref from knowledge.source_refs where source_ref_id = $1")
            .bind(source.id)
            .fetch_one(database.database.pool())
            .await?;
    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref,
             owner_context, document_id, title, lead, body, updated_at
         )
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                 now() - make_interval(secs => $10::double precision))",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(source.id)
    .bind(uuid::Uuid::now_v7())
    .bind(&tenant_ref)
    .bind(owner_context)
    .bind(document.document_id.0)
    .bind(title)
    .bind(lead)
    .bind(body)
    .bind(age_seconds)
    .execute(database.database.pool())
    .await?;
    Ok(tenant_ref)
}

#[tokio::test]
async fn another_tenants_matching_document_is_invisible() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let foreign_tenant = TenantRef::of_user(UserId::new_v7());
    let own_tenant = TenantRef::of_user(UserId::new_v7());
    let foreign = project_row(
        &database,
        &foreign_tenant,
        "other-owner",
        "Alpha insights",
        "Alpha evidence.",
        "",
        10,
    )
    .await?;
    let own = project_row(
        &database,
        &own_tenant,
        "own-owner",
        "Unrelated piece",
        "Nothing relevant.",
        "",
        10,
    )
    .await?;
    assert_ne!(foreign, own);

    let page = search_page(
        database.database.pool(),
        &SearchQuery::new(&own, "alpha", 10, 0)?,
    )
    .await?;

    assert!(page.results.is_empty());
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_title_match_outranks_a_body_only_match_with_deterministic_ordering()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let shared = TenantRef::of_user(UserId::new_v7());
    // The title hit is deliberately older, so ranking weight, not recency,
    // decides the order.
    let shared_text = project_row(
        &database,
        &shared,
        "owner",
        "Gamma report",
        "Other facts.",
        "",
        30,
    )
    .await?;
    project_row(
        &database,
        &shared,
        "owner",
        "Plain report",
        "Quiet intro.",
        "Deep gamma details.",
        10,
    )
    .await?;

    let page = search_page(
        database.database.pool(),
        &SearchQuery::new(&shared_text, "gamma", 10, 0)?,
    )
    .await?;

    assert_eq!(page.results.len(), 2);
    assert_eq!(page.results[0].title, "Gamma report");
    assert_eq!(page.results[1].title, "Plain report");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn snippets_stay_word_bounded_with_score_above_zero() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let shared = TenantRef::of_user(UserId::new_v7());
    let long_lead = format!("{}delta anchor.", "Filler ".repeat(40));
    let shared_text = project_row(
        &database,
        &shared,
        "owner",
        "Delta report",
        &long_lead,
        "",
        10,
    )
    .await?;

    let page = search_page(
        database.database.pool(),
        &SearchQuery::new(&shared_text, "delta", 10, 0)?,
    )
    .await?;

    assert_eq!(page.results.len(), 1);
    let hit = &page.results[0];
    let snippet = hit.snippet.clone().ok_or("snippet missing")?;
    assert!(snippet.contains("delta"), "unexpected snippet: {snippet}");
    assert!(
        snippet.split_whitespace().count() <= 16,
        "snippet exceeded word bound: {snippet}"
    );
    let rank = hit.rank.ok_or("rank missing")?;
    assert!(rank > 0.0);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_blank_query_browses_by_descending_update_time_without_snippet_or_score()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let shared = TenantRef::of_user(UserId::new_v7());
    let shared_text = project_row(
        &database,
        &shared,
        "owner",
        "Older piece",
        "Stale text.",
        "",
        100,
    )
    .await?;
    project_row(
        &database,
        &shared,
        "owner",
        "Newer piece",
        "Fresh text.",
        "",
        40,
    )
    .await?;

    for blank in ["", "   "] {
        let page = search_page(
            database.database.pool(),
            &SearchQuery::new(&shared_text, blank, 10, 0)?,
        )
        .await?;

        assert_eq!(page.results.len(), 2);
        assert_eq!(page.results[0].title, "Newer piece");
        assert_eq!(page.results[1].title, "Older piece");
        assert!(page.results.iter().all(|hit| hit.snippet.is_none()));
        assert!(page.results.iter().all(|hit| hit.rank.is_none()));
    }

    database.cleanup().await?;
    Ok(())
}

#[test]
fn rejects_out_of_bounds_pages() {
    for (limit, offset) in [(0, 0), (101, 0), (10, -1)] {
        assert!(
            matches!(
                SearchQuery::new("tenant-a", "delta", limit, offset),
                Err(SearchError::InvalidParameters)
            ),
            "expected InvalidParameters for limit={limit}, offset={offset}"
        );
    }
    for (limit, offset) in [(1, 0), (100, 25)] {
        assert!(
            SearchQuery::new("tenant-a", "delta", limit, offset).is_ok(),
            "expected acceptance within bounds for limit={limit}, offset={offset}"
        );
    }
}
