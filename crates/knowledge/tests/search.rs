//! Reader-side ranked retrieval checks over a disposable database.

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{ProviderError, SearchError, SearchQuery, SourceReference, search_page};
use sha2::Digest as _;

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
            block_id: BlockId::new_v7(),
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

/// Identity constants shared by every embedded fixture row so the
/// semantic leg binds one coherent model version.
const FIXTURE_PROVIDER: &str = "scripted";
const FIXTURE_MODEL: &str = "fixture-embedder";
const FIXTURE_PROMPT_VERSION: &str = "none.v1";
const FIXTURE_DIMENSIONS: i32 = 1536;

fn fixture_vector(directions: &[(usize, f32)]) -> pgvector::Vector {
    let mut values = vec![0.0_f32; usize::try_from(FIXTURE_DIMENSIONS).unwrap_or(0)];
    for (index, magnitude) in directions {
        if let Some(slot) = values.get_mut(*index) {
            *slot = *magnitude;
        }
    }
    pgvector::Vector::from(values)
}

struct EmbeddedRow {
    tenant_ref: String,
}

/// One fixture document's projection inputs.
struct FixtureDocument<'a> {
    tenant: &'a TenantRef,
    owner_context: &'a str,
    title: &'a str,
    lead: &'a str,
    body: &'a str,
    age_seconds: i64,
}

/// Registers one source revision with a completed run, an accepted
/// output, the projected search row, and one embedding chunk under the
/// given provider model identity, `age_seconds` ago.
async fn project_embedded_row(
    database: &TestDatabase,
    doc: &FixtureDocument<'_>,
    provider: &str,
    model: &str,
    embedding: pgvector::Vector,
) -> Result<EmbeddedRow, Box<dyn std::error::Error>> {
    let document = Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:hybrid")?,
        content_digest: digest('b')?,
        title: Some(doc.title.to_owned()),
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            block_id: BlockId::new_v7(),
            text: doc.lead.to_owned(),
        }],
        provenance: Vec::new(),
    };
    let source = database
        .database
        .register_source(&SourceReference {
            tenant: *doc.tenant,
            owner_context: doc.owner_context.to_owned(),
            document_id: document.document_id,
            content_digest: document.content_digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse(doc.owner_context)?,
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
    let output_id = insert_fixture_run_output(database, source.id).await?;
    insert_fixture_search_document(
        database,
        doc,
        source.id,
        output_id,
        &tenant_ref,
        document.document_id.0,
    )
    .await?;
    let target = FixtureChunkTarget {
        source_ref_id: source.id,
        output_id,
        tenant_ref: tenant_ref.clone(),
        document_id: document.document_id.0,
    };
    insert_fixture_chunk(database, doc, &target, provider, model, embedding).await?;
    Ok(EmbeddedRow { tenant_ref })
}

/// Creates the completed run and accepted output for one fixture source.
async fn insert_fixture_run_output(
    database: &TestDatabase,
    source_ref_id: uuid::Uuid,
) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
    let run_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         )
         values ($1, $2, 'article-analysis.v1', 'v1', 'v1', 'scripted', 'completed')",
    )
    .bind(run_id)
    .bind(source_ref_id)
    .execute(database.database.pool())
    .await?;
    let output_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_outputs (output_id, run_id, result, raw_response)
         values ($1, $2, '{}', '{}')",
    )
    .bind(output_id)
    .bind(run_id)
    .execute(database.database.pool())
    .await?;
    Ok(output_id)
}

/// Projects the tenant-scoped lexical row for one fixture document.
async fn insert_fixture_search_document(
    database: &TestDatabase,
    doc: &FixtureDocument<'_>,
    source_ref_id: uuid::Uuid,
    output_id: uuid::Uuid,
    tenant_ref: &str,
    document_id: uuid::Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref,
             owner_context, document_id, title, lead, body, updated_at
         )
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                 now() - make_interval(secs => $10::double precision))",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(source_ref_id)
    .bind(output_id)
    .bind(tenant_ref)
    .bind(doc.owner_context)
    .bind(document_id)
    .bind(doc.title)
    .bind(doc.lead)
    .bind(doc.body)
    .bind(doc.age_seconds)
    .execute(database.database.pool())
    .await?;
    Ok(())
}

/// Identity and provenance ids for one fixture chunk row.
struct FixtureChunkTarget {
    source_ref_id: uuid::Uuid,
    output_id: uuid::Uuid,
    tenant_ref: String,
    document_id: uuid::Uuid,
}

/// Inserts the single embedding chunk backing one fixture row.
async fn insert_fixture_chunk(
    database: &TestDatabase,
    doc: &FixtureDocument<'_>,
    target: &FixtureChunkTarget,
    provider: &str,
    model: &str,
    embedding: pgvector::Vector,
) -> Result<(), Box<dyn std::error::Error>> {
    let chunk_text = format!("{}\n\n{}", doc.title, doc.lead);
    let mut hasher = sha2::Sha256::new();
    hasher.update(chunk_text.as_bytes());
    let digest_hex = format!("{:x}", hasher.finalize());
    sqlx::query(
        "insert into knowledge.embedding_chunks (
             embedding_chunk_id, source_ref_id, output_id, tenant_ref,
             owner_context, document_id, ordinal, chunk_text, chunk_digest_hex,
             chunking_version, provider, model, dimensions, prompt_version,
             embedding, created_at
         )
         values ($1, $2, $3, $4, $5, $6, 0, $7, $8,
                 'article-chunks.v1', $9, $10, $11, $12, $13,
                 now() - make_interval(secs => $14::double precision))",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(target.source_ref_id)
    .bind(target.output_id)
    .bind(&target.tenant_ref)
    .bind(doc.owner_context)
    .bind(target.document_id)
    .bind(&chunk_text)
    .bind(digest_hex)
    .bind(provider)
    .bind(model)
    .bind(FIXTURE_DIMENSIONS)
    .bind(FIXTURE_PROMPT_VERSION)
    .bind(embedding)
    .bind(doc.age_seconds)
    .execute(database.database.pool())
    .await?;
    Ok(())
}

fn hybrid_leg(vector: &pgvector::Vector) -> ratatoskr_knowledge::SemanticLeg {
    ratatoskr_knowledge::SemanticLeg {
        vector: vector.to_vec(),
        provider: FIXTURE_PROVIDER.to_owned(),
        model: FIXTURE_MODEL.to_owned(),
        prompt_version: FIXTURE_PROMPT_VERSION.to_owned(),
        chunking_version: "article-chunks.v1".to_owned(),
    }
}

#[tokio::test]
async fn hybrid_ranking_orders_by_reciprocal_rank_fusion() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let tenant = TenantRef::of_user(UserId::new_v7());
    // Lexical leg: Alpha ranks first (title hit), Beta second, Gamma no
    // match. Semantic leg against the query direction [1, 0, ..]: Gamma
    // is nearest, Beta mid, and Alpha carries only a legacy-model chunk,
    // so the active-identity binding leaves it without a semantic rank.
    let alpha = project_embedded_row(
        &database,
        &FixtureDocument {
            tenant: &tenant,
            owner_context: "owner",
            title: "Alpha engine",
            lead: "Alpha evidence dominates.",
            body: "",
            age_seconds: 30,
        },
        FIXTURE_PROVIDER,
        "legacy-embedder",
        fixture_vector(&[(1, 1.0)]),
    )
    .await?;
    project_embedded_row(
        &database,
        &FixtureDocument {
            tenant: &tenant,
            owner_context: "owner",
            title: "Quiet bridge",
            lead: "A calm aside.",
            body: "One alpha mention.",
            age_seconds: 20,
        },
        FIXTURE_PROVIDER,
        FIXTURE_MODEL,
        fixture_vector(&[(0, 0.6), (1, 0.8)]),
    )
    .await?;
    project_embedded_row(
        &database,
        &FixtureDocument {
            tenant: &tenant,
            owner_context: "owner",
            title: "Gamma harbor",
            lead: "Unrelated wording entirely.",
            body: "",
            age_seconds: 10,
        },
        FIXTURE_PROVIDER,
        FIXTURE_MODEL,
        fixture_vector(&[(0, 1.0)]),
    )
    .await?;

    let query = SearchQuery::new(&alpha.tenant_ref, "alpha", 10, 0)?;
    let page = ratatoskr_knowledge::hybrid_search_page(
        database.database.pool(),
        &query,
        &hybrid_leg(&fixture_vector(&[(0, 1.0)])),
    )
    .await?;

    // Reciprocal Rank Fusion at k=60 over lexical [Alpha, Beta] and
    // semantic [Gamma, Beta]: Beta pairs ranks 2+2 to ~= 0.032258 and
    // leads; Alpha (lexically first but semantically invisible under the
    // active identity) and Gamma tie at one leg each with 1/61, and the
    // recency tiebreaker favors the newer Gamma over the older Alpha.
    assert_eq!(
        page.results
            .iter()
            .map(|hit| hit.title.as_str())
            .collect::<Vec<_>>(),
        ["Quiet bridge", "Gamma harbor", "Alpha engine"]
    );
    let fused_scores: Vec<f32> = page
        .results
        .iter()
        .map(|hit| hit.rank.expect("hybrid hits carry fused scores"))
        .collect();
    assert!(fused_scores[0] > fused_scores[1]);
    assert!(
        (fused_scores[1] - fused_scores[2]).abs() < f32::EPSILON,
        "one-leg ties must carry identical fused scores: {} vs {}",
        fused_scores[1],
        fused_scores[2]
    );
    assert!(page.results.iter().all(|hit| hit.snippet.is_some()));

    let replay = ratatoskr_knowledge::hybrid_search_page(
        database.database.pool(),
        &query,
        &hybrid_leg(&fixture_vector(&[(0, 1.0)])),
    )
    .await?;
    assert_eq!(
        page.results
            .iter()
            .map(|hit| (hit.document_id, hit.rank))
            .collect::<Vec<_>>(),
        replay
            .results
            .iter()
            .map(|hit| (hit.document_id, hit.rank))
            .collect::<Vec<_>>(),
        "repeated queries must return identical pages"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn hybrid_legs_are_equally_tenant_scoped() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let foreign_tenant = TenantRef::of_user(UserId::new_v7());
    let own_tenant = TenantRef::of_user(UserId::new_v7());
    // The foreign document would rank first on both legs alone.
    project_embedded_row(
        &database,
        &FixtureDocument {
            tenant: &foreign_tenant,
            owner_context: "other-owner",
            title: "Foreign alpha monolith",
            lead: "Alpha alpha alpha.",
            body: "",
            age_seconds: 5,
        },
        FIXTURE_PROVIDER,
        FIXTURE_MODEL,
        fixture_vector(&[(0, 1.0)]),
    )
    .await?;
    let own = project_embedded_row(
        &database,
        &FixtureDocument {
            tenant: &own_tenant,
            owner_context: "own-owner",
            title: "Own alpha fragment",
            lead: "A quieter alpha note.",
            body: "",
            age_seconds: 5,
        },
        FIXTURE_PROVIDER,
        FIXTURE_MODEL,
        fixture_vector(&[(0, 0.9), (1, 0.1)]),
    )
    .await?;

    let query = SearchQuery::new(&own.tenant_ref, "alpha", 10, 0)?;
    let page = ratatoskr_knowledge::hybrid_search_page(
        database.database.pool(),
        &query,
        &hybrid_leg(&fixture_vector(&[(0, 1.0)])),
    )
    .await?;

    assert_eq!(page.results.len(), 1);
    assert_eq!(page.results[0].title, "Own alpha fragment");
    database.cleanup().await?;
    Ok(())
}

/// Test double whose every call returns one fixed outcome: either an
/// exact echo vector or a permanent failure, so fixture geometry stays
/// predictable across both retrieval paths.
struct FixedOutcomeProvider {
    identity: ratatoskr_knowledge::EmbeddingIdentity,
    outcome: Result<ratatoskr_knowledge::EmbeddingResponse, ProviderError>,
}

impl ratatoskr_knowledge::EmbeddingProvider for FixedOutcomeProvider {
    fn identity(&self) -> ratatoskr_knowledge::EmbeddingIdentity {
        self.identity.clone()
    }

    fn embed(
        &self,
        _inputs: Vec<String>,
    ) -> impl std::future::Future<
        Output = Result<
            ratatoskr_knowledge::EmbeddingResponse,
            ratatoskr_knowledge::ProviderFailure,
        >,
    > + Send {
        let outcome = match &self.outcome {
            Ok(response) => Ok(response.clone()),
            Err(error) => Err(ratatoskr_knowledge::ProviderFailure {
                error: *error,
                class: ratatoskr_knowledge::ProviderFailureClass::Unclassified,
                http_status: None,
            }),
        };
        std::future::ready(outcome)
    }
}

fn fixed_provider(
    outcome: Result<ratatoskr_knowledge::EmbeddingResponse, ProviderError>,
) -> FixedOutcomeProvider {
    FixedOutcomeProvider {
        identity: ratatoskr_knowledge::EmbeddingIdentity {
            provider: "scripted_fake".to_owned(),
            model: "fake_default_v1".to_owned(),
            dimensions: 1536,
            prompt_version: "none.v1".to_owned(),
        },
        outcome,
    }
}

#[tokio::test]
async fn search_degrades_to_lexical_without_provider() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let tenant = TenantRef::of_user(UserId::new_v7());
    // Seeded under the double's exact identity so a healthy retriever
    // fuses both legs. Alpha deliberately carries a superseded model so
    // the semantic leg never sees it.
    let alpha = project_embedded_row(
        &database,
        &FixtureDocument {
            tenant: &tenant,
            owner_context: "owner",
            title: "Alpha engine",
            lead: "Alpha evidence dominates.",
            body: "",
            age_seconds: 30,
        },
        "scripted_fake",
        "legacy-embedder",
        fixture_vector(&[(1, 1.0)]),
    )
    .await?;
    project_embedded_row(
        &database,
        &FixtureDocument {
            tenant: &tenant,
            owner_context: "owner",
            title: "Quiet bridge",
            lead: "A calm aside.",
            body: "One alpha mention.",
            age_seconds: 20,
        },
        "scripted_fake",
        "fake_default_v1",
        fixture_vector(&[(0, 0.6), (1, 0.8)]),
    )
    .await?;
    project_embedded_row(
        &database,
        &FixtureDocument {
            tenant: &tenant,
            owner_context: "owner",
            title: "Gamma harbor",
            lead: "Unrelated wording entirely.",
            body: "",
            age_seconds: 10,
        },
        "scripted_fake",
        "fake_default_v1",
        fixture_vector(&[(0, 1.0)]),
    )
    .await?;

    let query = SearchQuery::new(&alpha.tenant_ref, "alpha", 10, 0)?;
    let query_vector = fixture_vector(&[(0, 1.0)]);
    let working = ratatoskr_knowledge::HybridRetriever::new(fixed_provider(Ok(
        ratatoskr_knowledge::EmbeddingResponse {
            vectors: vec![query_vector.to_vec()],
            input_tokens: 4,
        },
    )));
    let (hybrid_page, path) = working.page(database.database.pool(), &query).await?;
    assert_eq!(path, ratatoskr_knowledge::RankingPath::Hybrid);
    // Lexical [Alpha, Beta] plus semantic [Gamma, Beta]: Beta pairs both
    // legs and leads; Alpha and Gamma tie at one leg each and recency
    // favors newer Gamma.
    let fused_scores: Vec<f32> = hybrid_page
        .results
        .iter()
        .map(|hit| hit.rank.expect("hybrid hits carry fused scores"))
        .collect();
    assert_eq!(
        hybrid_page
            .results
            .iter()
            .map(|hit| hit.title.as_str())
            .collect::<Vec<_>>(),
        ["Quiet bridge", "Gamma harbor", "Alpha engine"]
    );
    assert!(fused_scores[0] > fused_scores[1]);
    assert!(
        (fused_scores[1] - fused_scores[2]).abs() < f32::EPSILON,
        "one-leg ties must carry identical fused scores: {} vs {}",
        fused_scores[1],
        fused_scores[2]
    );

    let failing =
        ratatoskr_knowledge::HybridRetriever::new(fixed_provider(Err(ProviderError::Transient)));
    let (fallback_page, fallback_path) = failing.page(database.database.pool(), &query).await?;
    assert_eq!(fallback_path, ratatoskr_knowledge::RankingPath::LexicalOnly);
    let lexical_page = search_page(database.database.pool(), &query).await?;
    assert_eq!(
        fallback_page
            .results
            .iter()
            .map(|hit| hit.title.as_str())
            .collect::<Vec<_>>(),
        lexical_page
            .results
            .iter()
            .map(|hit| hit.title.as_str())
            .collect::<Vec<_>>(),
        "degraded pages must equal plain lexical ranking"
    );

    let blank = SearchQuery::new(&alpha.tenant_ref, "", 10, 0)?;
    let (_, browse_path) = failing.page(database.database.pool(), &blank).await?;
    assert_eq!(browse_path, ratatoskr_knowledge::RankingPath::BrowseRecent);

    database.cleanup().await?;
    Ok(())
}
