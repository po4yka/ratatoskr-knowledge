//! Operator-plane state tests.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt as _;
use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport, TestDatabase};
use ratatoskr_knowledge::{
    BudgetLedger, BudgetLimits, ControlledEmbeddings, EmbeddingsSettings, HybridRetriever,
    OpenAiCompatibleEmbeddings, ProviderSecret, RateLimiter, RetryPolicy, TokenPrices,
};
use ratatoskr_knowledge_service::{Lifecycle, Metrics, admin_router};
use serde_json::Value;
use tower::ServiceExt as _;

#[tokio::test]
async fn readiness_follows_storage_startup_and_drain() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let lifecycle = Lifecycle::starting();
    let app = admin_router(
        lifecycle.clone(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
    );

    assert_response(&app, "/live", StatusCode::OK).await?;
    assert_response(&app, "/ready", StatusCode::SERVICE_UNAVAILABLE).await?;
    lifecycle.mark_ready();
    assert_response(&app, "/ready", StatusCode::OK).await?;
    assert_response(&app, "/metrics", StatusCode::OK).await?;
    assert_response(&app, "/version", StatusCode::OK).await?;
    lifecycle.begin_drain();
    assert_response(&app, "/ready", StatusCode::SERVICE_UNAVAILABLE).await?;
    assert_response(&app, "/live", StatusCode::OK).await?;

    database.cleanup().await?;
    Ok(())
}

async fn assert_response(
    app: &axum::Router,
    path: &str,
    expected: StatusCode,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), expected);
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL),
        Some(&header::HeaderValue::from_static("no-store"))
    );
    let _body = response.into_body().collect().await?;
    Ok(())
}

#[tokio::test]
async fn capability_document_declares_library_surfaces() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let app = admin_router(
        Lifecycle::starting(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/capabilities")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL),
        Some(&header::HeaderValue::from_static("no-store"))
    );
    let document: Value =
        serde_json::from_slice(&response.into_body().collect().await?.to_bytes())?;
    assert_eq!(document["service"], "knowledge");
    assert_eq!(
        document["capabilities"],
        serde_json::json!(["library.search", "library.read_state"])
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn search_endpoint_returns_ranked_results_and_requires_tenant()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    seed_search_hit(
        database.database.pool(),
        "seed-tenant-a",
        "seed-owner",
        "Delta report",
        "Delta evidence.",
        10,
    )
    .await?;
    let app = admin_router(
        Lifecycle::starting(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/search?tenant=seed-tenant-a&q=delta&limit=10&offset=0")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL),
        Some(&header::HeaderValue::from_static("no-store"))
    );
    let body = response.into_body().collect().await?.to_bytes();
    let page: Value = serde_json::from_slice(&body)?;
    let results = page["results"].as_array().ok_or("results missing")?;
    assert_eq!(results.len(), 1);
    let hit = &results[0];
    assert_eq!(hit["title"], "Delta report");
    assert_eq!(hit["owner_context"], "seed-owner");
    assert!(hit["document_id"].is_string());
    assert!(hit["snippet"].is_string());
    let rank = hit["rank"].as_f64().ok_or("rank missing")?;
    assert!(rank > 0.0);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/internal/search?q=delta")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn user_content_routes_require_tenant_scope_and_hide_foreign_targets()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let app = admin_router(
        Lifecycle::starting(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
    );
    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/user-content/command")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"operation":"create_tag","tenant":"user:route-a","name":"Private"}"#,
                ))?,
        )
        .await?;
    assert_eq!(created.status(), StatusCode::OK);
    assert_eq!(
        created.headers().get(header::CACHE_CONTROL),
        Some(&header::HeaderValue::from_static("no-store"))
    );

    let missing_scope = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/user-content/command")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"operation":"create_tag","name":"Private"}"#))?,
        )
        .await?;
    assert_eq!(missing_scope.status(), StatusCode::BAD_REQUEST);

    let foreign = app
        .oneshot(Request::builder().uri("/internal/user-content/collection?tenant=user:route-b&collection_id=00000000-0000-7000-8000-000000000001").body(Body::empty())?)
        .await?;
    assert_eq!(foreign.status(), StatusCode::NOT_FOUND);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn library_search_and_read_state_adapter_is_bounded_and_tenant_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let (read_output, unread_output, foreign_output) = seed_library_state(&database).await?;
    let app = admin_router(
        Lifecycle::starting(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/search?tenant=library-owner&q=library&read_state=unread&limit=1&offset=0")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL),
        Some(&header::HeaderValue::from_static("no-store"))
    );
    let page: Value = serde_json::from_slice(&response.into_body().collect().await?.to_bytes())?;
    assert_eq!(page["results"][0]["analysis_id"], unread_output.to_string());
    assert_eq!(page["results"][0]["read_state"], "unread");
    assert_eq!(page["has_more"], false);

    let invalid = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/search?tenant=library-owner&read_state=archived")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/user-content/command")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "operation": "set_read_state",
                        "tenant": "library-owner",
                        "output_id": read_output,
                        "read_state": "read"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(update.status(), StatusCode::OK);
    assert_eq!(
        update.headers().get(header::CACHE_CONTROL),
        Some(&header::HeaderValue::from_static("no-store"))
    );
    let updated: Value = serde_json::from_slice(&update.into_body().collect().await?.to_bytes())?;
    assert_eq!(updated["read_state"], "read");
    let favorite: bool = sqlx::query_scalar(
        "select favorite from knowledge.analysis_user_states
         where tenant_ref = 'library-owner' and output_id = $1",
    )
    .bind(read_output)
    .fetch_one(database.database.pool())
    .await?;
    assert!(favorite);

    let mut absence_bodies = Vec::new();
    for output_id in [foreign_output, uuid::Uuid::now_v7()] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/user-content/command")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "operation": "set_read_state",
                            "tenant": "library-owner",
                            "output_id": output_id,
                            "read_state": "read"
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        absence_bodies.push(response.into_body().collect().await?.to_bytes());
    }
    assert_eq!(absence_bodies[0], absence_bodies[1]);

    database.cleanup().await?;
    Ok(())
}

async fn seed_library_state(
    database: &TestDatabase,
) -> Result<(uuid::Uuid, uuid::Uuid, uuid::Uuid), Box<dyn std::error::Error>> {
    let read_output = seed_search_hit(
        database.database.pool(),
        "library-owner",
        "owner",
        "Library newest",
        "Library evidence.",
        5,
    )
    .await?;
    let unread_output = seed_search_hit(
        database.database.pool(),
        "library-owner",
        "owner",
        "Library unread",
        "Library evidence.",
        10,
    )
    .await?;
    let foreign_output = seed_search_hit(
        database.database.pool(),
        "library-foreign",
        "foreign",
        "Library foreign",
        "Library evidence.",
        1,
    )
    .await?;
    sqlx::query(
        "insert into knowledge.analysis_user_states (
             tenant_ref, output_id, read_state, favorite
         ) values ('library-owner', $1, 'read', true)",
    )
    .bind(read_output)
    .execute(database.database.pool())
    .await?;
    Ok((read_output, unread_output, foreign_output))
}

#[tokio::test]
async fn metrics_counters_track_served_search_paths_without_embeddings()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    seed_search_hit(
        database.database.pool(),
        "seed-tenant-b",
        "seed-owner",
        "Echo report",
        "Echo evidence.",
        10,
    )
    .await?;
    let app = admin_router(
        Lifecycle::starting(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
    );

    // A blank query browses by recency.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/search?tenant=seed-tenant-b")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    // A non-blank query serves lexical ranking with snippets and scores,
    // identical to the library path, without any embeddings configuration.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/search?tenant=seed-tenant-b&q=echo")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await?.to_bytes();
    let page: Value = serde_json::from_slice(&body)?;
    let results = page["results"].as_array().ok_or("results missing")?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["title"], "Echo report");
    assert!(
        results[0]["snippet"].is_string(),
        "lexical parity keeps snippets"
    );
    assert!(results[0]["rank"].as_f64().is_some_and(|rank| rank > 0.0));

    let response = app
        .oneshot(Request::builder().uri("/metrics").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await?.to_bytes();
    let exposition = String::from_utf8(body.to_vec())?;
    assert!(exposition.contains("# TYPE search_browse_total counter"));
    assert!(exposition.contains("search_browse_total 1\n"));
    assert!(exposition.contains("# TYPE search_lexical_total counter"));
    assert!(exposition.contains("search_lexical_total 1\n"));
    assert!(exposition.contains("search_hybrid_total 0\n"));
    assert!(exposition.contains("embedding_index_passes_total 0\n"));
    assert!(exposition.contains("embedding_sources_indexed_total 0\n"));
    assert!(exposition.contains("embedding_index_failures_total 0\n"));

    database.cleanup().await?;
    Ok(())
}
/// Builds a hybrid retriever over one loopback fake `/embeddings` endpoint
/// whose single scripted reply echoes a valid zero vector.
///
/// The caller must keep the returned transport alive while requests are in
/// flight; dropping it stops the fake endpoint.
async fn fake_hybrid_retriever(
    pool: sqlx::PgPool,
) -> Result<
    (
        FakeTransport,
        Arc<ratatoskr_knowledge_service::HybridSearchRetriever>,
    ),
    Box<dyn std::error::Error>,
> {
    let envelope = serde_json::json!({
        "data": [{ "index": 0, "embedding": vec![0.0_f32; 1536] }],
        "usage": { "prompt_tokens": 3 }
    });
    let transport =
        FakeTransport::start(vec![FakeReply::bytes(200, serde_json::to_vec(&envelope)?)]).await?;
    let settings = EmbeddingsSettings {
        base_url: format!("http://{}", transport.local_addr()),
        model: "fixture-embedder".to_owned(),
        credential: ProviderSecret::new("secret-key".to_owned()),
        dimensions: 1536,
        prompt_version: "none.v1".to_owned(),
        max_input_characters: 8_000,
        response_byte_cap: 65_536,
        call_deadline: Duration::from_secs(5),
        connect_timeout: Duration::from_secs(1),
        retry: RetryPolicy::new(1, 0, 0),
    };
    let adapter = OpenAiCompatibleEmbeddings::new(settings)?;
    let retriever = Arc::new(HybridRetriever::new(ControlledEmbeddings::new(
        adapter,
        Arc::new(RateLimiter::new(Duration::ZERO)),
        BudgetLedger::new(pool),
        BudgetLimits {
            daily_tokens: u64::MAX,
            monthly_tokens: u64::MAX,
            daily_cost_micro_usd: u64::MAX,
            monthly_cost_micro_usd: u64::MAX,
        },
        TokenPrices {
            input_micro_usd_per_mtoken: 0,
            output_micro_usd_per_mtoken: 0,
        },
    )));
    Ok((transport, retriever))
}

#[tokio::test]
async fn search_uses_the_hybrid_retrieval_selection_when_configured()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    seed_search_hit(
        database.database.pool(),
        "seed-tenant-c",
        "seed-owner",
        "Echo report",
        "Echo evidence.",
        10,
    )
    .await?;
    let (transport, retriever) = fake_hybrid_retriever(database.database.pool().clone()).await?;
    let _transport_guard = transport;
    let app = admin_router(
        Lifecycle::starting(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        Some(Arc::clone(&retriever)),
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/search?tenant=seed-tenant-c&q=echo")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await?.to_bytes();
    let page: Value = serde_json::from_slice(&body)?;
    let results = page["results"].as_array().ok_or("results missing")?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["title"], "Echo report");

    let response = app
        .oneshot(Request::builder().uri("/metrics").body(Body::empty())?)
        .await?;
    let body = response.into_body().collect().await?.to_bytes();
    let exposition = String::from_utf8(body.to_vec())?;
    assert!(
        exposition.contains("search_hybrid_total 1\n"),
        "the hybrid path must be recorded: {exposition}"
    );

    database.cleanup().await?;
    Ok(())
}

/// Projects one searchable row directly, simulating an accepted analysis
/// whose output landed `age_seconds` ago.
async fn seed_search_hit(
    pool: &sqlx::PgPool,
    tenant: &str,
    owner_context: &str,
    title: &str,
    lead: &str,
    age_seconds: i64,
) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
    let source_ref_id: uuid::Uuid = sqlx::query_scalar(
        "insert into knowledge.source_refs (
             source_ref_id, tenant_ref, owner_context, source_document_id,
             content_digest_algorithm, content_digest_hex, source_blob
         )
         values (gen_random_uuid(), $1, $2, gen_random_uuid()::text, 'sha256', $3, '{}'::jsonb)
         returning source_ref_id",
    )
    .bind(tenant)
    .bind(owner_context)
    .bind("a".repeat(64))
    .fetch_one(pool)
    .await?;
    let run_id = uuid::Uuid::now_v7();
    let output_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         ) values ($1, $2, $3, 'admin-search', 'admin-search', 'admin-search', 'completed')",
    )
    .bind(run_id)
    .bind(source_ref_id)
    .bind(format!("admin-search-{output_id}"))
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into knowledge.analysis_outputs (
             output_id, run_id, result, raw_response, accepted
         ) values ($1, $2, '{}'::jsonb, '{}'::jsonb, true)",
    )
    .bind(output_id)
    .bind(run_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref,
             owner_context, document_id, title, lead, body, updated_at
         )
         values (
             gen_random_uuid(), $1, $2, $3, $4,
             gen_random_uuid(), $5, $6, '',
             now() - make_interval(secs => $7::double precision)
         )",
    )
    .bind(source_ref_id)
    .bind(output_id)
    .bind(tenant)
    .bind(owner_context)
    .bind(title)
    .bind(lead)
    .bind(age_seconds)
    .execute(pool)
    .await?;
    Ok(output_id)
}
