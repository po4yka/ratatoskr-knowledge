//! Operator-plane state tests.

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt as _;
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge_service::{Lifecycle, admin_router};
use serde_json::Value;
use tower::ServiceExt as _;

#[tokio::test]
async fn readiness_follows_storage_startup_and_drain() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let lifecycle = Lifecycle::starting();
    let app = admin_router(lifecycle.clone(), database.database.clone());

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
    let app = admin_router(Lifecycle::starting(), database.database.clone());

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

/// Projects one searchable row directly, simulating an accepted analysis
/// whose output landed `age_seconds` ago.
async fn seed_search_hit(
    pool: &sqlx::PgPool,
    tenant: &str,
    owner_context: &str,
    title: &str,
    lead: &str,
    age_seconds: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_ref_id: String = sqlx::query_scalar(
        "insert into knowledge.source_refs (
             source_ref_id, tenant_ref, owner_context, source_document_id,
             content_digest_algorithm, content_digest_hex, source_blob
         )
         values (gen_random_uuid(), $1, $2, gen_random_uuid()::text, 'sha256', $3, '{}'::jsonb)
         returning source_ref_id::text",
    )
    .bind(tenant)
    .bind(owner_context)
    .bind("a".repeat(64))
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref,
             owner_context, document_id, title, lead, body, updated_at
         )
         values (
             gen_random_uuid(), $1::uuid, gen_random_uuid(), $2, $3,
             gen_random_uuid(), $4, $5, '',
             now() - make_interval(secs => $6::double precision)
         )",
    )
    .bind(source_ref_id)
    .bind(tenant)
    .bind(owner_context)
    .bind(title)
    .bind(lead)
    .bind(age_seconds)
    .execute(pool)
    .await?;
    Ok(())
}
