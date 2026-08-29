//! Authenticated channel-digest result projection tests.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt as _;
use ratatoskr_event_envelope::CommandEnvelope;
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{ChannelRecapInbox, ChannelRecapInboxAdmission, ResultReaderSecret};
use ratatoskr_knowledge_service::{Lifecycle, Metrics, admin_router};
use sha2::{Digest as _, Sha256};
use tower::ServiceExt as _;

const READER_SECRET: &str = "knowledge-result-reader-test-secret";

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one route matrix proves uniform auth, absence, integrity, and storage outcomes"
)]
async fn result_route_requires_service_auth_and_returns_only_the_typed_recap()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let (analysis_id, recap, digest) = seed_completed_recap(&database).await?;
    let app = admin_router(
        Lifecycle::starting(),
        database.database.clone(),
        Arc::new(Metrics::new()),
        None,
        Some(ResultReaderSecret::new(READER_SECRET.to_owned())),
    );
    let path = format!("/internal/channel-digest-results/{analysis_id}");

    let missing_auth = request(&app, &path, None).await?;
    assert_eq!(missing_auth.status, StatusCode::UNAUTHORIZED);
    let wrong_auth = request(&app, &path, Some("wrong-reader-secret")).await?;
    assert_eq!(wrong_auth.status, StatusCode::UNAUTHORIZED);
    assert_eq!(missing_auth.body, wrong_auth.body);
    assert_safe_failure(&missing_auth, &[READER_SECRET, &analysis_id.to_string()]);

    let success = request(&app, &path, Some(READER_SECRET)).await?;
    assert_eq!(success.status, StatusCode::OK);
    assert_eq!(success.cache_control.as_deref(), Some("no-store"));
    assert!(success.body.len() <= 65_536);
    let projection: serde_json::Value = serde_json::from_slice(&success.body)?;
    assert_eq!(projection["analysis_id"], analysis_id.to_string());
    assert_eq!(projection["result_digest"]["algorithm"], "sha256");
    assert_eq!(projection["result_digest"]["hex"], digest);
    assert_eq!(projection["recap"], recap);
    for forbidden in ["raw_response", "\"prompt\":", "source post body"] {
        assert!(!String::from_utf8_lossy(&success.body).contains(forbidden));
    }

    let malformed = request(
        &app,
        "/internal/channel-digest-results/not-a-uuid",
        Some(READER_SECRET),
    )
    .await?;
    assert_eq!(malformed.status, StatusCode::BAD_REQUEST);
    assert_safe_failure(&malformed, &[READER_SECRET, "not-a-uuid"]);

    let mut absence_bodies = Vec::new();
    for absent in [uuid::Uuid::now_v7(), uuid::Uuid::now_v7()] {
        let response = request(
            &app,
            &format!("/internal/channel-digest-results/{absent}"),
            Some(READER_SECRET),
        )
        .await?;
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert_safe_failure(&response, &[READER_SECRET, &absent.to_string()]);
        absence_bodies.push(response.body);
    }
    assert_eq!(absence_bodies[0], absence_bodies[1]);

    sqlx::query(
        "update knowledge.channel_recap_results set result_digest_hex = $2 where result_id = $1",
    )
    .bind(analysis_id)
    .bind("1111111111111111111111111111111111111111111111111111111111111111")
    .execute(database.database.pool())
    .await?;
    let corrupt = request(&app, &path, Some(READER_SECRET)).await?;
    assert_eq!(corrupt.status, StatusCode::BAD_GATEWAY);
    assert_safe_failure(&corrupt, &[READER_SECRET, "Grounded fixture recap"]);

    database.database.close().await;
    let unavailable = request(&app, &path, Some(READER_SECRET)).await?;
    assert_eq!(unavailable.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_safe_failure(&unavailable, &[READER_SECRET, "Grounded fixture recap"]);

    database.cleanup().await?;
    Ok(())
}

struct ObservedResponse {
    status: StatusCode,
    cache_control: Option<String>,
    body: Vec<u8>,
}

async fn request(
    app: &axum::Router,
    path: &str,
    bearer: Option<&str>,
) -> Result<ObservedResponse, Box<dyn std::error::Error>> {
    let mut request = Request::builder().uri(path);
    if let Some(bearer) = bearer {
        request = request.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    }
    let response = app.clone().oneshot(request.body(Body::empty())?).await?;
    let status = response.status();
    let cache_control = response
        .headers()
        .get(header::CACHE_CONTROL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.into_body().collect().await?.to_bytes().to_vec();
    Ok(ObservedResponse {
        status,
        cache_control,
        body,
    })
}

fn assert_safe_failure(response: &ObservedResponse, forbidden: &[&str]) {
    assert_eq!(response.cache_control.as_deref(), Some("no-store"));
    assert!(response.body.len() <= 256);
    let body = String::from_utf8_lossy(&response.body);
    for value in forbidden {
        assert!(!body.contains(value));
    }
}

async fn seed_completed_recap(
    database: &TestDatabase,
) -> Result<(uuid::Uuid, serde_json::Value, String), Box<dyn std::error::Error>> {
    let command: CommandEnvelope = serde_json::from_value(serde_json::json!({
        "command_id": "018f0000-0000-7000-8000-000000000211",
        "command_type": "knowledge.channel_digest_recap.requested.v1",
        "issued_at": "2026-08-21T10:00:01Z",
        "producer": "ratatoskr-channel-digests",
        "aggregate_id": "channel-digest-run:018f0000-0000-7000-8000-000000000212",
        "correlation_id": "operation:018f0000-0000-7000-8000-000000000213",
        "tenant_id": "user:018f0000-0000-7000-8000-000000000214",
        "schema_version": 1,
        "payload": {
            "operation_id": "018f0000-0000-7000-8000-000000000213",
            "owner": "user:018f0000-0000-7000-8000-000000000214",
            "digest_run_id": "018f0000-0000-7000-8000-000000000212",
            "window": {"start_at": "2026-08-20T10:00:00Z", "end_at": "2026-08-21T10:00:00Z"},
            "output_language": "ru",
            "source_count": 1,
            "channel_count": 1,
            "manifest_ref": "channel-digest-manifest:018f0000-0000-7000-8000-000000000215",
            "manifest_digest": {"algorithm": "sha256", "hex": "0000000000000000000000000000000000000000000000000000000000000000"},
            "analysis_family": "channel_digest_recap",
            "analysis_contract": "channel_digest_recap.v1"
        }
    }))?;
    assert_eq!(
        ChannelRecapInbox::new(&database.database)
            .accept(&command)
            .await?,
        ChannelRecapInboxAdmission::Accepted
    );
    let run_id: uuid::Uuid = sqlx::query_scalar(
        "select recap_run_id from knowledge.channel_recap_runs where inbox_command_id = $1",
    )
    .bind(command.command_id.0)
    .fetch_one(database.database.pool())
    .await?;
    let analysis_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000216")?;
    let recap = serde_json::json!({
        "contract_version": "channel_digest_recap.v1",
        "prompt_version": "channel_digest_recap_prompt.v1",
        "context_version": "channel_digest_recap_context.v1",
        "output_language": "ru",
        "manifest_digest": {"algorithm": "sha256", "hex": "0000000000000000000000000000000000000000000000000000000000000000"},
        "headline": "Grounded fixture recap",
        "overview": "One bounded event is summarized.",
        "topics": [{"label": "Fixture", "summary": "Grounded summary.", "citations": ["channel-post-revision:018f0000-0000-7000-8000-000000000217"]}],
        "notable_items": [],
        "coverage": {"selected_count": 1, "included_count": 1, "omitted_count": 0, "channel_count": 1},
        "warnings": []
    });
    let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&recap)?));
    let coverage = recap.get("coverage").ok_or("recap coverage is missing")?;
    sqlx::query(
        "insert into knowledge.channel_recap_results
             (result_id, recap_run_id, result, result_digest_hex, coverage)
         values ($1, $2, $3, $4, $5)",
    )
    .bind(analysis_id)
    .bind(run_id)
    .bind(&recap)
    .bind(&digest)
    .bind(coverage)
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "update knowledge.channel_recap_runs set state = 'completed' where recap_run_id = $1",
    )
    .bind(run_id)
    .execute(database.database.pool())
    .await?;
    Ok((analysis_id, recap, digest))
}
