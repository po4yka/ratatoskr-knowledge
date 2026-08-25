use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use axum::Router;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ratatoskr_knowledge::{Database, RankingPath, SearchQuery, search_page};

use crate::{HybridSearchRetriever, Metrics};

const STARTING: u8 = 0;
const READY: u8 = 1;
const DRAINING: u8 = 2;

/// Shared process lifecycle used by readiness checks.
#[derive(Debug, Clone)]
pub struct Lifecycle {
    state: Arc<AtomicU8>,
}

impl Lifecycle {
    /// Creates a starting lifecycle.
    #[must_use]
    pub fn starting() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(STARTING)),
        }
    }

    /// Marks storage startup complete.
    pub fn mark_ready(&self) {
        self.state.store(READY, Ordering::Release);
    }

    /// Starts drain and makes readiness fail.
    pub fn begin_drain(&self) {
        self.state.store(DRAINING, Ordering::Release);
    }

    fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == READY
    }
}

/// Shared state behind every operator-plane route.
#[derive(Debug, Clone)]
struct AdminState {
    lifecycle: Lifecycle,
    database: Database,
    metrics: Arc<Metrics>,
    retriever: Option<Arc<HybridSearchRetriever>>,
}

/// Builds the loopback operator router over lifecycle and storage handles.
///
/// `retriever` is absent when no embeddings credential is configured; the
/// search route then serves the plain lexical path byte-for-byte.
pub fn admin_router(
    lifecycle: Lifecycle,
    database: Database,
    metrics: Arc<Metrics>,
    retriever: Option<Arc<HybridSearchRetriever>>,
) -> Router {
    let state = AdminState {
        lifecycle,
        database,
        metrics,
        retriever,
    };
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_route))
        .route("/version", get(version))
        .route("/internal/search", get(search))
        .with_state(state)
        .layer(middleware::from_fn(no_store))
}

async fn live() -> StatusCode {
    StatusCode::OK
}

async fn ready(axum::extract::State(state): axum::extract::State<AdminState>) -> StatusCode {
    if state.lifecycle.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics_route(axum::extract::State(state): axum::extract::State<AdminState>) -> String {
    state.metrics.render()
}

async fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Largest permitted page size when the request omits `limit`.
const DEFAULT_SEARCH_LIMIT: i64 = 25;

/// Parsed `/internal/search` query parameters.
#[derive(serde::Deserialize)]
struct SearchParams {
    tenant: Option<String>,
    q: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn search(
    axum::extract::State(state): axum::extract::State<AdminState>,
    axum::extract::Query(params): axum::extract::Query<SearchParams>,
) -> Response {
    let Some(tenant) = params.tenant else {
        return bad_request("missing_tenant");
    };
    let Ok(query) = SearchQuery::new(
        tenant,
        params.q.unwrap_or_default(),
        params.limit.unwrap_or(DEFAULT_SEARCH_LIMIT),
        params.offset.unwrap_or(0),
    ) else {
        return bad_request("invalid_parameters");
    };
    let blank = query.raw_query().trim().is_empty();
    let served = if let (Some(retriever), false) = (&state.retriever, blank) {
        retriever
            .page(state.database.pool(), &query)
            .await
            .inspect(|(_, path)| state.metrics.record_ranking_path(*path))
            .map(|(page, _)| page)
    } else {
        let path = if blank {
            RankingPath::BrowseRecent
        } else {
            RankingPath::LexicalOnly
        };
        search_page(state.database.pool(), &query)
            .await
            .inspect(|_| state.metrics.record_ranking_path(path))
    };
    match served {
        Ok(page) => match serde_json::to_value(&page) {
            Ok(value) => json_response(StatusCode::OK, &value),
            Err(_) => search_failed(),
        },
        Err(_) => search_failed(),
    }
}

fn bad_request(code: &'static str) -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        &serde_json::json!({ "error": code }),
    )
}

fn search_failed() -> Response {
    json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        &serde_json::json!({ "error": "search_unavailable" }),
    )
}

fn json_response(status: StatusCode, value: &serde_json::Value) -> Response {
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        value.to_string(),
    )
        .into_response()
}

async fn no_store(request: axum::extract::Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
