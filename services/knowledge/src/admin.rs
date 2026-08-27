use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use axum::Json;
use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use ratatoskr_knowledge::{
    AnalysisState, CollectionTarget, Database, FeedbackCategory, HighlightAnchor, RankingPath,
    SearchQuery, UserContentError, add_collection_item, create_collection, create_highlight,
    create_tag, list_collection_items, merge_tags, move_collection_item, record_feedback,
    search_page, set_analysis_state, tag_analysis, tag_name,
};

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
        .route("/internal/user-content/command", post(user_content_command))
        .route("/internal/user-content/collection", get(collection_items))
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

#[derive(serde::Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum UserContentCommand {
    CreateTag {
        tenant: String,
        name: String,
    },
    MergeTags {
        tenant: String,
        source_tag_id: uuid::Uuid,
        destination_tag_id: uuid::Uuid,
    },
    TagAnalysis {
        tenant: String,
        tag_id: uuid::Uuid,
        output_id: uuid::Uuid,
    },
    CreateCollection {
        tenant: String,
        name: String,
    },
    AddCollectionItem {
        tenant: String,
        collection_id: uuid::Uuid,
        target: CollectionTarget,
        position: Option<u32>,
    },
    MoveCollectionItem {
        tenant: String,
        collection_id: uuid::Uuid,
        target: CollectionTarget,
        destination: u32,
    },
    SetAnalysisState {
        tenant: String,
        output_id: uuid::Uuid,
        state: AnalysisState,
    },
    RecordFeedback {
        tenant: String,
        output_id: uuid::Uuid,
        category: FeedbackCategory,
        detail: Option<String>,
    },
    CreateHighlight {
        tenant: String,
        document: ratatoskr_document_contracts::Document,
        anchor: HighlightAnchor,
    },
}

async fn user_content_command(
    axum::extract::State(state): axum::extract::State<AdminState>,
    command: Result<Json<UserContentCommand>, JsonRejection>,
) -> Response {
    let Ok(Json(command)) = command else {
        return bad_request("invalid_json");
    };
    let result = match command {
        UserContentCommand::CreateTag { tenant, name } => match tag_name(&name) {
            Ok(name) => create_tag(state.database.pool(), &tenant, name)
                .await
                .map(|id| serde_json::json!({"tag_id": id})),
            Err(error) => Err(error),
        },
        UserContentCommand::MergeTags {
            tenant,
            source_tag_id,
            destination_tag_id,
        } => merge_tags(
            state.database.pool(),
            &tenant,
            source_tag_id,
            destination_tag_id,
        )
        .await
        .map(|()| serde_json::json!({})),
        UserContentCommand::TagAnalysis {
            tenant,
            tag_id,
            output_id,
        } => tag_analysis(state.database.pool(), &tenant, tag_id, output_id)
            .await
            .map(|()| serde_json::json!({})),
        UserContentCommand::CreateCollection { tenant, name } => {
            create_collection(state.database.pool(), &tenant, &name)
                .await
                .map(|id| serde_json::json!({"collection_id": id}))
        }
        UserContentCommand::AddCollectionItem {
            tenant,
            collection_id,
            target,
            position,
        } => add_collection_item(
            state.database.pool(),
            &tenant,
            collection_id,
            target,
            position,
        )
        .await
        .map(|item| serde_json::json!({"item": item})),
        UserContentCommand::MoveCollectionItem {
            tenant,
            collection_id,
            target,
            destination,
        } => move_collection_item(
            state.database.pool(),
            &tenant,
            collection_id,
            target,
            destination,
        )
        .await
        .map(|()| serde_json::json!({})),
        UserContentCommand::SetAnalysisState {
            tenant,
            output_id,
            state: analysis_state,
        } => set_analysis_state(state.database.pool(), &tenant, output_id, analysis_state)
            .await
            .map(|state| serde_json::json!({"state": state})),
        UserContentCommand::RecordFeedback {
            tenant,
            output_id,
            category,
            detail,
        } => record_feedback(
            state.database.pool(),
            &tenant,
            output_id,
            category,
            detail.as_deref(),
        )
        .await
        .map(|id| serde_json::json!({"feedback_id": id})),
        UserContentCommand::CreateHighlight {
            tenant,
            document,
            anchor,
        } => create_highlight(state.database.pool(), &tenant, &document, anchor)
            .await
            .map(|id| serde_json::json!({"highlight_id": id})),
    };
    user_content_response(result)
}

fn user_content_response(result: Result<serde_json::Value, UserContentError>) -> Response {
    match result {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(UserContentError::Invalid) => bad_request("invalid_user_content"),
        Err(UserContentError::Conflict) => json_response(
            StatusCode::CONFLICT,
            &serde_json::json!({"error":"user_content_conflict"}),
        ),
        Err(UserContentError::NotFound) => json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error":"user_content_not_found"}),
        ),
        Err(_) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error":"user_content_unavailable"}),
        ),
    }
}

#[derive(serde::Deserialize)]
struct CollectionParams {
    tenant: Option<String>,
    collection_id: Option<uuid::Uuid>,
}

async fn collection_items(
    axum::extract::State(state): axum::extract::State<AdminState>,
    axum::extract::Query(params): axum::extract::Query<CollectionParams>,
) -> Response {
    let (Some(tenant), Some(collection_id)) = (params.tenant, params.collection_id) else {
        return bad_request("missing_tenant_or_collection");
    };
    match list_collection_items(state.database.pool(), &tenant, collection_id).await {
        Ok(items) => json_response(StatusCode::OK, &serde_json::json!({"items":items})),
        Err(UserContentError::NotFound) => json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error":"user_content_not_found"}),
        ),
        Err(_) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error":"user_content_unavailable"}),
        ),
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
