use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use axum::Json;
use axum::Router;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use ratatoskr_knowledge::{
    AnalysisState, ChannelRecapResultReadError, ChannelRecapRunStore, CollectionTarget, Database,
    FeedbackCategory, HighlightAnchor, RankingPath, ReadState, ReadStateFilter, ResultReaderSecret,
    SearchQuery, UserContentError, add_collection_item, create_collection, create_highlight,
    create_tag, list_collection_items, merge_tags, move_collection_item, record_feedback,
    search_page, set_analysis_state, set_read_state, tag_analysis, tag_name,
};

use crate::{HybridSearchRetriever, Metrics};

const STORAGE_READY: u8 = 1;
const CHANNEL_RECAP_REQUIRED: u8 = 2;
const CHANNEL_RECAP_READY: u8 = 4;
const DRAINING: u8 = 8;
const PRIMARY_REQUIRED: u8 = 16;
const PRIMARY_BUS_READY: u8 = 32;
const PRIMARY_WORKERS_READY: u8 = 64;
const PRIMARY_OUTBOX_READY: u8 = 128;
const CHANNEL_DIGEST_RESULT_RESPONSE_BYTES: usize = 65_536;

/// Fixed loopback route for one completed Knowledge-owned channel recap.
pub const CHANNEL_DIGEST_RESULT_ROUTE: &str = "/internal/channel-digest-results/{analysis_id}";

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
            state: Arc::new(AtomicU8::new(0)),
        }
    }

    /// Creates a starting lifecycle that will require recap dependencies.
    #[must_use]
    pub fn starting_with_channel_recap() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(CHANNEL_RECAP_REQUIRED)),
        }
    }

    /// Creates a starting lifecycle that requires the full primary supervisor set.
    #[must_use]
    pub fn starting_primary(channel_recap: bool) -> Self {
        let recap = if channel_recap {
            CHANNEL_RECAP_REQUIRED
        } else {
            0
        };
        Self {
            state: Arc::new(AtomicU8::new(PRIMARY_REQUIRED | recap)),
        }
    }

    /// Records whether both recap transport and source dependencies are usable.
    pub fn set_channel_recap_ready(&self, ready: bool) {
        if ready {
            self.state.fetch_or(CHANNEL_RECAP_READY, Ordering::AcqRel);
        } else {
            self.state.fetch_and(!CHANNEL_RECAP_READY, Ordering::AcqRel);
        }
    }

    /// Records whether the exact primary durable is open on a live broker connection.
    pub fn set_primary_bus_ready(&self, ready: bool) {
        self.set_flag(PRIMARY_BUS_READY, ready);
    }

    /// Records whether every configured leased analysis worker is alive.
    pub fn set_primary_workers_ready(&self, ready: bool) {
        self.set_flag(PRIMARY_WORKERS_READY, ready);
    }

    /// Records whether the independent terminal outbox publisher is connected and alive.
    pub fn set_primary_outbox_ready(&self, ready: bool) {
        self.set_flag(PRIMARY_OUTBOX_READY, ready);
    }

    fn set_flag(&self, flag: u8, ready: bool) {
        if ready {
            self.state.fetch_or(flag, Ordering::AcqRel);
        } else {
            self.state.fetch_and(!flag, Ordering::AcqRel);
        }
    }

    /// Marks storage startup complete.
    pub fn mark_ready(&self) {
        self.state.fetch_or(STORAGE_READY, Ordering::AcqRel);
    }

    /// Starts drain and makes readiness fail.
    pub fn begin_drain(&self) {
        self.state.fetch_or(DRAINING, Ordering::AcqRel);
    }

    fn is_ready(&self) -> bool {
        let state = self.state.load(Ordering::Acquire);
        state & STORAGE_READY != 0
            && state & DRAINING == 0
            && (state & CHANNEL_RECAP_REQUIRED == 0 || state & CHANNEL_RECAP_READY != 0)
            && (state & PRIMARY_REQUIRED == 0
                || state & (PRIMARY_BUS_READY | PRIMARY_WORKERS_READY | PRIMARY_OUTBOX_READY)
                    == (PRIMARY_BUS_READY | PRIMARY_WORKERS_READY | PRIMARY_OUTBOX_READY))
    }
}

/// Shared state behind every operator-plane route.
#[derive(Debug, Clone)]
struct AdminState {
    lifecycle: Lifecycle,
    database: Database,
    metrics: Arc<Metrics>,
    retriever: Option<Arc<HybridSearchRetriever>>,
    result_reader_secret: Option<ResultReaderSecret>,
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
    result_reader_secret: Option<ResultReaderSecret>,
) -> Router {
    let state = AdminState {
        lifecycle,
        database,
        metrics,
        retriever,
        result_reader_secret,
    };
    let router = Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_route))
        .route("/version", get(version))
        .route("/v1/capabilities", get(capabilities))
        .route("/internal/search", get(search))
        .route("/internal/user-content/command", post(user_content_command))
        .route("/internal/user-content/collection", get(collection_items));
    let router = if state.result_reader_secret.is_some() {
        router.route(CHANNEL_DIGEST_RESULT_ROUTE, get(channel_digest_result))
    } else {
        router
    };
    router
        .with_state(state)
        .layer(middleware::from_fn(no_store))
}

async fn channel_digest_result(
    axum::extract::State(state): axum::extract::State<AdminState>,
    headers: HeaderMap,
    axum::extract::Path(analysis_id): axum::extract::Path<String>,
) -> Response {
    let Some(secret) = &state.result_reader_secret else {
        return channel_digest_result_failure(
            StatusCode::NOT_FOUND,
            "channel_digest_result_not_found",
        );
    };
    let supplied = headers
        .get(header::AUTHORIZATION)
        .map_or(&[][..], HeaderValue::as_bytes);
    let expected = format!("Bearer {}", secret.expose_secret());
    if !constant_time_equal(supplied, expected.as_bytes()) {
        return channel_digest_result_failure(StatusCode::UNAUTHORIZED, "result_unauthorized");
    }
    let Ok(analysis_id) = uuid::Uuid::parse_str(&analysis_id) else {
        return channel_digest_result_failure(StatusCode::BAD_REQUEST, "invalid_analysis_id");
    };
    match ChannelRecapRunStore::new(&state.database)
        .read_completed_result(analysis_id)
        .await
    {
        Ok(projection) => {
            let value = serde_json::json!({
                "analysis_id": projection.analysis_id,
                "result_digest": {
                    "algorithm": "sha256",
                    "hex": projection.result_digest_hex,
                },
                "recap": projection.recap,
            });
            match serde_json::to_vec(&value) {
                Ok(body) if body.len() <= CHANNEL_DIGEST_RESULT_RESPONSE_BYTES => (
                    StatusCode::OK,
                    [(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    )],
                    body,
                )
                    .into_response(),
                _ => {
                    tracing::warn!(
                        route = "channel_digest_result",
                        class = "response_integrity"
                    );
                    channel_digest_result_failure(
                        StatusCode::BAD_GATEWAY,
                        "channel_digest_result_integrity",
                    )
                }
            }
        }
        Err(ChannelRecapResultReadError::NotFound) => {
            channel_digest_result_failure(StatusCode::NOT_FOUND, "channel_digest_result_not_found")
        }
        Err(ChannelRecapResultReadError::Integrity) => {
            tracing::warn!(route = "channel_digest_result", class = "stored_integrity");
            channel_digest_result_failure(
                StatusCode::BAD_GATEWAY,
                "channel_digest_result_integrity",
            )
        }
        Err(ChannelRecapResultReadError::Persistence(_)) => {
            tracing::warn!(
                route = "channel_digest_result",
                class = "storage_unavailable"
            );
            channel_digest_result_failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "channel_digest_result_unavailable",
            )
        }
        Err(_) => {
            tracing::warn!(route = "channel_digest_result", class = "read_unavailable");
            channel_digest_result_failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "channel_digest_result_unavailable",
            )
        }
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let length = left.len().max(right.len());
    let left_bytes = left.iter().copied().chain(std::iter::repeat(0));
    let right_bytes = right.iter().copied().chain(std::iter::repeat(0));
    let difference = left_bytes
        .zip(right_bytes)
        .take(length)
        .fold(left.len() ^ right.len(), |difference, (left, right)| {
            difference | usize::from(left ^ right)
        });
    difference == 0
}

fn channel_digest_result_failure(status: StatusCode, code: &'static str) -> Response {
    json_response(status, &serde_json::json!({"error": code}))
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

async fn capabilities() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "service": "knowledge",
        "capabilities": ["library.search", "library.read_state"]
    }))
}

/// Largest permitted page size when the request omits `limit`.
const DEFAULT_SEARCH_LIMIT: i64 = 25;

/// Parsed `/internal/search` query parameters.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchParams {
    tenant: Option<String>,
    q: Option<String>,
    read_state: Option<ReadStateFilter>,
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn search(
    axum::extract::State(state): axum::extract::State<AdminState>,
    params: Result<axum::extract::Query<SearchParams>, QueryRejection>,
) -> Response {
    let Ok(axum::extract::Query(params)) = params else {
        return bad_request("invalid_parameters");
    };
    let Some(tenant) = params.tenant else {
        return bad_request("missing_tenant");
    };
    let Ok(mut query) = SearchQuery::new(
        tenant,
        params.q.unwrap_or_default(),
        params.limit.unwrap_or(DEFAULT_SEARCH_LIMIT),
        params.offset.unwrap_or(0),
    ) else {
        return bad_request("invalid_parameters");
    };
    if let Some(read_state) = params.read_state {
        query = query.with_read_state(read_state);
    }
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
    SetReadState {
        tenant: String,
        output_id: uuid::Uuid,
        read_state: ReadState,
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
        UserContentCommand::SetReadState {
            tenant,
            output_id,
            read_state,
        } => read_state_response(&state.database, &tenant, output_id, read_state).await,
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

async fn read_state_response(
    database: &Database,
    tenant: &str,
    output_id: uuid::Uuid,
    read_state: ReadState,
) -> Result<serde_json::Value, UserContentError> {
    set_read_state(database.pool(), tenant, output_id, read_state)
        .await
        .map(|read_state| serde_json::json!({"read_state": read_state}))
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
