use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use axum::Router;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;

use crate::Metrics;

const STORAGE_READY: u8 = 1;
const CHANNEL_RECAP_REQUIRED: u8 = 2;
const CHANNEL_RECAP_READY: u8 = 4;
const DRAINING: u8 = 8;
const INGEST_REQUIRED: u8 = 16;
const INGEST_READY: u8 = 32;
const OPERATOR_BOUND: u8 = 64;
const API_BOUND: u8 = 128;

/// One of the two HTTP listeners the readiness check waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Listener {
    /// Health, metrics and build identity.
    Operator,
    /// Capabilities, search and user-content routes.
    Api,
}

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

    /// Creates a starting lifecycle that will require the ingest consumers.
    #[must_use]
    pub fn starting_with_ingest() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(INGEST_REQUIRED)),
        }
    }

    /// Makes readiness depend on the ingest consumers as well.
    pub fn require_ingest(&self) {
        self.state.fetch_or(INGEST_REQUIRED, Ordering::AcqRel);
    }

    /// Records whether every ingest consumer is verified and the completion publisher is healthy.
    pub fn set_ingest_ready(&self, ready: bool) {
        if ready {
            self.state.fetch_or(INGEST_READY, Ordering::AcqRel);
        } else {
            self.state.fetch_and(!INGEST_READY, Ordering::AcqRel);
        }
    }

    /// Records whether the recap worker's bus connection and durable are verified. The digest
    /// source is deliberately not part of it (XR-021 CONTRACTS.md R3-09).
    pub fn set_channel_recap_ready(&self, ready: bool) {
        if ready {
            self.state.fetch_or(CHANNEL_RECAP_READY, Ordering::AcqRel);
        } else {
            self.state.fetch_and(!CHANNEL_RECAP_READY, Ordering::AcqRel);
        }
    }

    /// Records that one listener is bound; readiness waits for both.
    pub fn mark_listener_bound(&self, listener: Listener) {
        let bit = match listener {
            Listener::Operator => OPERATOR_BOUND,
            Listener::Api => API_BOUND,
        };
        self.state.fetch_or(bit, Ordering::AcqRel);
    }

    /// Marks storage startup complete.
    pub fn mark_ready(&self) {
        self.state.fetch_or(STORAGE_READY, Ordering::AcqRel);
    }

    /// Starts drain and makes readiness fail.
    pub fn begin_drain(&self) {
        self.state.fetch_or(DRAINING, Ordering::AcqRel);
    }

    /// Whether `/ready` would report ready: storage is up, both listeners are bound, no drain has
    /// begun, and every required worker is ready.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        let state = self.state.load(Ordering::Acquire);
        state & STORAGE_READY != 0
            && state & OPERATOR_BOUND != 0
            && state & API_BOUND != 0
            && state & DRAINING == 0
            && (state & CHANNEL_RECAP_REQUIRED == 0 || state & CHANNEL_RECAP_READY != 0)
            && (state & INGEST_REQUIRED == 0 || state & INGEST_READY != 0)
    }
}
/// Shared state behind every operator-plane route.
#[derive(Debug, Clone)]
struct AdminState {
    lifecycle: Lifecycle,
    metrics: Arc<Metrics>,
}

/// Builds the loopback operator router over lifecycle and metrics handles.
///
/// It serves `/live`, `/ready`, `/metrics` and `/version` only; the library surface is on the API
/// router (XR-021 CONTRACTS.md R2-01).
pub fn admin_router(lifecycle: Lifecycle, metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_route))
        .route("/version", get(version))
        .with_state(AdminState { lifecycle, metrics })
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

/// Marks every response uncacheable; shared by the operator and API routers.
pub(crate) async fn no_store(request: axum::extract::Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
