#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Process boundary for Ratatoskr Knowledge.

mod admin;
mod api;
mod bus;
mod channel_recap;
mod channel_recap_delivery;
mod ingest;
mod ingest_deletion;
mod ingest_handlers;
mod ingest_notes;
mod ingest_publish;
mod metrics;
mod provider_runtime;
mod redelivery;

use ratatoskr_knowledge::{ControlledEmbeddings, HybridRetriever, OpenAiCompatibleEmbeddings};

pub use admin::{Lifecycle, Listener, admin_router};
pub use api::{CHANNEL_DIGEST_RESULT_ROUTE, api_router};
pub use bus::BusFailure;
pub use channel_recap::{ChannelRecapWorkerError, spawn_channel_recap_worker};
pub use channel_recap_delivery::run_with_progress;
pub use ingest::{
    EVENTS_STREAM, IngestFamily, PUBLISH_READINESS_GRACE, PublishHealth, TERMINAL_DELIVERY,
    spawn_ingest_workers, spawn_ingest_workers_with_provider, spawn_ingest_workers_with_schedule,
};
pub use ingest_publish::{PublishFailure, publish_event_outbox};
pub use metrics::Metrics;
pub use provider_runtime::{IngestProvider, ProviderRuntimeError, inference_limiter};
pub use redelivery::{Redelivery, RedeliverySchedule};

/// The production hybrid retrieval selector over the controlled embeddings
/// adapter.
pub type HybridSearchRetriever = HybridRetriever<ControlledEmbeddings<OpenAiCompatibleEmbeddings>>;
