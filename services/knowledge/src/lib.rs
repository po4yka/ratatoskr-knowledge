#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Process boundary for Ratatoskr Knowledge.

mod admin;
mod channel_recap;
mod metrics;

use ratatoskr_knowledge::{ControlledEmbeddings, HybridRetriever, OpenAiCompatibleEmbeddings};

pub use admin::{CHANNEL_DIGEST_RESULT_ROUTE, Lifecycle, admin_router};
pub use channel_recap::{ChannelRecapWorkerError, spawn_channel_recap_worker};
pub use metrics::Metrics;

/// The production hybrid retrieval selector over the controlled embeddings
/// adapter.
pub type HybridSearchRetriever = HybridRetriever<ControlledEmbeddings<OpenAiCompatibleEmbeddings>>;
