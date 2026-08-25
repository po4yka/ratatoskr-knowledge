#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Process boundary for Ratatoskr Knowledge.

mod admin;
mod metrics;

use ratatoskr_knowledge::{ControlledEmbeddings, HybridRetriever, OpenAiCompatibleEmbeddings};

pub use admin::{Lifecycle, admin_router};
pub use metrics::Metrics;

/// The production hybrid retrieval selector over the controlled embeddings
/// adapter.
pub type HybridSearchRetriever = HybridRetriever<ControlledEmbeddings<OpenAiCompatibleEmbeddings>>;
