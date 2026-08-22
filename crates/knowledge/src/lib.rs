#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Durable first-slice article analysis for Ratatoskr Knowledge.

mod article;
mod blob_store;
mod config;
mod context;
mod database;
mod openrouter;
mod pipeline;
mod provider;
mod runs;
mod telemetry;

#[cfg(feature = "test-support")]
pub mod test_support;

pub use article::{
    ArticleAnalysis, ArticleValidationError, KeyPoint, article_analysis_schema,
    validate_article_citations, validate_article_json,
};
pub use blob_store::{BlobError, BlobStore};
pub use config::{AdminConfig, Config, ConfigError, Limits, StorageConfig};
pub use context::{
    ContextError, GenerationRequest, PreparedContext, build_generation_request, prepare_context,
};
pub use database::{Database, PersistenceError};
pub use openrouter::{OpenRouterWireError, chat_completion_body};
pub use pipeline::{ArticlePipeline, PipelineError};
pub use provider::{LlmProvider, ProviderError, ProviderResponse, ProviderUsage, ScriptedProvider};
pub use runs::{
    AnalysisIdentity, AnalysisRun, Attempt, AttemptInput, AttemptOutcome, AttemptReason, RunState,
    SourceReference, SourceRevision,
};
pub use telemetry::{TelemetryError, ValidationClass, init_telemetry, record_validation_failure};
