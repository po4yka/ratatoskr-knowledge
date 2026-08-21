#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Durable first-slice article analysis for Ratatoskr Knowledge.

mod article;
mod config;
mod context;
mod database;
mod runs;
mod telemetry;

#[cfg(feature = "test-support")]
pub mod test_support;

pub use article::{
    ArticleAnalysis, ArticleValidationError, KeyPoint, article_analysis_schema,
    validate_article_citations, validate_article_json,
};
pub use config::{AdminConfig, Config, ConfigError, Limits};
pub use context::PreparedContext;
pub use database::{Database, PersistenceError};
pub use runs::{
    AnalysisIdentity, AnalysisRun, Attempt, AttemptInput, AttemptOutcome, AttemptReason, RunState,
    SourceReference, SourceRevision,
};
pub use telemetry::{TelemetryError, ValidationClass, init_telemetry, record_validation_failure};
