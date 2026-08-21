#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Durable first-slice article analysis for Ratatoskr Knowledge.

mod config;
mod database;
mod runs;
mod telemetry;

#[cfg(feature = "test-support")]
pub mod test_support;

pub use config::{AdminConfig, Config, ConfigError, Limits};
pub use database::{Database, PersistenceError};
pub use runs::{
    AnalysisIdentity, AnalysisRun, Attempt, AttemptInput, AttemptOutcome, AttemptReason, RunState,
    SourceReference, SourceRevision,
};
pub use telemetry::{TelemetryError, ValidationClass, init_telemetry, record_validation_failure};
