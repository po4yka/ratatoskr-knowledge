#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Durable first-slice article analysis for Ratatoskr Knowledge.

mod config;
mod database;

#[cfg(feature = "test-support")]
pub mod test_support;

pub use config::{AdminConfig, Config, ConfigError, Limits};
pub use database::{Database, PersistenceError};
