#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Durable first-slice article analysis for Ratatoskr Knowledge.

mod config;

pub use config::{AdminConfig, Config, ConfigError, Limits};
