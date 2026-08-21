#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Process boundary for Ratatoskr Knowledge.

mod admin;

pub use admin::{Lifecycle, admin_router};
