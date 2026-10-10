//! The ingest worker and the Knowledge identity against a real `JetStream` (XR-021 CONTRACTS.md
//! S02, S03, S04 and S09).
//!
//! The helper modules are compiled into this one test binary because every item they define is
//! used by at least one scenario in it.

#[path = "../ingest_support/mod.rs"]
mod ingest_support;

mod authorized;
mod fixtures;
mod harness;
mod lifecycle;
mod repository;
mod tenant;
