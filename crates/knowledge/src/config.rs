use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
/// Process configuration with finite built-in limits.
pub struct Config {
    /// Operator listener configuration.
    pub admin: AdminConfig,
    /// Resource and shutdown limits.
    pub limits: Limits,
}

#[derive(Debug, Clone, Serialize)]
/// Loopback-only operator listener configuration.
pub struct AdminConfig {
    /// Socket address for health, metrics, and build identity routes.
    pub listen_address: SocketAddr,
}

#[derive(Debug, Clone, Serialize)]
/// Finite limits used by the first analysis slice.
pub struct Limits {
    /// Maximum database connections.
    pub database_connections: u32,
    /// Maximum wait for a database connection.
    pub database_acquire_timeout_ms: u64,
    /// Maximum duration of one provider call.
    pub provider_timeout_ms: u64,
    /// Maximum Unicode characters in prepared source context.
    pub context_characters: usize,
    /// Maximum bytes in one raw provider response.
    pub raw_response_bytes: usize,
    /// Maximum graceful shutdown duration.
    pub shutdown_timeout_ms: u64,
    /// Maximum bytes accepted by the owned blob store.
    pub blob_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            admin: AdminConfig {
                listen_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9081),
            },
            limits: Limits {
                database_connections: 8,
                database_acquire_timeout_ms: 5_000,
                provider_timeout_ms: 30_000,
                context_characters: 32_000,
                raw_response_bytes: 1_048_576,
                shutdown_timeout_ms: 10_000,
                blob_bytes: 16_777_216,
            },
        }
    }
}
