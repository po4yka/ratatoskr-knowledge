//! Domain API listener configuration (`RATATOSKR__API__*`, XR-021 CONTRACTS.md R2-01).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::Serialize;

use crate::Config;
use crate::config::ConfigError;

const LISTEN_ADDRESS: &str = "RATATOSKR__API__LISTEN_ADDRESS";

/// Loopback-only listener for the library surface that Platform calls.
///
/// Edge routes the Knowledge domain API to `127.0.0.1:8091`. The operator listener keeps health,
/// metrics and build identity on a separate host-only port, so the two never share a route.
#[derive(Debug, Clone, Serialize)]
pub struct ApiConfig {
    /// Socket address for capabilities, search and user-content routes.
    pub listen_address: SocketAddr,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8091),
        }
    }
}

/// Applies one `RATATOSKR__API__*` entry.
pub(crate) fn apply_api_entry(
    api: &mut ApiConfig,
    key: &str,
    value: &str,
) -> Result<(), ConfigError> {
    match key {
        LISTEN_ADDRESS => {
            let address = value
                .parse::<SocketAddr>()
                .map_err(|_| ConfigError::new(key, "must be a socket address"))?;
            if !address.ip().is_loopback() || address.port() == 0 {
                return Err(ConfigError::new(
                    key,
                    "must be a loopback address with a port",
                ));
            }
            api.listen_address = address;
            Ok(())
        }
        _ => Err(ConfigError::new(key, "is not recognized")),
    }
}

/// Refuses an API listener that would share its socket address with the operator listener.
pub(crate) fn validate_listeners(config: &Config) -> Result<(), ConfigError> {
    if config.api.listen_address == config.admin.listen_address {
        return Err(ConfigError::new(
            LISTEN_ADDRESS,
            "must differ from the operator listener address RATATOSKR__ADMIN__LISTEN_ADDRESS",
        ));
    }
    Ok(())
}
