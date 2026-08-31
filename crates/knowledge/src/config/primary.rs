use std::path::{Path, PathBuf};

use serde::Serialize;

use super::{Config, ConfigError, parse_positive, url_is_loopback};

/// Explicit process role selected at deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeRole {
    /// Operator/search-only process with no primary event consumption.
    Admin,
    /// Full primary intake, leased analysis, and terminal publication process.
    Primary,
}

/// Exact primary-stream and GitHub owner-boundary configuration.
#[derive(Debug, Clone, Serialize)]
pub struct PrimaryConfig {
    /// NATS or TLS NATS endpoint.
    pub bus_endpoint: String,
    /// Canonical pre-provisioned event stream.
    pub bus_stream: String,
    /// Canonical pre-provisioned primary durable.
    pub bus_durable: String,
    /// Optional absolute `NKey` seed file; required for remote TLS NATS.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bus_credentials_file: Option<PathBuf>,
    /// GitHub Catalog's internal README-resolution origin.
    pub github_base_url: String,
    /// Absolute bounded bearer-token file shared with GitHub Catalog.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_token_file: Option<PathBuf>,
    /// Maximum messages fetched in one pull batch.
    pub fetch_batch: u32,
    /// Platform-provisioned acknowledgement deadline.
    pub ack_wait_seconds: u64,
    /// Number of leased analysis workers.
    pub worker_count: u32,
    /// Work lease duration.
    pub lease_seconds: u64,
    /// Maximum README response bytes.
    pub readme_response_bytes: usize,
}

pub(super) fn apply_primary_entry(
    primary: &mut PrimaryConfig,
    key: &str,
    value: &str,
) -> Result<(), ConfigError> {
    match key {
        "RATATOSKR__PRIMARY__BUS_ENDPOINT" => value.clone_into(&mut primary.bus_endpoint),
        "RATATOSKR__PRIMARY__BUS_STREAM" => value.clone_into(&mut primary.bus_stream),
        "RATATOSKR__PRIMARY__BUS_DURABLE" => value.clone_into(&mut primary.bus_durable),
        "RATATOSKR__PRIMARY__BUS_CREDENTIALS_FILE" => {
            primary.bus_credentials_file = Some(PathBuf::from(value));
        }
        "RATATOSKR__PRIMARY__GITHUB_BASE_URL" => value.clone_into(&mut primary.github_base_url),
        "RATATOSKR__PRIMARY__GITHUB_TOKEN_FILE" => {
            primary.github_token_file = Some(PathBuf::from(value));
        }
        "RATATOSKR__PRIMARY__FETCH_BATCH" => {
            primary.fetch_batch = parse_positive(key, value)?;
        }
        "RATATOSKR__PRIMARY__ACK_WAIT_SECONDS" => {
            primary.ack_wait_seconds = parse_positive(key, value)?;
        }
        "RATATOSKR__PRIMARY__WORKER_COUNT" => {
            primary.worker_count = parse_positive(key, value)?;
        }
        "RATATOSKR__PRIMARY__LEASE_SECONDS" => {
            primary.lease_seconds = parse_positive(key, value)?;
        }
        "RATATOSKR__PRIMARY__README_RESPONSE_BYTES" => {
            primary.readme_response_bytes = parse_positive(key, value)?;
        }
        _ => return Err(ConfigError::new(key, "is not recognized")),
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one strict validator owns the complete primary-role security boundary"
)]
pub(super) fn validate_primary(config: &Config) -> Result<(), ConfigError> {
    if config.runtime_role != RuntimeRole::Primary {
        return Ok(());
    }
    let primary = &config.primary;
    if config.provider.openrouter.is_none() {
        return Err(ConfigError::new(
            "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
            "is required for the primary role",
        ));
    }
    let bus = reqwest::Url::parse(&primary.bus_endpoint).map_err(|_| {
        ConfigError::new(
            "RATATOSKR__PRIMARY__BUS_ENDPOINT",
            "must be a valid NATS URL",
        )
    })?;
    let allowed_bus = matches!(bus.scheme(), "tls" | "nats")
        && (bus.scheme() == "tls" || url_is_loopback(&bus))
        && bus.username().is_empty()
        && bus.password().is_none()
        && bus.query().is_none()
        && bus.fragment().is_none();
    if !allowed_bus {
        return Err(ConfigError::new(
            "RATATOSKR__PRIMARY__BUS_ENDPOINT",
            "must use TLS or loopback NATS",
        ));
    }
    if let Some(path) = primary.bus_credentials_file.as_ref() {
        validate_secret_file(path, "RATATOSKR__PRIMARY__BUS_CREDENTIALS_FILE")?;
    }
    if bus.scheme() == "tls" && primary.bus_credentials_file.is_none() {
        return Err(ConfigError::new(
            "RATATOSKR__PRIMARY__BUS_CREDENTIALS_FILE",
            "must be a readable absolute file for remote TLS NATS",
        ));
    }
    for (actual, expected, key) in [
        (
            primary.bus_stream.as_str(),
            "ratatoskr_events",
            "RATATOSKR__PRIMARY__BUS_STREAM",
        ),
        (
            primary.bus_durable.as_str(),
            "ratatoskr_knowledge_main",
            "RATATOSKR__PRIMARY__BUS_DURABLE",
        ),
    ] {
        if actual != expected {
            return Err(ConfigError::new(
                key,
                "must equal the canonical fleet value",
            ));
        }
    }
    let token_path = primary.github_token_file.as_ref().ok_or_else(|| {
        ConfigError::new(
            "RATATOSKR__PRIMARY__GITHUB_TOKEN_FILE",
            "is required for the primary role",
        )
    })?;
    validate_secret_file(token_path, "RATATOSKR__PRIMARY__GITHUB_TOKEN_FILE")?;
    let github = reqwest::Url::parse(&primary.github_base_url).map_err(|_| {
        ConfigError::new(
            "RATATOSKR__PRIMARY__GITHUB_BASE_URL",
            "must be an HTTPS or private internal URL",
        )
    })?;
    let host = github.host_str().unwrap_or_default();
    let private_http = github.scheme() == "http"
        && (url_is_loopback(&github)
            || (!host.contains('.') && !host.contains(':'))
            || host.parse::<std::net::IpAddr>().is_ok_and(|ip| match ip {
                std::net::IpAddr::V4(ip) => ip.is_private() || ip.is_link_local(),
                std::net::IpAddr::V6(ip) => ip.is_unique_local() || ip.is_unicast_link_local(),
            }));
    if (github.scheme() != "https" && !private_http)
        || !github.username().is_empty()
        || github.password().is_some()
        || github.query().is_some()
        || github.fragment().is_some()
    {
        return Err(ConfigError::new(
            "RATATOSKR__PRIMARY__GITHUB_BASE_URL",
            "must be an HTTPS or private internal URL",
        ));
    }
    if !(1..=256).contains(&primary.fetch_batch)
        || !(1..=600).contains(&primary.ack_wait_seconds)
        || !(1..=32).contains(&primary.worker_count)
        || !(5..=3_600).contains(&primary.lease_seconds)
        || !(1..=1_048_576).contains(&primary.readme_response_bytes)
    {
        return Err(ConfigError::new(
            "RATATOSKR__PRIMARY__FETCH_BATCH",
            "primary runtime limits are invalid",
        ));
    }
    let lease_ms = primary.lease_seconds.saturating_mul(1_000);
    if lease_ms < config.limits.provider_timeout_ms.saturating_add(5_000) {
        return Err(ConfigError::new(
            "RATATOSKR__PRIMARY__LEASE_SECONDS",
            "must exceed the longest provider or resolver deadline by at least five seconds",
        ));
    }
    Ok(())
}

fn validate_secret_file(path: &Path, key: &'static str) -> Result<(), ConfigError> {
    if !path.is_absolute() || !path.is_file() {
        return Err(ConfigError::new(key, "must be a readable absolute file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let metadata = std::fs::metadata(path)
            .map_err(|_| ConfigError::new(key, "must be a readable absolute file"))?;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ConfigError::new(
                key,
                "must not be readable or writable by group or other users",
            ));
        }
    }
    Ok(())
}
