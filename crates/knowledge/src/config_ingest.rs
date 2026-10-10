//! Ingest-worker configuration (`RATATOSKR__INGEST__*`, XR-021 CONTRACTS.md S09).

use std::path::PathBuf;

use serde::Serialize;

use crate::config::{ConfigError, parse_positive, url_is_loopback};
use crate::{Config, ProviderSecret};

const ENABLED: &str = "RATATOSKR__INGEST__ENABLED";
const PROVIDER_MODE: &str = "RATATOSKR__INGEST__PROVIDER_MODE";
const BUS_ENDPOINT: &str = "RATATOSKR__INGEST__BUS_ENDPOINT";
const BUS_CREDENTIALS_FILE: &str = "RATATOSKR__INGEST__BUS_CREDENTIALS_FILE";
const FETCH_BATCH: &str = "RATATOSKR__INGEST__FETCH_BATCH";
const ACK_WAIT_SECONDS: &str = "RATATOSKR__INGEST__ACK_WAIT_SECONDS";
const README_BASE_URL: &str = "RATATOSKR__INGEST__GITHUB_README_BASE_URL";
const README_SECRET: &str = "RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET";

/// Provider selection for the ingest worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestProviderMode {
    /// Configured controlled `OpenRouter` provider.
    OpenRouter,
    /// Deterministic extractive provider for the article family, accepted on a loopback broker.
    Fixture,
}

/// Dormant-by-default ingest consumers, their broker, and the repository README source.
#[derive(Debug, Clone, Serialize)]
pub struct IngestConfig {
    /// Whether the four ingest consumers are part of this process.
    pub enabled: bool,
    /// Provider mode for analysis.
    pub provider_mode: IngestProviderMode,
    /// NATS or TLS NATS endpoint.
    pub bus_endpoint: String,
    /// Optional absolute `NKey` seed file; loopback fixture profiles may use anonymous NATS.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bus_credentials_file: Option<PathBuf>,
    /// Maximum messages pulled per batch.
    pub fetch_batch: u32,
    /// Exact durable acknowledgement deadline.
    pub ack_wait_seconds: u64,
    /// Loopback origin of the GitHub README byte endpoint.
    pub github_readme_base_url: String,
    /// Service-to-service credential for the README byte endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_readme_service_secret: Option<ProviderSecret>,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider_mode: IngestProviderMode::OpenRouter,
            bus_endpoint: "nats://127.0.0.1:4222".to_owned(),
            bus_credentials_file: None,
            fetch_batch: 16,
            ack_wait_seconds: 120,
            github_readme_base_url: "http://127.0.0.1:8092/".to_owned(),
            github_readme_service_secret: None,
        }
    }
}

/// Applies one `RATATOSKR__INGEST__*` entry.
///
/// Values are checked for syntax here; the cross-field rules that only matter while the worker is
/// enabled are in [`validate_ingest`].
pub(crate) fn apply_ingest_entry(
    ingest: &mut IngestConfig,
    key: &str,
    value: &str,
) -> Result<(), ConfigError> {
    match key {
        ENABLED => {
            ingest.enabled = value
                .parse::<bool>()
                .map_err(|_| ConfigError::new(key, "must be true or false"))?;
        }
        PROVIDER_MODE => {
            ingest.provider_mode = match value {
                "openrouter" => IngestProviderMode::OpenRouter,
                "fixture" => IngestProviderMode::Fixture,
                _ => return Err(ConfigError::new(key, "must be openrouter or fixture")),
            };
        }
        BUS_ENDPOINT => value.clone_into(&mut ingest.bus_endpoint),
        BUS_CREDENTIALS_FILE => ingest.bus_credentials_file = Some(PathBuf::from(value)),
        FETCH_BATCH => ingest.fetch_batch = parse_positive(key, value)?,
        ACK_WAIT_SECONDS => ingest.ack_wait_seconds = parse_positive(key, value)?,
        README_BASE_URL => {
            validate_readme_origin(value)?;
            value.clone_into(&mut ingest.github_readme_base_url);
        }
        README_SECRET => {
            if value.is_empty() || value.len() > 4_096 {
                return Err(ConfigError::new(
                    key,
                    "must be a non-empty credential of at most 4096 bytes",
                ));
            }
            ingest.github_readme_service_secret = Some(ProviderSecret::new(value.to_owned()));
        }
        _ => return Err(ConfigError::new(key, "is not recognized")),
    }
    Ok(())
}

fn validate_readme_origin(value: &str) -> Result<(), ConfigError> {
    let rule = "must be a loopback HTTP URL";
    let url = reqwest::Url::parse(value).map_err(|_| ConfigError::new(README_BASE_URL, rule))?;
    if url.scheme() != "http"
        || !url_is_loopback(&url)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::new(README_BASE_URL, rule));
    }
    Ok(())
}

/// Checks the whole ingest section when the worker is enabled.
///
/// A worker that consumes must also be able to finish what it consumes: the bus must be
/// reachable safely, the provider mode must be backed, and the README source must be configured.
pub(crate) fn validate_ingest(config: &Config) -> Result<(), ConfigError> {
    let ingest = &config.ingest;
    if !ingest.enabled {
        return Ok(());
    }
    let bus = reqwest::Url::parse(&ingest.bus_endpoint)
        .map_err(|_| ConfigError::new(BUS_ENDPOINT, "must be a valid NATS URL"))?;
    let allowed_bus = matches!(bus.scheme(), "tls" | "nats")
        && (bus.scheme() == "tls" || url_is_loopback(&bus))
        && bus.username().is_empty()
        && bus.password().is_none()
        && bus.query().is_none()
        && bus.fragment().is_none();
    if !allowed_bus {
        return Err(ConfigError::new(
            BUS_ENDPOINT,
            "must use TLS or loopback NATS",
        ));
    }
    if let Some(path) = ingest.bus_credentials_file.as_ref()
        && (!path.is_absolute() || !path.is_file())
    {
        return Err(ConfigError::new(
            BUS_CREDENTIALS_FILE,
            "must be a readable absolute file",
        ));
    }
    if bus.scheme() == "tls" && ingest.bus_credentials_file.is_none() {
        return Err(ConfigError::new(
            BUS_CREDENTIALS_FILE,
            "must be a readable absolute file for remote TLS NATS",
        ));
    }
    if !(1..=256).contains(&ingest.fetch_batch) {
        return Err(ConfigError::new(FETCH_BATCH, "must be between 1 and 256"));
    }
    if !(1..=600).contains(&ingest.ack_wait_seconds) {
        return Err(ConfigError::new(
            ACK_WAIT_SECONDS,
            "must be between 1 and 600",
        ));
    }
    match ingest.provider_mode {
        IngestProviderMode::Fixture if !url_is_loopback(&bus) => {
            return Err(ConfigError::new(
                PROVIDER_MODE,
                "fixture mode requires a loopback bus endpoint",
            ));
        }
        IngestProviderMode::OpenRouter if config.provider.openrouter.is_none() => {
            return Err(ConfigError::new(
                "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
                "is required for openrouter ingest mode",
            ));
        }
        IngestProviderMode::Fixture | IngestProviderMode::OpenRouter => {}
    }
    if ingest.github_readme_service_secret.is_none() {
        return Err(ConfigError::new(
            README_SECRET,
            "is required when ingest is enabled",
        ));
    }
    validate_readme_origin(&ingest.github_readme_base_url)
}
