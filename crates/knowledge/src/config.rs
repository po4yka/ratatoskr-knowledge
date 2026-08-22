use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::{error, fmt};

use serde::Serialize;

const ENV_PREFIX: &str = "RATATOSKR__";

/// Credential value that redacts itself in diagnostics and serialization.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderSecret(String);

impl ProviderSecret {
    /// Wraps one credential value.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// Exposes the credential only to the adapter's authorization path.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ProviderSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[redacted]")
    }
}

impl Serialize for ProviderSecret {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("[redacted]")
    }
}

#[derive(Debug, Clone, Serialize)]
/// Process configuration with finite built-in limits.
pub struct Config {
    /// Operator listener configuration.
    pub admin: AdminConfig,
    /// Owned durable storage configuration.
    pub storage: StorageConfig,
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
/// `PostgreSQL` and content-addressed storage locations.
pub struct StorageConfig {
    /// Knowledge `PostgreSQL` connection URL.
    #[serde(skip_serializing)]
    pub database_url: String,
    /// Knowledge-owned blob root.
    #[serde(skip_serializing)]
    pub blob_root: PathBuf,
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

/// Configuration loading failure that never includes a supplied value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    key: String,
    rule: &'static str,
}

impl Config {
    /// Loads the current process environment.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for an unknown prefixed key, non-Unicode value, or invalid value.
    pub fn load() -> Result<Self, ConfigError> {
        let mut entries = Vec::new();
        for (key, value) in std::env::vars_os() {
            let Some(key) = key.to_str() else {
                continue;
            };
            if !key.starts_with(ENV_PREFIX) {
                continue;
            }
            let Some(value) = value.to_str() else {
                return Err(ConfigError::new(key, "must contain Unicode text"));
            };
            entries.push((key.to_owned(), value.to_owned()));
        }

        Self::from_environment(entries)
    }

    /// Loads configuration from prefixed environment entries.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for an unknown key or invalid value.
    pub fn from_environment<I, K, V>(entries: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut config = Self::default();
        for (key, value) in entries {
            let key = key.as_ref();
            if !key.starts_with(ENV_PREFIX) {
                continue;
            }
            apply_entry(&mut config, key, value.as_ref())?;
        }

        Ok(config)
    }
}

impl ConfigError {
    fn new(key: &str, rule: &'static str) -> Self {
        Self {
            key: key.to_owned(),
            rule,
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "configuration key {} {}", self.key, self.rule)
    }
}

impl error::Error for ConfigError {}

fn apply_entry(config: &mut Config, key: &str, value: &str) -> Result<(), ConfigError> {
    match key {
        "RATATOSKR__ADMIN__LISTEN_ADDRESS" => {
            let address = value
                .parse::<SocketAddr>()
                .map_err(|_| ConfigError::new(key, "must be a socket address"))?;
            if !address.ip().is_loopback() || address.port() == 0 {
                return Err(ConfigError::new(
                    key,
                    "must be a loopback address with a port",
                ));
            }
            config.admin.listen_address = address;
        }
        "RATATOSKR__STORAGE__DATABASE_URL" => {
            value
                .parse::<sqlx::postgres::PgConnectOptions>()
                .map_err(|_| ConfigError::new(key, "must be a PostgreSQL connection URL"))?;
            value.clone_into(&mut config.storage.database_url);
        }
        "RATATOSKR__STORAGE__BLOB_ROOT" => {
            if value.is_empty() {
                return Err(ConfigError::new(key, "must be a non-empty path"));
            }
            config.storage.blob_root = PathBuf::from(value);
        }
        "RATATOSKR__LIMITS__DATABASE_CONNECTIONS" => {
            config.limits.database_connections = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__DATABASE_ACQUIRE_TIMEOUT_MS" => {
            config.limits.database_acquire_timeout_ms = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__PROVIDER_TIMEOUT_MS" => {
            config.limits.provider_timeout_ms = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__CONTEXT_CHARACTERS" => {
            config.limits.context_characters = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__RAW_RESPONSE_BYTES" => {
            config.limits.raw_response_bytes = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__SHUTDOWN_TIMEOUT_MS" => {
            config.limits.shutdown_timeout_ms = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__BLOB_BYTES" => {
            config.limits.blob_bytes = parse_positive(key, value)?;
        }
        _ => return Err(ConfigError::new(key, "is not recognized")),
    }
    Ok(())
}

fn parse_positive<T>(key: &str, value: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr + Default + PartialOrd,
{
    let parsed = value
        .parse::<T>()
        .map_err(|_| ConfigError::new(key, "must be a positive integer"))?;
    if parsed <= T::default() {
        return Err(ConfigError::new(key, "must be a positive integer"));
    }
    Ok(parsed)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            admin: AdminConfig {
                listen_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9081),
            },
            storage: StorageConfig {
                database_url: "postgres://knowledge:knowledge@127.0.0.1:5432/knowledge".to_owned(),
                blob_root: PathBuf::from("data/blobs"),
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
