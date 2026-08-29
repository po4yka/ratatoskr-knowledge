use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::{error, fmt};

use serde::Serialize;

const ENV_PREFIX: &str = "RATATOSKR__";

/// Vector dimensionality every stored embedding must carry.
///
/// Mirrors the fixed vector column typmod in `schema.sql` (`embedding
/// vector(1536)`): a similarity index requires a fixed dimensionality, so the
/// loader rejects any configured embeddings model dimension other than this.
pub(crate) const EMBEDDING_STORAGE_DIMENSIONS: i32 = 1536;

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
    /// Real provider configuration; absent keeps the process offline.
    pub provider: ProviderConfig,
    /// Optional channel-digest recap worker and exact transport topology.
    pub channel_recap: ChannelRecapConfig,
}

/// Provider selection for the recap worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelRecapProviderMode {
    /// Deterministic offline provider for fixtures and composed acceptance.
    Scripted,
    /// Configured controlled `OpenRouter` provider.
    OpenRouter,
}

/// Exact dormant channel-recap consumer and digest-source configuration.
#[derive(Debug, Clone, Serialize)]
pub struct ChannelRecapConfig {
    /// Whether the recap consumer is part of this process.
    pub enabled: bool,
    /// Provider mode; scripted mode needs no inference credential.
    pub provider_mode: ChannelRecapProviderMode,
    /// Loopback digest-source origin.
    pub digest_source_base_url: String,
    /// Service-to-service digest-source credential.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest_source_service_secret: Option<crate::DigestSourceSecret>,
    /// NATS or TLS NATS endpoint.
    pub bus_endpoint: String,
    /// Canonical pre-provisioned command stream.
    pub bus_stream: String,
    /// Canonical pre-provisioned durable name.
    pub bus_durable: String,
    /// Exact recap command subject.
    pub bus_subject: String,
    /// Optional absolute `NKey` seed file; loopback scripted profiles may use anonymous NATS.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bus_credentials_file: Option<PathBuf>,
    /// Maximum messages pulled per batch.
    pub fetch_batch: u32,
    /// Exact durable acknowledgement deadline.
    pub ack_wait_seconds: u64,
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
    /// Output token bound sent to the real provider.
    pub provider_max_output_tokens: u32,
    /// Minimum spacing between real provider requests, per minute.
    pub provider_requests_per_minute: u32,
    /// Daily input-plus-output token ceiling for real provider calls.
    pub provider_daily_token_budget: u64,
    /// Monthly input-plus-output token ceiling for real provider calls.
    pub provider_monthly_token_budget: u64,
    /// Daily estimated-cost ceiling in micro-US dollars.
    pub provider_daily_cost_micro_usd: u64,
    /// Monthly estimated-cost ceiling in micro-US dollars.
    pub provider_monthly_cost_micro_usd: u64,
    /// Maximum duration of one embeddings call.
    pub embeddings_timeout_ms: u64,
    /// Maximum Unicode characters in one embeddings request input.
    pub embeddings_max_input_characters: usize,
    /// Maximum sources processed in one indexing pass.
    pub embeddings_batch_sources: u32,
    /// Sleep between quiet indexing passes.
    pub embeddings_poll_interval_ms: u64,
    /// Minimum spacing between embeddings requests, per minute.
    pub embeddings_requests_per_minute: u32,
    /// Failed indexing attempts recorded before a source stops being retried.
    pub embeddings_max_failure_attempts: u32,
    /// Daily token ceiling for embeddings calls.
    pub embeddings_daily_token_budget: u64,
    /// Monthly token ceiling for embeddings calls.
    pub embeddings_monthly_token_budget: u64,
    /// Daily estimated-cost ceiling for embeddings calls in micro-US dollars.
    pub embeddings_daily_cost_micro_usd: u64,
    /// Monthly estimated-cost ceiling for embeddings calls in micro-US dollars.
    pub embeddings_monthly_cost_micro_usd: u64,
    /// Target Unicode characters per stored chunk window.
    pub chunk_target_characters: usize,
    /// Overlap Unicode characters carried between consecutive chunk windows.
    pub chunk_overlap_characters: usize,
}

#[derive(Debug, Clone, Serialize)]
/// Real provider configuration; absent means the process stays offline.
pub struct ProviderConfig {
    /// `OpenRouter` chat-completions adapter settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openrouter: Option<OpenRouterProviderConfig>,
    /// OpenAI-compatible embeddings adapter settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embeddings: Option<EmbeddingsProviderConfig>,
}

#[derive(Debug, Clone, Serialize)]
/// One OpenAI-compatible embeddings adapter's environment-derived settings.
pub struct EmbeddingsProviderConfig {
    /// Credential that redacts itself everywhere but the authorization header.
    pub api_key: ProviderSecret,
    /// Concrete upstream embeddings model id.
    pub model: String,
    /// Embeddings root URL; HTTPS or loopback plain text only.
    pub base_url: String,
    /// Vector dimensionality; must equal [`EMBEDDING_STORAGE_DIMENSIONS`].
    pub dimensions: i32,
    /// Opaque reviewed label recorded with every embedding row.
    pub prompt_version: String,
    /// Input-token price in micro-US dollars per million tokens.
    pub input_micro_usd_per_mtoken: u64,
}

#[derive(Debug, Clone, Serialize)]
/// One `OpenRouter` adapter's environment-derived settings.
pub struct OpenRouterProviderConfig {
    /// Credential that redacts itself everywhere but the authorization header.
    pub api_key: ProviderSecret,
    /// Concrete upstream model id.
    pub model: String,
    /// Chat-completions root URL; HTTPS or loopback plain text only.
    pub base_url: String,
    /// Input-token price in micro-US dollars per million tokens.
    pub input_micro_usd_per_mtoken: u64,
    /// Output-token price in micro-US dollars per million tokens.
    pub output_micro_usd_per_mtoken: u64,
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
        let mut draft = ProviderDraft::default();
        let mut embeddings_draft = EmbeddingsProviderDraft::default();
        for (key, value) in entries {
            let key = key.as_ref();
            if !key.starts_with(ENV_PREFIX) {
                continue;
            }
            apply_entry(
                &mut config,
                &mut draft,
                &mut embeddings_draft,
                key,
                value.as_ref(),
            )?;
        }
        config.provider.openrouter = draft.finish()?;
        config.provider.embeddings = embeddings_draft.finish()?;
        validate_channel_recap(&config)?;
        if config.limits.chunk_overlap_characters >= config.limits.chunk_target_characters {
            return Err(ConfigError::new(
                "RATATOSKR__LIMITS__CHUNK_OVERLAP_CHARACTERS",
                "must be smaller than the chunk target characters",
            ));
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

fn apply_entry(
    config: &mut Config,
    draft: &mut ProviderDraft,
    embeddings_draft: &mut EmbeddingsProviderDraft,
    key: &str,
    value: &str,
) -> Result<(), ConfigError> {
    if key.starts_with("RATATOSKR__LIMITS__") {
        return apply_limits_entry(&mut config.limits, key, value);
    }
    if key.starts_with("RATATOSKR__PROVIDER__EMBEDDINGS__") {
        return apply_embeddings_entry(embeddings_draft, key, value);
    }
    if key.starts_with("RATATOSKR__CHANNEL_RECAP__") {
        return apply_channel_recap_entry(&mut config.channel_recap, key, value);
    }
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
        "RATATOSKR__PROVIDER__OPENROUTER__API_KEY" => {
            if value.is_empty() {
                return Err(ConfigError::new(key, "must be a non-empty credential"));
            }
            draft.api_key = Some(value.to_owned());
        }
        "RATATOSKR__PROVIDER__OPENROUTER__MODEL" => {
            validate_model_id(key, value)?;
            draft.model = Some(value.to_owned());
        }
        "RATATOSKR__PROVIDER__OPENROUTER__BASE_URL" => {
            draft.base_url = Some(value.to_owned());
        }
        "RATATOSKR__PROVIDER__OPENROUTER__INPUT_MICRO_USD_PER_MTOKEN" => {
            draft.input_price = Some(parse_nonnegative(key, value)?);
        }
        "RATATOSKR__PROVIDER__OPENROUTER__OUTPUT_MICRO_USD_PER_MTOKEN" => {
            draft.output_price = Some(parse_nonnegative(key, value)?);
        }
        _ => return Err(ConfigError::new(key, "is not recognized")),
    }
    Ok(())
}

fn apply_channel_recap_entry(
    recap: &mut ChannelRecapConfig,
    key: &str,
    value: &str,
) -> Result<(), ConfigError> {
    match key {
        "RATATOSKR__CHANNEL_RECAP__ENABLED" => {
            recap.enabled = value
                .parse::<bool>()
                .map_err(|_| ConfigError::new(key, "must be true or false"))?;
        }
        "RATATOSKR__CHANNEL_RECAP__PROVIDER_MODE" => {
            recap.provider_mode = match value {
                "scripted" => ChannelRecapProviderMode::Scripted,
                "openrouter" => ChannelRecapProviderMode::OpenRouter,
                _ => return Err(ConfigError::new(key, "must be scripted or openrouter")),
            };
        }
        "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_BASE_URL" => {
            value.clone_into(&mut recap.digest_source_base_url);
        }
        "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_SERVICE_SECRET" => {
            if value.is_empty() {
                return Err(ConfigError::new(key, "must be a non-empty credential"));
            }
            recap.digest_source_service_secret =
                Some(crate::DigestSourceSecret::new(value.to_owned()));
        }
        "RATATOSKR__CHANNEL_RECAP__BUS_ENDPOINT" => value.clone_into(&mut recap.bus_endpoint),
        "RATATOSKR__CHANNEL_RECAP__BUS_STREAM" => value.clone_into(&mut recap.bus_stream),
        "RATATOSKR__CHANNEL_RECAP__BUS_DURABLE" => value.clone_into(&mut recap.bus_durable),
        "RATATOSKR__CHANNEL_RECAP__BUS_SUBJECT" => value.clone_into(&mut recap.bus_subject),
        "RATATOSKR__CHANNEL_RECAP__BUS_CREDENTIALS_FILE" => {
            recap.bus_credentials_file = Some(PathBuf::from(value));
        }
        "RATATOSKR__CHANNEL_RECAP__FETCH_BATCH" => {
            recap.fetch_batch = parse_positive(key, value)?;
        }
        "RATATOSKR__CHANNEL_RECAP__ACK_WAIT_SECONDS" => {
            recap.ack_wait_seconds = parse_positive(key, value)?;
        }
        _ => return Err(ConfigError::new(key, "is not recognized")),
    }
    Ok(())
}

fn validate_channel_recap(config: &Config) -> Result<(), ConfigError> {
    let recap = &config.channel_recap;
    if !recap.enabled {
        return Ok(());
    }
    if recap.digest_source_service_secret.is_none() {
        return Err(ConfigError::new(
            "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_SERVICE_SECRET",
            "is required when channel recap is enabled",
        ));
    }
    let source = reqwest::Url::parse(&recap.digest_source_base_url).map_err(|_| {
        ConfigError::new(
            "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_BASE_URL",
            "must be a loopback HTTP URL",
        )
    })?;
    if source.scheme() != "http"
        || !url_is_loopback(&source)
        || !source.username().is_empty()
        || source.password().is_some()
        || source.query().is_some()
        || source.fragment().is_some()
    {
        return Err(ConfigError::new(
            "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_BASE_URL",
            "must be a loopback HTTP URL",
        ));
    }
    let bus = reqwest::Url::parse(&recap.bus_endpoint).map_err(|_| {
        ConfigError::new(
            "RATATOSKR__CHANNEL_RECAP__BUS_ENDPOINT",
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
            "RATATOSKR__CHANNEL_RECAP__BUS_ENDPOINT",
            "must use TLS or loopback NATS",
        ));
    }
    if let Some(path) = recap.bus_credentials_file.as_ref()
        && (!path.is_absolute() || !path.is_file())
    {
        return Err(ConfigError::new(
            "RATATOSKR__CHANNEL_RECAP__BUS_CREDENTIALS_FILE",
            "must be a readable absolute file",
        ));
    }
    if bus.scheme() == "tls" && recap.bus_credentials_file.is_none() {
        return Err(ConfigError::new(
            "RATATOSKR__CHANNEL_RECAP__BUS_CREDENTIALS_FILE",
            "must be a readable absolute file for remote TLS NATS",
        ));
    }
    for (actual, expected, key) in [
        (
            recap.bus_stream.as_str(),
            "ratatoskr_commands",
            "RATATOSKR__CHANNEL_RECAP__BUS_STREAM",
        ),
        (
            recap.bus_durable.as_str(),
            "ratatoskr_knowledge_channel_recap",
            "RATATOSKR__CHANNEL_RECAP__BUS_DURABLE",
        ),
        (
            recap.bus_subject.as_str(),
            "cmd.knowledge.channel_digest_recap.requested.v1",
            "RATATOSKR__CHANNEL_RECAP__BUS_SUBJECT",
        ),
    ] {
        if actual != expected {
            return Err(ConfigError::new(
                key,
                "must equal the canonical fleet value",
            ));
        }
    }
    if !(1..=256).contains(&recap.fetch_batch) || !(1..=600).contains(&recap.ack_wait_seconds) {
        return Err(ConfigError::new(
            "RATATOSKR__CHANNEL_RECAP__FETCH_BATCH",
            "fetch and acknowledgement limits are invalid",
        ));
    }
    if recap.provider_mode == ChannelRecapProviderMode::OpenRouter
        && config.provider.openrouter.is_none()
    {
        return Err(ConfigError::new(
            "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
            "is required for openrouter recap mode",
        ));
    }
    Ok(())
}

fn url_is_loopback(url: &reqwest::Url) -> bool {
    url.host_str()
        .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1"))
}

fn apply_limits_entry(limits: &mut Limits, key: &str, value: &str) -> Result<(), ConfigError> {
    match key {
        "RATATOSKR__LIMITS__DATABASE_CONNECTIONS" => {
            limits.database_connections = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__DATABASE_ACQUIRE_TIMEOUT_MS" => {
            limits.database_acquire_timeout_ms = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__PROVIDER_TIMEOUT_MS" => {
            limits.provider_timeout_ms = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__CONTEXT_CHARACTERS" => {
            limits.context_characters = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__RAW_RESPONSE_BYTES" => {
            limits.raw_response_bytes = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__SHUTDOWN_TIMEOUT_MS" => {
            limits.shutdown_timeout_ms = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__BLOB_BYTES" => {
            limits.blob_bytes = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__PROVIDER_MAX_OUTPUT_TOKENS" => {
            limits.provider_max_output_tokens = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__PROVIDER_REQUESTS_PER_MINUTE" => {
            limits.provider_requests_per_minute = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__PROVIDER_DAILY_TOKEN_BUDGET" => {
            limits.provider_daily_token_budget = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__PROVIDER_MONTHLY_TOKEN_BUDGET" => {
            limits.provider_monthly_token_budget = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__PROVIDER_DAILY_COST_MICRO_USD" => {
            limits.provider_daily_cost_micro_usd = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__PROVIDER_MONTHLY_COST_MICRO_USD" => {
            limits.provider_monthly_cost_micro_usd = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_TIMEOUT_MS" => {
            limits.embeddings_timeout_ms = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_MAX_INPUT_CHARACTERS" => {
            limits.embeddings_max_input_characters = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_BATCH_SOURCES" => {
            limits.embeddings_batch_sources = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_POLL_INTERVAL_MS" => {
            limits.embeddings_poll_interval_ms = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_REQUESTS_PER_MINUTE" => {
            limits.embeddings_requests_per_minute = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_MAX_FAILURE_ATTEMPTS" => {
            limits.embeddings_max_failure_attempts = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_DAILY_TOKEN_BUDGET" => {
            limits.embeddings_daily_token_budget = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_MONTHLY_TOKEN_BUDGET" => {
            limits.embeddings_monthly_token_budget = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_DAILY_COST_MICRO_USD" => {
            limits.embeddings_daily_cost_micro_usd = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__EMBEDDINGS_MONTHLY_COST_MICRO_USD" => {
            limits.embeddings_monthly_cost_micro_usd = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__CHUNK_TARGET_CHARACTERS" => {
            limits.chunk_target_characters = parse_positive(key, value)?;
        }
        "RATATOSKR__LIMITS__CHUNK_OVERLAP_CHARACTERS" => {
            limits.chunk_overlap_characters = parse_positive(key, value)?;
        }
        _ => return Err(ConfigError::new(key, "is not recognized")),
    }
    Ok(())
}

fn apply_embeddings_entry(
    draft: &mut EmbeddingsProviderDraft,
    key: &str,
    value: &str,
) -> Result<(), ConfigError> {
    match key {
        "RATATOSKR__PROVIDER__EMBEDDINGS__API_KEY" => {
            if value.is_empty() {
                return Err(ConfigError::new(key, "must be a non-empty credential"));
            }
            draft.api_key = Some(value.to_owned());
        }
        "RATATOSKR__PROVIDER__EMBEDDINGS__MODEL" => {
            validate_model_id(key, value)?;
            draft.model = Some(value.to_owned());
        }
        "RATATOSKR__PROVIDER__EMBEDDINGS__BASE_URL" => {
            draft.base_url = Some(value.to_owned());
        }
        "RATATOSKR__PROVIDER__EMBEDDINGS__DIMENSIONS" => {
            draft.dimensions = Some(parse_positive(key, value)?);
        }
        "RATATOSKR__PROVIDER__EMBEDDINGS__PROMPT_VERSION" => {
            if value.is_empty() {
                return Err(ConfigError::new(key, "must be a non-empty label"));
            }
            draft.prompt_version = Some(value.to_owned());
        }
        "RATATOSKR__PROVIDER__EMBEDDINGS__INPUT_MICRO_USD_PER_MTOKEN" => {
            draft.input_price = Some(parse_nonnegative(key, value)?);
        }
        _ => return Err(ConfigError::new(key, "is not recognized")),
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ProviderDraft {
    api_key: Option<String>,
    model: Option<String>,
    base_url: Option<String>,
    input_price: Option<u64>,
    output_price: Option<u64>,
}

impl ProviderDraft {
    fn finish(self) -> Result<Option<OpenRouterProviderConfig>, ConfigError> {
        let Some(api_key) = self.api_key else {
            return Ok(None);
        };
        let Some(model) = self.model else {
            return Err(ConfigError::new(
                "RATATOSKR__PROVIDER__OPENROUTER__MODEL",
                "is required when an API key is configured",
            ));
        };
        let base_url = self
            .base_url
            .unwrap_or_else(|| "https://openrouter.ai/api/v1".to_owned());
        validate_base_url("RATATOSKR__PROVIDER__OPENROUTER__BASE_URL", &base_url)?;
        Ok(Some(OpenRouterProviderConfig {
            api_key: ProviderSecret::new(api_key),
            model,
            base_url,
            input_micro_usd_per_mtoken: self.input_price.unwrap_or_default(),
            output_micro_usd_per_mtoken: self.output_price.unwrap_or_default(),
        }))
    }
}

#[derive(Debug, Default)]
struct EmbeddingsProviderDraft {
    api_key: Option<String>,
    model: Option<String>,
    base_url: Option<String>,
    dimensions: Option<i32>,
    prompt_version: Option<String>,
    input_price: Option<u64>,
}

impl EmbeddingsProviderDraft {
    fn finish(self) -> Result<Option<EmbeddingsProviderConfig>, ConfigError> {
        const DIMENSIONS_KEY: &str = "RATATOSKR__PROVIDER__EMBEDDINGS__DIMENSIONS";
        let dimensions = self.dimensions.unwrap_or(EMBEDDING_STORAGE_DIMENSIONS);
        if dimensions != EMBEDDING_STORAGE_DIMENSIONS {
            return Err(ConfigError::new(
                DIMENSIONS_KEY,
                "must equal the storage dimensionality",
            ));
        }
        let Some(api_key) = self.api_key else {
            return Ok(None);
        };
        let Some(model) = self.model else {
            return Err(ConfigError::new(
                "RATATOSKR__PROVIDER__EMBEDDINGS__MODEL",
                "is required when an API key is configured",
            ));
        };
        let base_url = self
            .base_url
            .unwrap_or_else(|| "https://api.openai.com/v1".to_owned());
        validate_base_url("RATATOSKR__PROVIDER__EMBEDDINGS__BASE_URL", &base_url)?;
        let prompt_version = self.prompt_version.unwrap_or_else(|| "none.v1".to_owned());
        Ok(Some(EmbeddingsProviderConfig {
            api_key: ProviderSecret::new(api_key),
            model,
            base_url,
            dimensions,
            prompt_version,
            input_micro_usd_per_mtoken: self.input_price.unwrap_or_default(),
        }))
    }
}

fn validate_base_url(key: &str, base_url: &str) -> Result<(), ConfigError> {
    let parsed =
        reqwest::Url::parse(base_url).map_err(|_| ConfigError::new(key, "must be a valid URL"))?;
    let loopback_host = parsed
        .host_str()
        .is_some_and(|host| host == "localhost" || host == "127.0.0.1" || host == "[::1]");
    if parsed.scheme() == "https" || (parsed.scheme() == "http" && loopback_host) {
        Ok(())
    } else {
        Err(ConfigError::new(
            key,
            "must use HTTPS or a loopback plain-text address",
        ))
    }
}

fn validate_model_id(key: &str, value: &str) -> Result<(), ConfigError> {
    let printable = !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| (33..=126).contains(&byte));
    if printable {
        Ok(())
    } else {
        Err(ConfigError::new(
            key,
            "must be a bounded printable model id",
        ))
    }
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

fn parse_nonnegative(key: &str, value: &str) -> Result<u64, ConfigError> {
    value
        .parse::<u64>()
        .map_err(|_| ConfigError::new(key, "must be a non-negative integer"))
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
                provider_max_output_tokens: 2_048,
                provider_requests_per_minute: 60,
                provider_daily_token_budget: 2_000_000,
                provider_monthly_token_budget: 20_000_000,
                provider_daily_cost_micro_usd: 5_000_000,
                provider_monthly_cost_micro_usd: 50_000_000,
                embeddings_timeout_ms: 30_000,
                embeddings_max_input_characters: 120_000,
                embeddings_batch_sources: 8,
                embeddings_poll_interval_ms: 5_000,
                embeddings_requests_per_minute: 60,
                embeddings_max_failure_attempts: 5,
                embeddings_daily_token_budget: 2_000_000,
                embeddings_monthly_token_budget: 20_000_000,
                embeddings_daily_cost_micro_usd: 5_000_000,
                embeddings_monthly_cost_micro_usd: 50_000_000,
                chunk_target_characters: 1_600,
                chunk_overlap_characters: 200,
            },
            provider: ProviderConfig {
                openrouter: None,
                embeddings: None,
            },
            channel_recap: ChannelRecapConfig {
                enabled: false,
                provider_mode: ChannelRecapProviderMode::Scripted,
                digest_source_base_url: "http://127.0.0.1:8098/".to_owned(),
                digest_source_service_secret: None,
                bus_endpoint: "nats://127.0.0.1:4222".to_owned(),
                bus_stream: "ratatoskr_commands".to_owned(),
                bus_durable: "ratatoskr_knowledge_channel_recap".to_owned(),
                bus_subject: "cmd.knowledge.channel_digest_recap.requested.v1".to_owned(),
                bus_credentials_file: None,
                fetch_batch: 32,
                ack_wait_seconds: 30,
            },
        }
    }
}
