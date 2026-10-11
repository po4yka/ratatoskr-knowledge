//! Channel-recap configuration entries and validation (`RATATOSKR__CHANNEL_RECAP__*`).

use std::path::PathBuf;

use crate::Config;
use crate::config::{
    CHANNEL_RECAP_ACK_WAIT_SECONDS, ChannelRecapConfig, ChannelRecapProviderMode, ConfigError,
    parse_positive, url_is_loopback,
};
use crate::result_reader_secret::ResultReaderSecret;

pub(crate) fn apply_channel_recap_entry(
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
        "RATATOSKR__CHANNEL_RECAP__RESULT_READER_SERVICE_SECRET" => {
            if value.is_empty() || value.len() > 4_096 {
                return Err(ConfigError::new(
                    key,
                    "must be a non-empty credential of at most 4096 bytes",
                ));
            }
            recap.result_reader_service_secret = Some(ResultReaderSecret::new(value.to_owned()));
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
            recap.ack_wait_seconds = value
                .parse::<u64>()
                .ok()
                .filter(|seconds| *seconds == CHANNEL_RECAP_ACK_WAIT_SECONDS)
                .ok_or_else(|| {
                    ConfigError::new(
                        key,
                        "must be 30, the ack wait Edge fixes on the channel recap durable (XR-021 CONTRACTS.md S04)",
                    )
                })?;
        }
        _ => return Err(ConfigError::new(key, "is not recognized")),
    }
    Ok(())
}

pub(crate) fn validate_channel_recap(config: &Config) -> Result<(), ConfigError> {
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
    validate_digest_source(recap)?;
    let bus = validate_bus(recap)?;
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
    if !(1..=256).contains(&recap.fetch_batch) {
        return Err(ConfigError::new(
            "RATATOSKR__CHANNEL_RECAP__FETCH_BATCH",
            "must be between 1 and 256",
        ));
    }
    validate_provider_mode(config, &bus)
}

fn validate_digest_source(recap: &ChannelRecapConfig) -> Result<(), ConfigError> {
    let rule = "must be a loopback HTTP URL";
    let key = "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_BASE_URL";
    let source = reqwest::Url::parse(&recap.digest_source_base_url)
        .map_err(|_| ConfigError::new(key, rule))?;
    if source.scheme() != "http"
        || !url_is_loopback(&source)
        || !source.username().is_empty()
        || source.password().is_some()
        || source.query().is_some()
        || source.fragment().is_some()
    {
        return Err(ConfigError::new(key, rule));
    }
    Ok(())
}

fn validate_bus(recap: &ChannelRecapConfig) -> Result<reqwest::Url, ConfigError> {
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
    Ok(bus)
}

/// `openrouter` needs its provider key; `scripted` answers every recap with fixed text, so it is
/// accepted only on an anonymous loopback broker (XR-021 CONTRACTS.md R2-13).
fn validate_provider_mode(config: &Config, bus: &reqwest::Url) -> Result<(), ConfigError> {
    let recap = &config.channel_recap;
    match recap.provider_mode {
        ChannelRecapProviderMode::OpenRouter if config.provider.openrouter.is_none() => {
            Err(ConfigError::new(
                "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
                "is required for openrouter recap mode",
            ))
        }
        ChannelRecapProviderMode::Scripted
            if bus.scheme() != "nats" || recap.bus_credentials_file.is_some() =>
        {
            Err(ConfigError::new(
                "RATATOSKR__CHANNEL_RECAP__PROVIDER_MODE",
                "scripted mode requires an anonymous loopback nats endpoint",
            ))
        }
        ChannelRecapProviderMode::Scripted | ChannelRecapProviderMode::OpenRouter => Ok(()),
    }
}
