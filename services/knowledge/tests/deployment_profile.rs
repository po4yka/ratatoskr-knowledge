//! XR-021 CONTRACTS.md R2-10: the shipped operator example is loadable and enables the flows that
//! are not enabled by default, so it cannot rot unnoticed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ratatoskr_knowledge::{ChannelRecapProviderMode, Config, IngestProviderMode};

const EXAMPLE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/systemd/knowledge.conf.example"
);

/// Ports `docs/DEPLOYMENT_TARGET.md` allocates to Knowledge and to the services it talks to:
/// 8091 Knowledge domain API, 9081 Knowledge operator listener, 8092 GitHub README source, 8098
/// channel-digests API, 4222 NATS, 5432 `PostgreSQL`.
const ALLOCATED_PORTS: [u16; 6] = [8091, 9081, 8092, 8098, 4222, 5432];

/// The seed path the example installs for both bus identities.
const SEED_PATH: &str = "/etc/ratatoskr/knowledge.nkey";

/// Parses a systemd `EnvironmentFile`: `KEY=VALUE` lines, `#` comments, blank lines.
fn parse_environment_file(text: &str) -> Result<BTreeMap<String, String>, String> {
    let mut entries = BTreeMap::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("line {} is not KEY=VALUE", number + 1))?;
        if entries
            .insert(key.trim().to_owned(), value.trim().to_owned())
            .is_some()
        {
            return Err(format!("{key} is set twice"));
        }
    }
    Ok(entries)
}

fn port_of(address_or_url: &str) -> Option<u16> {
    let authority = address_or_url
        .split_once("://")
        .map_or(address_or_url, |(_, rest)| rest);
    let host_port = authority.split('/').next()?;
    host_port.rsplit_once(':')?.1.parse().ok()
}

fn temporary_seed() -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let directory = std::env::temp_dir().join(format!(
        "knowledge-deployment-profile-{}",
        uuid::Uuid::now_v7().simple()
    ));
    std::fs::create_dir_all(&directory)?;
    let seed = directory.join("knowledge.nkey");
    std::fs::write(&seed, "SU-test-seed-placeholder")?;
    Ok((directory, seed))
}

fn load(
    entries: &BTreeMap<String, String>,
    seed: &Path,
) -> Result<Config, Box<dyn std::error::Error>> {
    let substituted: Vec<(String, String)> = entries
        .iter()
        .map(|(key, value)| {
            let value = if value == SEED_PATH {
                seed.display().to_string()
            } else {
                value.clone()
            };
            (key.clone(), value)
        })
        .collect();
    Ok(Config::from_environment(substituted)?)
}

#[test]
fn the_shipped_example_enables_ingest_recap_and_both_listeners()
-> Result<(), Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(EXAMPLE)?;
    let entries = parse_environment_file(&text)?;
    let (directory, seed) = temporary_seed()?;
    let config = load(&entries, &seed);
    let _ignored = std::fs::remove_dir_all(directory);
    let config = config?;

    assert!(config.ingest.enabled, "ingest is dormant unless enabled");
    assert_eq!(config.ingest.provider_mode, IngestProviderMode::OpenRouter);
    assert_eq!(config.ingest.bus_endpoint, "nats://127.0.0.1:4222");
    assert!(config.ingest.bus_credentials_file.is_some());
    assert_eq!(
        config.ingest.github_readme_base_url,
        "http://127.0.0.1:8092/"
    );
    assert!(config.ingest.github_readme_service_secret.is_some());

    assert!(config.channel_recap.enabled);
    assert_eq!(
        config.channel_recap.provider_mode,
        ChannelRecapProviderMode::OpenRouter
    );
    assert_eq!(config.channel_recap.bus_endpoint, "nats://127.0.0.1:4222");
    assert!(config.channel_recap.bus_credentials_file.is_some());
    assert_eq!(
        config.channel_recap.digest_source_base_url,
        "http://127.0.0.1:8098/"
    );
    assert!(config.channel_recap.digest_source_service_secret.is_some());
    assert!(config.channel_recap.result_reader_service_secret.is_some());
    assert!(config.provider.openrouter.is_some());

    assert_eq!(config.api.listen_address.to_string(), "127.0.0.1:8091");
    assert_eq!(config.admin.listen_address.to_string(), "127.0.0.1:9081");

    for key in [
        "RATATOSKR__API__LISTEN_ADDRESS",
        "RATATOSKR__ADMIN__LISTEN_ADDRESS",
    ] {
        assert!(entries.contains_key(key), "the example states {key}");
    }
    Ok(())
}

#[test]
fn every_bind_and_peer_in_the_example_is_an_allocated_port() -> Result<(), String> {
    let text = std::fs::read_to_string(EXAMPLE).map_err(|error| error.to_string())?;
    let entries = parse_environment_file(&text)?;
    let addressed: Vec<(&String, u16)> = entries
        .iter()
        .filter(|(key, _)| {
            key.ends_with("LISTEN_ADDRESS")
                || key.ends_with("__BASE_URL")
                || key.ends_with("BUS_ENDPOINT")
                || key.ends_with("DATABASE_URL")
        })
        .filter(|(key, _)| !key.starts_with("RATATOSKR__PROVIDER__"))
        .filter_map(|(key, value)| port_of(value).map(|port| (key, port)))
        .collect();
    assert!(!addressed.is_empty(), "the example binds and names peers");
    for (key, port) in addressed {
        assert!(
            ALLOCATED_PORTS.contains(&port),
            "{key} uses port {port}, which docs/DEPLOYMENT_TARGET.md does not allocate to Knowledge"
        );
    }
    Ok(())
}

#[test]
fn secrets_in_the_example_are_placeholders() -> Result<(), String> {
    let text = std::fs::read_to_string(EXAMPLE).map_err(|error| error.to_string())?;
    let entries = parse_environment_file(&text)?;
    for (key, value) in &entries {
        if key.ends_with("SECRET") || key.ends_with("API_KEY") {
            assert_eq!(value, "CHANGE-ME", "{key} must be a placeholder");
        }
    }
    Ok(())
}
