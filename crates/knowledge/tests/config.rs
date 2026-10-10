//! Configuration boundary tests.

use ratatoskr_knowledge::Config;

#[test]
fn defaults_are_finite_and_security_cannot_be_disabled() -> Result<(), serde_json::Error> {
    let config = Config::default();

    assert!(config.admin.listen_address.ip().is_loopback());
    assert_ne!(config.admin.listen_address.port(), 0);
    assert!(config.limits.database_connections > 0);
    assert!(config.limits.database_acquire_timeout_ms > 0);
    assert!(config.limits.provider_timeout_ms > 0);
    assert!(config.limits.context_characters > 0);
    assert!(config.limits.raw_response_bytes > 0);
    assert!(config.limits.shutdown_timeout_ms > 0);
    assert!(config.limits.blob_bytes > 0);

    let encoded = serde_json::to_string(&config)?;
    assert!(!encoded.contains("disable"));
    Ok(())
}

#[test]
fn invalid_environment_is_reported_without_its_value() {
    let unknown = Config::from_environment([("RATATOSKR__LIMITS__MYSTERY", "LEAKME")]);
    let wrong = Config::from_environment([("RATATOSKR__LIMITS__PROVIDER_TIMEOUT_MS", "LEAKME")]);

    let unknown_diagnostic = unknown.expect_err("unknown key must fail").to_string();
    let wrong_diagnostic = wrong.expect_err("invalid value must fail").to_string();

    assert!(unknown_diagnostic.contains("RATATOSKR__LIMITS__MYSTERY"));
    assert!(wrong_diagnostic.contains("RATATOSKR__LIMITS__PROVIDER_TIMEOUT_MS"));
    assert!(!unknown_diagnostic.contains("LEAKME"));
    assert!(!wrong_diagnostic.contains("LEAKME"));
}

#[test]
fn provider_keys_are_finite_strict_and_secret() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::default();

    assert!(config.limits.provider_max_output_tokens > 0);
    assert!(config.limits.provider_requests_per_minute > 0);
    assert!(config.limits.provider_daily_token_budget > 0);
    assert!(config.limits.provider_monthly_token_budget > 0);
    assert!(config.limits.provider_daily_cost_micro_usd > 0);
    assert!(config.limits.provider_monthly_cost_micro_usd > 0);

    let encoded = serde_json::to_string(&config)?;
    let diagnostic = format!("{config:?}");
    assert!(!encoded.contains("sk-or-v1-LEAKME"));
    assert!(!diagnostic.contains("sk-or-v1-LEAKME"));

    let configured = Config::from_environment([
        (
            "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
            "sk-or-v1-LEAKME",
        ),
        (
            "RATATOSKR__PROVIDER__OPENROUTER__MODEL",
            "openai/gpt-oss-20b",
        ),
    ])?;
    let openrouter = configured
        .provider
        .openrouter
        .as_ref()
        .ok_or("provider must be configured")?;
    assert_eq!(openrouter.model, "openai/gpt-oss-20b");
    assert_eq!(openrouter.api_key.expose_secret(), "sk-or-v1-LEAKME");
    let encoded = serde_json::to_string(&configured)?;
    assert!(!encoded.contains("sk-or-v1-LEAKME"));

    let missing_model = Config::from_environment([(
        "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
        "sk-or-v1-LEAKME",
    )]);
    let missing_diagnostic = missing_model
        .expect_err("model must be required with a credential")
        .to_string();
    assert!(missing_diagnostic.contains("RATATOSKR__PROVIDER__OPENROUTER__MODEL"));

    let plain_text = Config::from_environment([
        (
            "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
            "sk-or-v1-LEAKME",
        ),
        (
            "RATATOSKR__PROVIDER__OPENROUTER__MODEL",
            "openai/gpt-oss-20b",
        ),
        (
            "RATATOSKR__PROVIDER__OPENROUTER__BASE_URL",
            "http://inference.example.internal/v1",
        ),
    ]);
    let plain_diagnostic = plain_text
        .expect_err("plain-text remote base URL must fail")
        .to_string();
    assert!(
        plain_diagnostic.contains("RATATOSKR__PROVIDER__OPENROUTER__BASE_URL"),
        "the failing key must be named"
    );

    let unknown = Config::from_environment([("RATATOSKR__PROVIDER__MYSTERY", "LEAKME")]);
    let unknown_diagnostic = unknown.expect_err("unknown key must fail").to_string();
    assert!(unknown_diagnostic.contains("RATATOSKR__PROVIDER__MYSTERY"));
    assert!(!unknown_diagnostic.contains("LEAKME"));
    Ok(())
}

#[test]
fn channel_recap_result_reader_secret_is_redacted_and_bounded()
-> Result<(), Box<dyn std::error::Error>> {
    const KEY: &str = "RATATOSKR__CHANNEL_RECAP__RESULT_READER_SERVICE_SECRET";
    const SECRET: &str = "channel-result-reader-LEAKME";

    let defaults = Config::default();
    assert!(
        defaults
            .channel_recap
            .result_reader_service_secret
            .is_none()
    );
    assert!(!serde_json::to_string(&defaults)?.contains("RESULT_READER_SERVICE_SECRET"));

    let configured = Config::from_environment([(KEY, SECRET)])?;
    assert!(!configured.channel_recap.enabled);
    assert_eq!(
        configured
            .channel_recap
            .result_reader_service_secret
            .as_ref()
            .ok_or("result-reader secret must be configured")?
            .expose_secret(),
        SECRET
    );
    assert!(!serde_json::to_string(&configured)?.contains(SECRET));
    assert!(!format!("{configured:?}").contains(SECRET));

    for invalid in [String::new(), "x".repeat(4_097)] {
        let error = Config::from_environment([(KEY, invalid.as_str())])
            .expect_err("empty and oversized service secrets must fail")
            .to_string();
        assert!(error.contains(KEY));
        if !invalid.is_empty() {
            assert!(!error.contains(invalid.as_str()));
        }
    }
    Ok(())
}

#[test]
fn embeddings_configuration_parses_strictly() -> Result<(), Box<dyn std::error::Error>> {
    let defaults = Config::default();
    assert!(defaults.provider.embeddings.is_none());
    assert_eq!(defaults.limits.embeddings_timeout_ms, 30_000);
    assert_eq!(defaults.limits.embeddings_max_input_characters, 120_000);
    assert_eq!(defaults.limits.embeddings_batch_sources, 8);
    assert_eq!(defaults.limits.embeddings_poll_interval_ms, 5_000);
    assert_eq!(defaults.limits.embeddings_requests_per_minute, 60);
    assert_eq!(defaults.limits.embeddings_max_failure_attempts, 5);
    assert_eq!(defaults.limits.embeddings_daily_token_budget, 2_000_000);
    assert_eq!(defaults.limits.embeddings_monthly_token_budget, 20_000_000);
    assert_eq!(defaults.limits.embeddings_daily_cost_micro_usd, 5_000_000);
    assert_eq!(
        defaults.limits.embeddings_monthly_cost_micro_usd,
        50_000_000
    );
    assert_eq!(defaults.limits.chunk_target_characters, 1_600);
    assert_eq!(defaults.limits.chunk_overlap_characters, 200);

    let configured = Config::from_environment([
        (
            "RATATOSKR__PROVIDER__EMBEDDINGS__API_KEY",
            "sk-embed-LEAKME",
        ),
        (
            "RATATOSKR__PROVIDER__EMBEDDINGS__MODEL",
            "text-embedding-3-small",
        ),
        (
            "RATATOSKR__PROVIDER__EMBEDDINGS__BASE_URL",
            "http://127.0.0.1:8080/v1",
        ),
        ("RATATOSKR__PROVIDER__EMBEDDINGS__DIMENSIONS", "1536"),
        (
            "RATATOSKR__PROVIDER__EMBEDDINGS__PROMPT_VERSION",
            "prefix.v2",
        ),
        (
            "RATATOSKR__PROVIDER__EMBEDDINGS__INPUT_MICRO_USD_PER_MTOKEN",
            "17",
        ),
        ("RATATOSKR__LIMITS__EMBEDDINGS_TIMEOUT_MS", "45000"),
        (
            "RATATOSKR__LIMITS__EMBEDDINGS_MAX_INPUT_CHARACTERS",
            "90000",
        ),
        ("RATATOSKR__LIMITS__EMBEDDINGS_BATCH_SOURCES", "3"),
        ("RATATOSKR__LIMITS__EMBEDDINGS_POLL_INTERVAL_MS", "1500"),
        ("RATATOSKR__LIMITS__EMBEDDINGS_REQUESTS_PER_MINUTE", "30"),
        ("RATATOSKR__LIMITS__EMBEDDINGS_MAX_FAILURE_ATTEMPTS", "2"),
        ("RATATOSKR__LIMITS__EMBEDDINGS_DAILY_TOKEN_BUDGET", "111111"),
        (
            "RATATOSKR__LIMITS__EMBEDDINGS_MONTHLY_TOKEN_BUDGET",
            "222222",
        ),
        (
            "RATATOSKR__LIMITS__EMBEDDINGS_DAILY_COST_MICRO_USD",
            "333333",
        ),
        (
            "RATATOSKR__LIMITS__EMBEDDINGS_MONTHLY_COST_MICRO_USD",
            "444444",
        ),
        ("RATATOSKR__LIMITS__CHUNK_TARGET_CHARACTERS", "2400"),
        ("RATATOSKR__LIMITS__CHUNK_OVERLAP_CHARACTERS", "300"),
    ])?;
    let embeddings = configured
        .provider
        .embeddings
        .as_ref()
        .ok_or("embeddings provider must be configured")?;
    assert_eq!(embeddings.model, "text-embedding-3-small");
    assert_eq!(embeddings.api_key.expose_secret(), "sk-embed-LEAKME");
    assert_eq!(embeddings.base_url, "http://127.0.0.1:8080/v1");
    assert_eq!(embeddings.dimensions, 1536);
    assert_eq!(embeddings.prompt_version, "prefix.v2");
    assert_eq!(embeddings.input_micro_usd_per_mtoken, 17);
    assert_eq!(configured.limits.embeddings_timeout_ms, 45_000);
    assert_eq!(configured.limits.embeddings_max_input_characters, 90_000);
    assert_eq!(configured.limits.embeddings_batch_sources, 3);
    assert_eq!(configured.limits.embeddings_poll_interval_ms, 1_500);
    assert_eq!(configured.limits.embeddings_requests_per_minute, 30);
    assert_eq!(configured.limits.embeddings_max_failure_attempts, 2);
    assert_eq!(configured.limits.embeddings_daily_token_budget, 111_111);
    assert_eq!(configured.limits.embeddings_monthly_token_budget, 222_222);
    assert_eq!(configured.limits.embeddings_daily_cost_micro_usd, 333_333);
    assert_eq!(configured.limits.embeddings_monthly_cost_micro_usd, 444_444);
    assert_eq!(configured.limits.chunk_target_characters, 2_400);
    assert_eq!(configured.limits.chunk_overlap_characters, 300);

    let encoded = serde_json::to_string(&configured)?;
    let diagnostic = format!("{configured:?}");
    assert!(!encoded.contains("sk-embed-LEAKME"));
    assert!(!diagnostic.contains("sk-embed-LEAKME"));

    Ok(())
}

#[test]
fn embeddings_configuration_rejects_invalid_settings() {
    let encoded = serde_json::to_string(&Config::default()).unwrap();
    assert!(!encoded.contains("sk-embed-LEAKME"));

    let missing_model = Config::from_environment([(
        "RATATOSKR__PROVIDER__EMBEDDINGS__API_KEY",
        "sk-embed-LEAKME",
    )]);
    let missing_diagnostic = missing_model
        .expect_err("model must be required with a credential")
        .to_string();
    assert!(missing_diagnostic.contains("RATATOSKR__PROVIDER__EMBEDDINGS__MODEL"));
    assert!(!missing_diagnostic.contains("sk-embed-LEAKME"));

    for (key, value) in [
        ("RATATOSKR__PROVIDER__EMBEDDINGS__MYSTERY", "LEAKME"),
        ("RATATOSKR__LIMITS__EMBEDDINGS_MYSTERY", "LEAKME"),
    ] {
        let unknown = Config::from_environment([(key, value)]);
        let unknown_diagnostic = unknown.expect_err("unknown key must fail").to_string();
        assert!(unknown_diagnostic.contains(key));
        assert!(!unknown_diagnostic.contains("LEAKME"));
    }

    let plain_text = Config::from_environment([
        (
            "RATATOSKR__PROVIDER__EMBEDDINGS__API_KEY",
            "sk-embed-LEAKME",
        ),
        (
            "RATATOSKR__PROVIDER__EMBEDDINGS__MODEL",
            "text-embedding-3-small",
        ),
        (
            "RATATOSKR__PROVIDER__EMBEDDINGS__BASE_URL",
            "http://inference.example.internal/v1",
        ),
    ]);
    let plain_diagnostic = plain_text
        .expect_err("plain-text remote base URL must fail")
        .to_string();
    assert!(
        plain_diagnostic.contains("RATATOSKR__PROVIDER__EMBEDDINGS__BASE_URL"),
        "the failing key must be named"
    );

    let overlap_equal = Config::from_environment([
        ("RATATOSKR__LIMITS__CHUNK_TARGET_CHARACTERS", "300"),
        ("RATATOSKR__LIMITS__CHUNK_OVERLAP_CHARACTERS", "300"),
    ]);
    let overlap_diagnostic = overlap_equal
        .expect_err("overlap must stay below the chunk target")
        .to_string();
    assert!(overlap_diagnostic.contains("RATATOSKR__LIMITS__CHUNK_OVERLAP_CHARACTERS"));

    let wrong_dimensions =
        Config::from_environment([("RATATOSKR__PROVIDER__EMBEDDINGS__DIMENSIONS", "768")]);
    let dimensions_diagnostic = wrong_dimensions
        .expect_err("dimensions must equal the storage dimensionality")
        .to_string();
    assert!(dimensions_diagnostic.contains("RATATOSKR__PROVIDER__EMBEDDINGS__DIMENSIONS"));
}

/// XR-021 CONTRACTS.md S09: the ingest section is dormant by default and accepted when complete.
#[test]
fn ingest_section_defaults_and_accepts_a_complete_configuration()
-> Result<(), Box<dyn std::error::Error>> {
    let defaults = Config::default();
    assert!(!defaults.ingest.enabled);
    assert_eq!(defaults.ingest.fetch_batch, 1);
    assert_eq!(defaults.ingest.ack_wait_seconds, 120);

    let accepted = Config::from_environment([
        ("RATATOSKR__INGEST__ENABLED", "true"),
        ("RATATOSKR__INGEST__PROVIDER_MODE", "fixture"),
        (
            "RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET",
            "readme-LEAKME",
        ),
    ])?;
    assert!(accepted.ingest.enabled);
    assert_eq!(
        accepted.ingest.provider_mode,
        ratatoskr_knowledge::IngestProviderMode::Fixture
    );
    assert!(!serde_json::to_string(&accepted)?.contains("readme-LEAKME"));
    assert!(!format!("{accepted:?}").contains("readme-LEAKME"));

    // The whole section is inert while disabled, so a half-configured ingest does not block boot.
    let dormant = Config::from_environment([("RATATOSKR__INGEST__PROVIDER_MODE", "fixture")])?;
    assert!(!dormant.ingest.enabled);
    Ok(())
}

/// An enabled ingest configuration with `entries` added must be refused with a diagnostic that
/// names `key` and leaks no value.
fn assert_ingest_rejected(
    entries: &[(&str, &str)],
    key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut all = vec![
        ("RATATOSKR__INGEST__ENABLED", "true"),
        (
            "RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET",
            "readme-LEAKME",
        ),
    ];
    all.extend_from_slice(entries);
    let diagnostic = match Config::from_environment(all) {
        Ok(_) => return Err(format!("the configuration naming {key} was accepted").into()),
        Err(error) => error.to_string(),
    };
    assert!(diagnostic.contains(key), "{diagnostic} should name {key}");
    assert!(
        !diagnostic.contains("LEAKME"),
        "{diagnostic} leaked a value"
    );
    Ok(())
}

/// XR-021 CONTRACTS.md S09: the bus and provider settings are validated as a whole before the
/// process may consume.
#[test]
fn ingest_section_refuses_unsafe_bus_and_provider_settings()
-> Result<(), Box<dyn std::error::Error>> {
    let seed_file = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    assert_ingest_rejected(
        &[
            ("RATATOSKR__INGEST__PROVIDER_MODE", "fixture"),
            (
                "RATATOSKR__INGEST__BUS_ENDPOINT",
                "tls://nats.example.internal:4222",
            ),
            ("RATATOSKR__INGEST__BUS_CREDENTIALS_FILE", seed_file),
        ],
        "RATATOSKR__INGEST__PROVIDER_MODE",
    )?;
    assert_ingest_rejected(
        &[
            ("RATATOSKR__PROVIDER__OPENROUTER__API_KEY", "sk-or-LEAKME"),
            (
                "RATATOSKR__PROVIDER__OPENROUTER__MODEL",
                "openai/gpt-oss-20b",
            ),
            (
                "RATATOSKR__INGEST__BUS_ENDPOINT",
                "tls://nats.example.internal:4222",
            ),
        ],
        "RATATOSKR__INGEST__BUS_CREDENTIALS_FILE",
    )?;
    assert_ingest_rejected(
        &[("RATATOSKR__INGEST__PROVIDER_MODE", "openrouter")],
        "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
    )?;
    assert_ingest_rejected(
        &[("RATATOSKR__INGEST__PROVIDER_MODE", "mystery")],
        "RATATOSKR__INGEST__PROVIDER_MODE",
    )?;
    Ok(())
}

/// XR-021 CONTRACTS.md S09: the README source is loopback-only and the worker limits are finite.
#[test]
fn ingest_section_refuses_unsafe_readme_source_and_limits() -> Result<(), Box<dyn std::error::Error>>
{
    assert_ingest_rejected(
        &[
            ("RATATOSKR__INGEST__PROVIDER_MODE", "fixture"),
            (
                "RATATOSKR__INGEST__GITHUB_README_BASE_URL",
                "https://github.example/",
            ),
        ],
        "RATATOSKR__INGEST__GITHUB_README_BASE_URL",
    )?;
    assert_ingest_rejected(
        &[
            ("RATATOSKR__INGEST__PROVIDER_MODE", "fixture"),
            (
                "RATATOSKR__INGEST__GITHUB_README_BASE_URL",
                "http://github.example.internal:8092/",
            ),
        ],
        "RATATOSKR__INGEST__GITHUB_README_BASE_URL",
    )?;
    assert_ingest_rejected(
        &[
            ("RATATOSKR__INGEST__PROVIDER_MODE", "fixture"),
            ("RATATOSKR__INGEST__ACK_WAIT_SECONDS", "0"),
        ],
        "RATATOSKR__INGEST__ACK_WAIT_SECONDS",
    )?;

    let without_secret = Config::from_environment([
        ("RATATOSKR__INGEST__ENABLED", "true"),
        ("RATATOSKR__INGEST__PROVIDER_MODE", "fixture"),
    ])
    .expect_err("repository analysis cannot complete without its README source")
    .to_string();
    assert!(without_secret.contains("RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET"));

    let unknown = Config::from_environment([("RATATOSKR__INGEST__MYSTERY", "LEAKME")])
        .expect_err("unknown ingest key must fail")
        .to_string();
    assert!(unknown.contains("RATATOSKR__INGEST__MYSTERY"));
    assert!(!unknown.contains("LEAKME"));
    Ok(())
}

/// The operator listener default is the port the fleet allocation table assigns to Knowledge.
#[test]
fn admin_default_is_the_allocated_operator_port() {
    assert_eq!(Config::default().admin.listen_address.port(), 9081);
}
