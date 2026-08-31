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
fn primary_role_requires_bus_provider_and_authenticated_github_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let token_file = std::env::temp_dir().join(format!(
        "ratatoskr-knowledge-github-token-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::write(&token_file, "bounded-test-service-token\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o600))?;
    }
    let token_path = token_file.to_string_lossy().into_owned();
    let entries = [
        ("RATATOSKR__RUNTIME__ROLE", "primary"),
        (
            "RATATOSKR__PROVIDER__OPENROUTER__API_KEY",
            "synthetic-provider-token",
        ),
        (
            "RATATOSKR__PROVIDER__OPENROUTER__MODEL",
            "scripted/knowledge",
        ),
        (
            "RATATOSKR__PROVIDER__OPENROUTER__BASE_URL",
            "http://127.0.0.1:8099/v1",
        ),
        ("RATATOSKR__PRIMARY__GITHUB_TOKEN_FILE", token_path.as_str()),
        (
            "RATATOSKR__PRIMARY__GITHUB_BASE_URL",
            "http://github-catalog:9083/",
        ),
    ];
    let configured = Config::from_environment(entries)?;
    assert_eq!(configured.primary.bus_stream, "ratatoskr_events");
    assert_eq!(configured.primary.bus_durable, "ratatoskr_knowledge_main");
    assert_eq!(configured.primary.readme_response_bytes, 1_048_576);

    let missing_provider = Config::from_environment([
        ("RATATOSKR__RUNTIME__ROLE", "primary"),
        ("RATATOSKR__PRIMARY__GITHUB_TOKEN_FILE", token_path.as_str()),
    ])
    .expect_err("primary must not silently become admin-only")
    .to_string();
    assert!(missing_provider.contains("RATATOSKR__PROVIDER__OPENROUTER__API_KEY"));

    let drifted = Config::from_environment(entries.into_iter().chain([(
        "RATATOSKR__PRIMARY__BUS_DURABLE",
        "knowledge-created-consumer",
    )]))
    .expect_err("primary must refuse a non-canonical durable")
    .to_string();
    assert!(drifted.contains("RATATOSKR__PRIMARY__BUS_DURABLE"));

    let unsafe_lease = Config::from_environment(entries.into_iter().chain([
        ("RATATOSKR__PRIMARY__LEASE_SECONDS", "5"),
        ("RATATOSKR__LIMITS__PROVIDER_TIMEOUT_MS", "5000"),
    ]))
    .expect_err("a lease must outlive every external-call deadline")
    .to_string();
    assert!(unsafe_lease.contains("RATATOSKR__PRIMARY__LEASE_SECONDS"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let bus_seed = std::env::temp_dir().join(format!(
            "ratatoskr-knowledge-nkey-seed-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::write(&bus_seed, "synthetic-nkey-seed\n")?;
        std::fs::set_permissions(&bus_seed, std::fs::Permissions::from_mode(0o640))?;
        let seed_path = bus_seed.to_string_lossy().into_owned();
        let insecure = Config::from_environment(entries.into_iter().chain([(
            "RATATOSKR__PRIMARY__BUS_CREDENTIALS_FILE",
            seed_path.as_str(),
        )]))
        .expect_err("group-readable bus credentials must fail closed")
        .to_string();
        assert!(insecure.contains("RATATOSKR__PRIMARY__BUS_CREDENTIALS_FILE"));
        assert!(!insecure.contains("synthetic-nkey-seed"));
        std::fs::remove_file(bus_seed)?;
    }

    std::fs::remove_file(token_file)?;
    Ok(())
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
