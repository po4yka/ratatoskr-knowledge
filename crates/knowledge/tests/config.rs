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
