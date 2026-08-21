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
