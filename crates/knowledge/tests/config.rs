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
