use std::fmt;

use serde::Serialize;

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
