//! Redacted authority for the channel-recap result reader.

use std::fmt;

use serde::Serialize;

/// Credential accepted only by the channel-recap result reader.
#[derive(Clone, PartialEq, Eq)]
pub struct ResultReaderSecret(String);

impl ResultReaderSecret {
    /// Wraps one bounded service credential.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// Exposes the credential only to the HTTP authorization boundary.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ResultReaderSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[redacted]")
    }
}

impl Serialize for ResultReaderSecret {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("[redacted]")
    }
}
