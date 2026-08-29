/// Closed validation-failure vocabulary safe for telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationClass {
    /// The provider bytes are not JSON.
    JsonSyntax,
    /// The JSON does not match the generated schema.
    Schema,
    /// A citation is absent from the supplied source context.
    Citation,
}

/// Telemetry bootstrap failure.
#[derive(Debug, thiserror::Error)]
#[error("telemetry was already initialized")]
pub struct TelemetryError(#[source] Box<dyn std::error::Error + Send + Sync>);

impl ValidationClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::JsonSyntax => "json_syntax",
            Self::Schema => "schema",
            Self::Citation => "citation",
        }
    }
}

/// Installs the process-wide structured telemetry subscriber once.
///
/// # Errors
///
/// Returns [`TelemetryError`] when another global subscriber is already installed.
pub fn init_telemetry() -> Result<(), TelemetryError> {
    tracing_subscriber::fmt()
        .json()
        .try_init()
        .map_err(TelemetryError)
}

/// Records one validation failure with bounded fields.
pub fn record_validation_failure(class: ValidationClass, _source_text: &str, _response_text: &str) {
    tracing::warn!(
        operation = "article_analysis",
        state = "response_received",
        outcome = "invalid",
        validation_class = class.as_str(),
        attempt_count = 1_u8,
        duration_ms = 0_u64
    );
}

/// Records one channel-recap pipeline boundary with closed, content-free fields.
pub(crate) fn record_channel_recap_pipeline(
    state: &'static str,
    outcome: &'static str,
    duration_ms: i32,
) {
    tracing::info!(
        operation = "channel_digest_recap",
        state = state,
        outcome = outcome,
        attempt_count = 1_u8,
        duration_ms = duration_ms
    );
}
