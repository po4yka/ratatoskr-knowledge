use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Strict version-one article analysis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ArticleAnalysis {
    /// Human-readable source-grounded summary.
    #[schemars(length(min = 1, max = 2_000))]
    pub summary: String,
    /// Ordered source-grounded key points.
    #[schemars(length(min = 1, max = 10))]
    pub key_points: Vec<KeyPoint>,
}

/// One source-grounded article key point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct KeyPoint {
    /// Human-readable key point text.
    #[schemars(length(min = 1, max = 500))]
    pub text: String,
    /// Unique zero-based indexes into the supplied Document IR blocks.
    #[schemars(length(min = 1, max = 8))]
    pub source_block_indexes: Vec<u32>,
}

/// Safe article validation failure class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ArticleValidationError {
    /// The canonical generated schema could not be compiled.
    #[error("the article schema could not be compiled")]
    SchemaDefinition,
    /// Provider JSON does not satisfy the generated schema.
    #[error("the article response failed structural validation")]
    Structural,
    /// A structurally valid value could not be decoded.
    #[error("the article response could not be decoded")]
    Decode,
}

/// Generates the canonical version-one article JSON Schema.
///
/// # Errors
///
/// Returns [`ArticleValidationError::SchemaDefinition`] when serialization fails.
pub fn article_analysis_schema() -> Result<serde_json::Value, ArticleValidationError> {
    serde_json::to_value(schemars::schema_for!(ArticleAnalysis))
        .map_err(|_| ArticleValidationError::SchemaDefinition)
}

/// Parses a provider JSON value into the typed article result.
///
/// # Errors
///
/// Returns [`ArticleValidationError`] when schema compilation, structural validation, or typed
/// decoding fails.
pub fn validate_article_json(
    value: &serde_json::Value,
) -> Result<ArticleAnalysis, ArticleValidationError> {
    let schema = article_analysis_schema()?;
    let validator = jsonschema::options()
        .should_validate_formats(true)
        .build(&schema)
        .map_err(|_| ArticleValidationError::SchemaDefinition)?;
    validator
        .validate(value)
        .map_err(|_| ArticleValidationError::Structural)?;
    serde_json::from_value(value.clone()).map_err(|_| ArticleValidationError::Decode)
}
