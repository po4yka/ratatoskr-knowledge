//! Closed grounded structured result for channel-digest recap analysis.

use crate::PreparedChannelRecapContext;
use ratatoskr_identifiers::ContentDigest;

/// Fixed stored recap result contract.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub enum ChannelRecapContractVersion {
    /// First and only development contract.
    #[serde(rename = "channel_digest_recap.v1")]
    ChannelDigestRecapV1,
}

/// Fixed prompt version carried by accepted results.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub enum ChannelRecapPromptVersion {
    /// First channel-recap prompt.
    #[serde(rename = "channel_digest_recap_prompt.v1")]
    ChannelDigestRecapPromptV1,
}

/// Fixed deterministic context version carried by accepted results.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub enum ChannelRecapContextVersion {
    /// First channel-recap context policy.
    #[serde(rename = "channel_digest_recap_context.v1")]
    ChannelDigestRecapContextV1,
}

/// Supported recap output language.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum ChannelRecapOutputLanguage {
    /// Russian recap output.
    Ru,
    /// English recap output.
    En,
}

/// Closed safe non-content recap warning.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ChannelRecapWarning {
    /// One or more subscribed channels could not be acquired.
    PartialAcquisition,
    /// Complete verified sources were omitted under the context budget.
    ContextOmittedSources,
    /// Included sources contain materially conflicting claims.
    ConflictingSources,
    /// Multiple revisions of a provider message were represented.
    EditedSources,
    /// Included evidence is too limited for a broad recap.
    LimitedEvidence,
}

/// One opaque included source revision citation.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(transparent)]
pub struct ChannelRecapCitation(pub String);

/// One grounded recap topic.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ChannelRecapTopic {
    /// Bounded topic label.
    #[schemars(length(min = 1, max = 80))]
    pub label: String,
    /// Bounded grounded topic summary.
    #[schemars(length(min = 1, max = 400))]
    pub summary: String,
    /// Distinct included source citations.
    #[schemars(length(min = 1, max = 10))]
    pub citations: Vec<ChannelRecapCitation>,
}

/// One optional grounded notable item.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ChannelRecapNotableItem {
    /// Bounded item title.
    #[schemars(length(min = 1, max = 160))]
    pub title: String,
    /// Bounded item summary.
    #[schemars(length(min = 1, max = 320))]
    pub summary: String,
    /// Distinct included source citations.
    #[schemars(length(min = 1, max = 10))]
    pub citations: Vec<ChannelRecapCitation>,
}

/// Exact recap selection and context coverage.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ChannelRecapCoverage {
    /// Verified selected revisions.
    #[schemars(range(min = 1, max = 100))]
    pub selected_count: u16,
    /// Complete revisions included in provider context.
    #[schemars(range(min = 1, max = 100))]
    pub included_count: u16,
    /// Complete revisions omitted from provider context.
    #[schemars(range(min = 0, max = 99))]
    pub omitted_count: u16,
    /// Channels represented by included revisions.
    #[schemars(range(min = 1, max = 20))]
    pub channel_count: u16,
}

/// Strict grounded recap accepted from the provider boundary.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ChannelDigestRecap {
    /// Fixed result contract.
    pub contract_version: ChannelRecapContractVersion,
    /// Fixed prompt identity.
    pub prompt_version: ChannelRecapPromptVersion,
    /// Fixed deterministic context identity.
    pub context_version: ChannelRecapContextVersion,
    /// Requested result language.
    pub output_language: ChannelRecapOutputLanguage,
    /// Verified immutable source manifest digest.
    pub manifest_digest: ContentDigest,
    /// Bounded recap headline.
    #[schemars(length(min = 1, max = 160))]
    pub headline: String,
    /// Bounded recap overview.
    #[schemars(length(min = 1, max = 1_600))]
    pub overview: String,
    /// One through five grounded topic groups.
    #[schemars(length(min = 1, max = 5))]
    pub topics: Vec<ChannelRecapTopic>,
    /// Zero through five grounded notable items.
    #[schemars(length(max = 5))]
    pub notable_items: Vec<ChannelRecapNotableItem>,
    /// Exact prepared-context coverage.
    pub coverage: ChannelRecapCoverage,
    /// Up to ten distinct closed safe warnings.
    #[schemars(length(max = 10))]
    pub warnings: Vec<ChannelRecapWarning>,
}

/// Safe structured-result validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ChannelRecapResultError {
    /// Generated schema could not be built.
    #[error("the channel recap schema could not be built")]
    SchemaDefinition,
    /// Provider JSON did not satisfy the generated closed schema.
    #[error("the channel recap response failed structural validation")]
    Structural,
    /// Structurally valid JSON could not be decoded.
    #[error("the channel recap response could not be decoded")]
    Decode,
    /// A citation is duplicate, foreign, or omitted from prepared context.
    #[error("the channel recap response contains an invalid citation")]
    Citation,
    /// Version, language, digest, or coverage does not match prepared evidence.
    #[error("the channel recap response linkage is invalid")]
    Linkage,
    /// Provider-authored text contains a URL, which this result contract forbids.
    #[error("the channel recap response contains a forbidden link")]
    ForbiddenLink,
}

/// Generates the canonical closed recap JSON Schema.
///
/// # Errors
///
/// Returns a safe schema-definition failure when serialization is impossible.
pub fn channel_digest_recap_schema() -> Result<serde_json::Value, ChannelRecapResultError> {
    serde_json::to_value(schemars::schema_for!(ChannelDigestRecap))
        .map_err(|_| ChannelRecapResultError::SchemaDefinition)
}

/// Decodes a provider result before semantic grounding is implemented.
///
/// # Errors
///
/// Returns a safe typed decode failure.
pub fn validate_channel_digest_recap(
    value: &serde_json::Value,
    context: &PreparedChannelRecapContext,
    manifest_digest_hex: &str,
    output_language: ChannelRecapOutputLanguage,
) -> Result<ChannelDigestRecap, ChannelRecapResultError> {
    let schema = channel_digest_recap_schema()?;
    let validator = jsonschema::options()
        .should_validate_formats(true)
        .build(&schema)
        .map_err(|_| ChannelRecapResultError::SchemaDefinition)?;
    validator
        .validate(value)
        .map_err(|_| ChannelRecapResultError::Structural)?;
    let recap: ChannelDigestRecap =
        serde_json::from_value(value.clone()).map_err(|_| ChannelRecapResultError::Decode)?;
    if recap.output_language != output_language
        || recap.manifest_digest.hex.as_str() != manifest_digest_hex
        || !matches!(
            recap.manifest_digest.algorithm,
            ratatoskr_identifiers::DigestAlgorithm::Sha256
        )
        || usize::from(recap.coverage.selected_count) != context.selected_count
        || usize::from(recap.coverage.included_count) != context.included_count
        || usize::from(recap.coverage.omitted_count) != context.omitted_count
        || usize::from(recap.coverage.channel_count) != context.channel_count
        || recap
            .coverage
            .included_count
            .checked_add(recap.coverage.omitted_count)
            != Some(recap.coverage.selected_count)
    {
        return Err(ChannelRecapResultError::Linkage);
    }
    let distinct_warnings = recap
        .warnings
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if distinct_warnings.len() != recap.warnings.len() {
        return Err(ChannelRecapResultError::Linkage);
    }
    let included = context
        .sources
        .iter()
        .map(|source| source.revision_ref.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for citations in recap
        .topics
        .iter()
        .map(|topic| topic.citations.as_slice())
        .chain(
            recap
                .notable_items
                .iter()
                .map(|item| item.citations.as_slice()),
        )
    {
        validate_citations(citations, &included)?;
    }
    if recap_texts(&recap).any(contains_forbidden_link) {
        return Err(ChannelRecapResultError::ForbiddenLink);
    }
    Ok(recap)
}

fn validate_citations(
    citations: &[ChannelRecapCitation],
    included: &std::collections::BTreeSet<&str>,
) -> Result<(), ChannelRecapResultError> {
    let mut seen = std::collections::BTreeSet::new();
    for citation in citations {
        if !seen.insert(citation.0.as_str()) || !included.contains(citation.0.as_str()) {
            return Err(ChannelRecapResultError::Citation);
        }
    }
    Ok(())
}

fn recap_texts(recap: &ChannelDigestRecap) -> impl Iterator<Item = &str> {
    std::iter::once(recap.headline.as_str())
        .chain(std::iter::once(recap.overview.as_str()))
        .chain(
            recap
                .topics
                .iter()
                .flat_map(|topic| [topic.label.as_str(), topic.summary.as_str()]),
        )
        .chain(
            recap
                .notable_items
                .iter()
                .flat_map(|item| [item.title.as_str(), item.summary.as_str()]),
        )
}

fn contains_forbidden_link(value: &str) -> bool {
    let lowercase = value.to_ascii_lowercase();
    ["http://", "https://", "www.", "t.me/"]
        .iter()
        .any(|marker| lowercase.contains(marker))
}
