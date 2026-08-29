//! Versioned prompt boundary for adversarial public-channel source evidence.

use crate::PreparedChannelRecapContext;

const SYSTEM_POLICY: &str = include_str!("../../../prompts/channel-digest-recap.v1/system.txt");
const TASK_INSTRUCTION: &str = include_str!("../../../prompts/channel-digest-recap.v1/task.txt");

/// Stable channel-recap prompt identity.
pub const CHANNEL_RECAP_PROMPT_VERSION: &str = "channel_digest_recap_prompt.v1";

/// Trusted source label kept separate from untrusted post text.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChannelRecapSourceLabel {
    /// Opaque revision identity accepted for citations.
    pub revision_ref: String,
    /// Stable public-channel reference.
    pub channel_ref: String,
    /// Bounded channel display label.
    pub channel_label: String,
    /// Provider-authored publication instant.
    pub published_at: String,
    /// Digest of the complete normalized source text.
    pub content_digest_hex: String,
}

/// Explicitly untrusted provider-visible source field.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChannelRecapSourceContent {
    /// Opaque revision identity joining this field to one trusted label.
    pub revision_ref: String,
    /// Complete normalized post content treated only as evidence.
    pub untrusted_content: String,
}

/// Provider-neutral recap request with separated trust domains.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChannelRecapProviderRequest {
    /// Stable prompt resource identity.
    pub prompt_version: &'static str,
    /// Fixed system policy source content cannot replace.
    pub system_policy: String,
    /// Fixed recap task source content cannot replace.
    pub task_instruction: String,
    /// Closed generated structured-result schema.
    pub output_schema: serde_json::Value,
    /// Trusted labels and citation identities only.
    pub source_labels: Vec<ChannelRecapSourceLabel>,
    /// Untrusted source bodies in a dedicated field.
    pub untrusted_sources: Vec<ChannelRecapSourceContent>,
    /// External retrieval is forbidden for this analysis family.
    pub allow_external_fetch: bool,
    /// Finite maximum provider completion tokens.
    pub max_output_tokens: u32,
}

/// Safe provider-request construction failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ChannelRecapGenerationError {
    /// The fixed output schema or request fields could not be encoded.
    #[error("the channel recap provider request could not be encoded")]
    Encode,
    /// Output token budget is zero or above the family ceiling.
    #[error("the channel recap output token budget is invalid")]
    InvalidBudget,
}

/// Builds the fixed-policy bounded request from deterministic prepared context.
///
/// # Errors
///
/// Returns a safe encoding or output-budget failure.
pub fn build_channel_recap_provider_request(
    context: &PreparedChannelRecapContext,
    max_output_tokens: u32,
) -> Result<ChannelRecapProviderRequest, ChannelRecapGenerationError> {
    if !(1..=4_096).contains(&max_output_tokens) {
        return Err(ChannelRecapGenerationError::InvalidBudget);
    }
    let source_labels = context
        .sources
        .iter()
        .map(|source| ChannelRecapSourceLabel {
            revision_ref: source.revision_ref.clone(),
            channel_ref: source.channel_ref.clone(),
            channel_label: source.channel_label.clone(),
            published_at: source.published_at.clone(),
            content_digest_hex: source.content_digest_hex.clone(),
        })
        .collect();
    let untrusted_sources = context
        .sources
        .iter()
        .map(|source| ChannelRecapSourceContent {
            revision_ref: source.revision_ref.clone(),
            untrusted_content: source.content.clone(),
        })
        .collect();
    Ok(ChannelRecapProviderRequest {
        prompt_version: CHANNEL_RECAP_PROMPT_VERSION,
        system_policy: SYSTEM_POLICY.to_owned(),
        task_instruction: TASK_INSTRUCTION.to_owned(),
        output_schema: crate::channel_digest_recap_result::channel_digest_recap_schema()
            .map_err(|_| ChannelRecapGenerationError::Encode)?,
        source_labels,
        untrusted_sources,
        allow_external_fetch: false,
        max_output_tokens,
    })
}
