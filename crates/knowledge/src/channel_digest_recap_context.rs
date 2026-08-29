//! Deterministic bounded context preparation for channel-digest recap analysis.

use crate::VerifiedDigestManifest;
use sha2::{Digest as _, Sha256};

/// Stable context preparation contract identity.
pub const CHANNEL_RECAP_CONTEXT_VERSION: &str = "channel_digest_recap_context.v1";

/// Finite context-selection policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelRecapContextPolicy {
    /// Maximum complete source revisions admitted to inference.
    pub max_sources: usize,
    /// Maximum distinct channels admitted to inference.
    pub max_channels: usize,
    /// Maximum Unicode scalar values across selected source records.
    pub max_characters: usize,
}

/// Stable reason a verified source did not enter provider context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelRecapOmissionReason {
    /// Source ranked below the hard source-count ceiling.
    SourceLimit,
    /// Source would introduce a channel beyond the hard channel ceiling.
    ChannelLimit,
    /// Complete source did not fit the configured character/token budget.
    ContextBudget,
}

/// Explicit identity and reason for one omitted verified revision.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChannelRecapOmission {
    /// Stable immutable source revision reference.
    pub revision_ref: String,
    /// Deterministic omission class.
    pub reason: ChannelRecapOmissionReason,
}

/// Complete untrusted source unit passed to the provider boundary.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PreparedChannelRecapSource {
    /// Stable immutable revision reference used for citations.
    pub revision_ref: String,
    /// Stable public-channel reference.
    pub channel_ref: String,
    /// Bounded source-attribution label.
    pub channel_label: String,
    /// Provider-authored publication instant.
    pub published_at: String,
    /// Digest of the complete normalized content.
    pub content_digest_hex: String,
    /// Complete normalized untrusted content; never truncated.
    pub content: String,
}

/// Deterministic bounded recap context and exact coverage evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedChannelRecapContext {
    /// Context-policy identity.
    pub version: &'static str,
    /// Ordered complete included revisions.
    pub sources: Vec<PreparedChannelRecapSource>,
    /// Ordered explicit omissions.
    pub omissions: Vec<ChannelRecapOmission>,
    /// Total verified revisions considered.
    pub selected_count: usize,
    /// Complete revisions included in context.
    pub included_count: usize,
    /// Complete revisions omitted from context.
    pub omitted_count: usize,
    /// Distinct channels represented by included revisions.
    pub channel_count: usize,
    /// Unicode scalar values consumed by complete serialized source units.
    pub used_characters: usize,
    /// Deterministic conservative token estimate.
    pub estimated_tokens: usize,
    /// SHA-256 of version, ordered sources, omissions, and budget evidence.
    pub context_digest_hex: String,
}

/// Safe deterministic context-preparation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ChannelRecapContextError {
    /// Policy limits are zero or exceed the hard contract ceilings.
    #[error("the channel recap context policy is invalid")]
    InvalidPolicy,
    /// No complete verified revision fits the configured context budget.
    #[error("no complete channel source fits the context budget")]
    ContextBudget,
}

/// Builds deterministic complete-revision context from one verified manifest.
///
/// # Errors
///
/// Returns a safe policy or context-budget failure.
pub fn prepare_channel_recap_context(
    verified: &VerifiedDigestManifest,
    policy: ChannelRecapContextPolicy,
) -> Result<PreparedChannelRecapContext, ChannelRecapContextError> {
    if policy.max_sources == 0
        || policy.max_sources > 100
        || policy.max_channels == 0
        || policy.max_channels > 20
        || policy.max_characters == 0
    {
        return Err(ChannelRecapContextError::InvalidPolicy);
    }
    let mut ordered = verified.manifest.sources.clone();
    ordered.sort_by(|left, right| {
        right
            .published_at
            .cmp(&left.published_at)
            .then_with(|| left.channel_ref.cmp(&right.channel_ref))
            .then_with(|| left.message_id.cmp(&right.message_id))
            .then_with(|| left.revision.cmp(&right.revision))
            .then_with(|| left.revision_ref.cmp(&right.revision_ref))
    });
    let selected_count = ordered.len();
    let mut channels = std::collections::BTreeSet::new();
    let mut selected = Vec::new();
    let mut omissions = Vec::new();
    for source in ordered {
        if selected.len() >= policy.max_sources {
            omissions.push(omission(
                &source.revision_ref,
                ChannelRecapOmissionReason::SourceLimit,
            ));
            continue;
        }
        let introduces_channel = !channels.contains(source.channel_ref.as_str());
        if introduces_channel && channels.len() >= policy.max_channels {
            omissions.push(omission(
                &source.revision_ref,
                ChannelRecapOmissionReason::ChannelLimit,
            ));
            continue;
        }
        channels.insert(source.channel_ref.clone());
        selected.push(prepared_source(source));
    }
    let mut used_characters = source_character_count(&selected)?;
    while used_characters > policy.max_characters {
        let Some(removed) = selected.pop() else {
            return Err(ChannelRecapContextError::ContextBudget);
        };
        omissions.push(omission(
            &removed.revision_ref,
            ChannelRecapOmissionReason::ContextBudget,
        ));
        used_characters = source_character_count(&selected)?;
    }
    if selected.is_empty() {
        return Err(ChannelRecapContextError::ContextBudget);
    }
    omissions.sort_by(|left, right| {
        left.revision_ref
            .cmp(&right.revision_ref)
            .then_with(|| omission_rank(left.reason).cmp(&omission_rank(right.reason)))
    });
    let channel_count = selected
        .iter()
        .map(|source| source.channel_ref.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let included_count = selected.len();
    let omitted_count = omissions.len();
    let estimated_tokens = used_characters.saturating_add(3) / 4;
    let digest_material = serde_json::json!({
        "version": CHANNEL_RECAP_CONTEXT_VERSION,
        "sources": &selected,
        "omissions": &omissions,
        "selected_count": selected_count,
        "included_count": included_count,
        "omitted_count": omitted_count,
        "channel_count": channel_count,
        "used_characters": used_characters,
        "estimated_tokens": estimated_tokens,
    });
    let digest_bytes = serde_json::to_vec(&digest_material)
        .map_err(|_| ChannelRecapContextError::InvalidPolicy)?;
    Ok(PreparedChannelRecapContext {
        version: CHANNEL_RECAP_CONTEXT_VERSION,
        sources: selected,
        omissions,
        selected_count,
        included_count,
        omitted_count,
        channel_count,
        used_characters,
        estimated_tokens,
        context_digest_hex: format!("{:x}", Sha256::digest(digest_bytes)),
    })
}

fn prepared_source(source: crate::DigestManifestSource) -> PreparedChannelRecapSource {
    PreparedChannelRecapSource {
        revision_ref: source.revision_ref,
        channel_ref: source.channel_ref,
        channel_label: source.channel_label,
        published_at: source.published_at.to_wire(),
        content_digest_hex: source.content_digest.hex.to_string(),
        content: source.content,
    }
}

fn omission(revision_ref: &str, reason: ChannelRecapOmissionReason) -> ChannelRecapOmission {
    ChannelRecapOmission {
        revision_ref: revision_ref.to_owned(),
        reason,
    }
}

fn source_character_count(
    sources: &[PreparedChannelRecapSource],
) -> Result<usize, ChannelRecapContextError> {
    sources.iter().try_fold(0_usize, |total, source| {
        let encoded =
            serde_json::to_string(source).map_err(|_| ChannelRecapContextError::InvalidPolicy)?;
        Ok(total.saturating_add(encoded.chars().count()))
    })
}

const fn omission_rank(reason: ChannelRecapOmissionReason) -> u8 {
    match reason {
        ChannelRecapOmissionReason::SourceLimit => 0,
        ChannelRecapOmissionReason::ChannelLimit => 1,
        ChannelRecapOmissionReason::ContextBudget => 2,
    }
}
