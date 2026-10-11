//! Fixtures for the channel-recap integration tests: a manifest that went through the production
//! verifier and a recap run seeded up to `context_prepared`.

use ratatoskr_channel_digest_contracts::KnowledgeChannelDigestRecapRequested;
use ratatoskr_event_envelope::CommandEnvelope;
use sha2::Digest as _;

use crate::{
    ChannelRecapInbox, ChannelRecapInboxAdmission, ChannelRecapRunState, ChannelRecapRunStore,
    Database, PreparedChannelRecapContext, ProviderResponse, ProviderUsage, VerifiedDigestManifest,
    verify_digest_manifest,
};

type FixtureError = Box<dyn std::error::Error>;

/// Owner of every fixture manifest.
pub const FIXTURE_OWNER: &str = "user:018f0000-0000-7000-8000-000000000202";

/// Builds one contract-valid manifest source.
///
/// `index` makes the revision, message identity and publication second unique; `channel` selects
/// one of the fixture channels.
#[must_use]
pub fn fixture_source(index: usize, channel: usize, content: &str) -> serde_json::Value {
    let second = 59_usize.saturating_sub(index % 59);
    serde_json::json!({
        "revision_ref": format!("channel-post-revision:018f0000-0000-7000-8000-{index:012}"),
        "channel_ref": format!("telegram-public-channel:018f0000-0000-7000-8000-{:012}", 100 + channel),
        "channel_label": format!("Fixture {channel:02}"),
        "message_id": format!("{}", 1_000 + index),
        "published_at": format!("2026-08-21T09:00:{second:02}Z"),
        "content": content,
        "content_digest": {
            "algorithm": "sha256",
            "hex": format!("{:x}", sha2::Sha256::digest(content.as_bytes())),
        },
        "public_link": format!("https://t.me/fixture_{channel:02}/{}", 1_000 + index),
        "revision": 1
    })
}

/// Builds the recap request that links to canonical manifest `bytes`.
///
/// # Errors
///
/// Returns a decoding error when the fixture values do not form a valid request.
pub fn fixture_request(
    bytes: &[u8],
    sources: usize,
    channels: usize,
    output_language: &str,
    variant: u32,
) -> Result<KnowledgeChannelDigestRecapRequested, serde_json::Error> {
    fixture_request_for(
        FIXTURE_OWNER,
        bytes,
        sources,
        channels,
        output_language,
        variant,
    )
}

/// [`fixture_request`] for an explicit owner.
///
/// # Errors
///
/// Returns a decoding error when the fixture values do not form a valid request.
pub fn fixture_request_for(
    owner: &str,
    bytes: &[u8],
    sources: usize,
    channels: usize,
    output_language: &str,
    variant: u32,
) -> Result<KnowledgeChannelDigestRecapRequested, serde_json::Error> {
    serde_json::from_value(serde_json::json!({
        "operation_id": variant_id(201, variant),
        "owner": owner,
        "digest_run_id": variant_id(203, variant),
        "window": {
            "start_at": "2026-08-20T10:00:00Z",
            "end_at": "2026-08-21T10:00:00Z"
        },
        "output_language": output_language,
        "source_count": sources,
        "channel_count": channels,
        "manifest_ref": format!("channel-digest-manifest:{}", variant_id(204, variant)),
        "manifest_digest": {
            "algorithm": "sha256",
            "hex": format!("{:x}", sha2::Sha256::digest(bytes)),
        },
        "analysis_family": "channel_digest_recap",
        "analysis_contract": "channel_digest_recap.v1"
    }))
}

/// A fixture identity: `base` for variant 0, and a distinct one per further variant, so several
/// recap runs can share one database.
fn variant_id(base: u32, variant: u32) -> String {
    format!(
        "018f0000-0000-7000-8000-{:012}",
        u64::from(base) + u64::from(variant) * 1_000
    )
}

fn order_key(source: &serde_json::Value) -> (String, String, u128, u64) {
    let text = |name: &str| {
        source
            .get(name)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    (
        text("published_at"),
        text("channel_ref"),
        text("message_id").parse().unwrap_or_default(),
        source
            .get("revision")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
    )
}

/// Builds the canonical manifest bytes of `sources`, ordered as the contract requires, together
/// with the request that links to them.
///
/// # Errors
///
/// Returns an error when the fixture values cannot be encoded.
pub fn fixture_manifest_bytes(
    sources: Vec<serde_json::Value>,
    output_language: &str,
    variant: u32,
) -> Result<(Vec<u8>, KnowledgeChannelDigestRecapRequested), FixtureError> {
    fixture_manifest_bytes_for(FIXTURE_OWNER, sources, output_language, variant)
}

/// [`fixture_manifest_bytes`] for an explicit owner.
///
/// # Errors
///
/// Returns an error when the fixture values cannot be encoded.
pub fn fixture_manifest_bytes_for(
    owner: &str,
    mut sources: Vec<serde_json::Value>,
    output_language: &str,
    variant: u32,
) -> Result<(Vec<u8>, KnowledgeChannelDigestRecapRequested), FixtureError> {
    sources.sort_by_key(order_key);
    let channels = sources
        .iter()
        .filter_map(|source| {
            source
                .get("channel_ref")
                .and_then(serde_json::Value::as_str)
        })
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let count = sources.len();
    let bytes = serde_json::to_vec(&serde_json::json!({
        "schema": "channel_digest_manifest.v1",
        "manifest_ref": format!("channel-digest-manifest:{}", variant_id(204, variant)),
        "owner": owner,
        "digest_run_id": variant_id(203, variant),
        "window": {
            "start_at": "2026-08-20T10:00:00Z",
            "end_at": "2026-08-21T10:00:00Z"
        },
        "sources": sources
    }))?;
    let request = fixture_request_for(owner, &bytes, count, channels, output_language, variant)?;
    Ok((bytes, request))
}

/// A recap run committed up to `context_prepared`, as the worker leaves it before the provider.
#[derive(Debug)]
pub struct SeededRecap {
    /// Manifest verified through the production verifier.
    pub verified: VerifiedDigestManifest,
    /// The request the run belongs to.
    pub request: KnowledgeChannelDigestRecapRequested,
    /// The durable run identity.
    pub run_id: uuid::Uuid,
}

/// Admits a recap command for `sources` and drives its run to `context_prepared`.
///
/// # Errors
///
/// Returns an error when the fixture cannot be admitted or the run cannot be advanced.
pub async fn seed_recap_run(
    database: &Database,
    sources: Vec<serde_json::Value>,
    output_language: &str,
) -> Result<SeededRecap, FixtureError> {
    seed_recap_run_variant(database, sources, output_language, 0).await
}

/// [`seed_recap_run`] with a distinct run identity, for several runs in one database.
///
/// # Errors
///
/// Returns an error when the fixture cannot be admitted or the run cannot be advanced.
pub async fn seed_recap_run_variant(
    database: &Database,
    sources: Vec<serde_json::Value>,
    output_language: &str,
    variant: u32,
) -> Result<SeededRecap, FixtureError> {
    seed_recap_run_for(database, FIXTURE_OWNER, sources, output_language, variant).await
}

/// [`seed_recap_run_variant`] for an explicit owner, so several tenants can share one database.
///
/// # Errors
///
/// Returns an error when the fixture cannot be admitted or the run cannot be advanced.
pub async fn seed_recap_run_for(
    database: &Database,
    owner: &str,
    sources: Vec<serde_json::Value>,
    output_language: &str,
    variant: u32,
) -> Result<SeededRecap, FixtureError> {
    let (bytes, request) = fixture_manifest_bytes_for(owner, sources, output_language, variant)?;
    let verified = verify_digest_manifest(&request, &bytes)?;
    let envelope: CommandEnvelope = serde_json::from_value(serde_json::json!({
        "command_id": variant_id(502, variant),
        "command_type": "knowledge.channel_digest_recap.requested.v1",
        "issued_at": "2026-08-21T10:00:01Z",
        "producer": "ratatoskr-channel-digests",
        "aggregate_id": format!("channel-digest-run:{}", request.digest_run_id),
        "correlation_id": format!("operation:{}", request.operation_id.0),
        "tenant_id": request.owner,
        "schema_version": 1,
        "payload": request
    }))?;
    let admission = ChannelRecapInbox::new(database).accept(&envelope).await?;
    if admission != ChannelRecapInboxAdmission::Accepted {
        return Err("the fixture command was not newly admitted".into());
    }
    let run_id: uuid::Uuid = sqlx::query_scalar(
        "select recap_run_id from knowledge.channel_recap_runs where inbox_command_id = $1",
    )
    .bind(envelope.command_id.0)
    .fetch_one(database.pool())
    .await?;
    let runs = ChannelRecapRunStore::new(database);
    runs.accept_verified_manifest(run_id, ChannelRecapRunState::Received, &verified)
        .await?;
    runs.transition(
        run_id,
        ChannelRecapRunState::ManifestVerified,
        ChannelRecapRunState::ContextPrepared,
    )
    .await?;
    Ok(SeededRecap {
        verified,
        request,
        run_id,
    })
}

/// What a real model returns: the headline, overview, topics, notable items and warnings, and
/// nothing the system already knows. The first included source is cited by the one topic.
#[must_use]
pub fn model_output(context: &PreparedChannelRecapContext, warnings: &[&str]) -> serde_json::Value {
    let citation = context
        .sources
        .first()
        .map(|source| source.revision_ref.clone())
        .unwrap_or_default();
    serde_json::json!({
        "headline": "Grounded fixture recap",
        "overview": "The supplied fixture discusses one bounded event.",
        "topics": [{
            "label": "Fixture topic",
            "summary": "A summary grounded only in the supplied complete revision.",
            "citations": [citation]
        }],
        "notable_items": [],
        "warnings": warnings
    })
}

/// Wraps `value` as a scripted provider response.
///
/// # Errors
///
/// Returns an encoding error when `value` cannot be serialized.
pub fn provider_response(
    value: &serde_json::Value,
    request_id: &str,
) -> Result<ProviderResponse, serde_json::Error> {
    Ok(ProviderResponse {
        bytes: serde_json::to_vec(value)?,
        request_id: Some(request_id.to_owned()),
        usage: ProviderUsage {
            input_tokens: 30,
            output_tokens: 10,
        },
    })
}
