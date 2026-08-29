//! Deterministic offline quality gate for synthetic channel-recap fixtures.

use crate::{
    ChannelRecapContextError, ChannelRecapContextPolicy, ChannelRecapOutputLanguage,
    DigestManifestError, PreparedChannelRecapContext, prepare_channel_recap_context,
    validate_channel_digest_recap, verify_digest_manifest,
};
use ratatoskr_channel_digest_contracts::KnowledgeChannelDigestRecapRequested;
use sha2::{Digest as _, Sha256};

const METRICS: [&str; 6] = [
    "schema",
    "citations",
    "unsupported_claims",
    "coverage",
    "context_digest",
    "budgets",
];

/// One stable channel-recap evaluation metric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRecapEvalCheck {
    /// Stable metric name.
    pub name: String,
    /// Whether the fixture met the metric.
    pub passed: bool,
}

/// All metric outcomes for one synthetic fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRecapEvalCaseReport {
    /// Stable fixture identifier.
    pub case_id: String,
    /// Deterministic metric outcomes.
    pub checks: Vec<ChannelRecapEvalCheck>,
}

/// Complete channel-recap offline evaluation artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRecapEvaluationReport {
    /// Stable case order.
    pub cases: Vec<ChannelRecapEvalCaseReport>,
}

/// Safe committed-fixture evaluation failure.
#[derive(Debug, thiserror::Error)]
pub enum ChannelRecapEvaluationError {
    /// Committed fixture bytes could not be read, decoded, or evaluated.
    #[error("channel recap evaluation fixtures are invalid")]
    Fixture,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalCorpus {
    cases: Vec<EvalFixture>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalFixture {
    id: String,
    expectation: EvalExpectation,
    output_language: String,
    max_characters: usize,
    sources: Vec<EvalSource>,
    #[serde(default)]
    duplicate_first_source: bool,
    #[serde(default)]
    warnings: Vec<String>,
    #[serde(default)]
    unsupported_markers: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum EvalExpectation {
    Accepted,
    Empty,
    ManifestRejected,
    ContextBudget,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalSource {
    channel: u16,
    message_id: u64,
    revision: u32,
    published_at: String,
    content: String,
}

/// Runs the committed synthetic channel-recap corpus without a network or provider.
///
/// # Errors
///
/// Returns a safe fixture error when the committed corpus is malformed.
pub fn run_committed_channel_recap_evaluation()
-> Result<ChannelRecapEvaluationReport, ChannelRecapEvaluationError> {
    let corpus: EvalCorpus = serde_json::from_slice(include_bytes!(
        "../fixtures/channel_digest_recap_eval/cases.json"
    ))
    .map_err(|_| ChannelRecapEvaluationError::Fixture)?;
    if corpus.cases.is_empty() {
        return Err(ChannelRecapEvaluationError::Fixture);
    }
    let cases = corpus
        .cases
        .iter()
        .map(evaluate_case)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ChannelRecapEvaluationReport { cases })
}

fn evaluate_case(
    fixture: &EvalFixture,
) -> Result<ChannelRecapEvalCaseReport, ChannelRecapEvaluationError> {
    if fixture.expectation == EvalExpectation::Empty {
        return Ok(report(fixture, fixture.sources.is_empty()));
    }
    let (request, manifest_bytes) = fixture_manifest(fixture)?;
    let verified = verify_digest_manifest(&request, &manifest_bytes);
    if fixture.expectation == EvalExpectation::ManifestRejected {
        return Ok(report(
            fixture,
            matches!(verified, Err(DigestManifestError::Integrity)),
        ));
    }
    let verified = verified.map_err(|_| ChannelRecapEvaluationError::Fixture)?;
    let policy = ChannelRecapContextPolicy {
        max_sources: 100,
        max_channels: 20,
        max_characters: fixture.max_characters,
    };
    let context = prepare_channel_recap_context(&verified, policy);
    if fixture.expectation == EvalExpectation::ContextBudget {
        return Ok(report(
            fixture,
            matches!(context, Err(ChannelRecapContextError::ContextBudget)),
        ));
    }
    let context = context.map_err(|_| ChannelRecapEvaluationError::Fixture)?;
    let repeated = prepare_channel_recap_context(&verified, policy)
        .map_err(|_| ChannelRecapEvaluationError::Fixture)?;
    let language = output_language(&fixture.output_language)?;
    let result = fixture_result(fixture, &context, &verified.digest_hex);
    let validated =
        validate_channel_digest_recap(&result, &context, &verified.digest_hex, language);
    let encoded =
        serde_json::to_string(&result).map_err(|_| ChannelRecapEvaluationError::Fixture)?;
    let included = context
        .sources
        .iter()
        .map(|source| source.revision_ref.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let citations_pass = result
        .get("topics")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|topic| {
            topic
                .get("citations")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(serde_json::Value::as_str)
        .all(|citation| included.contains(citation));
    let unsupported_pass = fixture
        .unsupported_markers
        .iter()
        .all(|marker| !encoded.contains(marker));
    let coverage_pass = context.selected_count == context.included_count + context.omitted_count
        && context.channel_count
            == context
                .sources
                .iter()
                .map(|source| source.channel_ref.as_str())
                .collect::<std::collections::BTreeSet<_>>()
                .len();
    let values = [
        validated.is_ok(),
        citations_pass,
        unsupported_pass,
        coverage_pass,
        context.context_digest_hex == repeated.context_digest_hex,
        context.used_characters <= fixture.max_characters
            && context.included_count <= 100
            && context.channel_count <= 20,
    ];
    Ok(ChannelRecapEvalCaseReport {
        case_id: fixture.id.clone(),
        checks: METRICS
            .into_iter()
            .zip(values)
            .map(|(name, passed)| ChannelRecapEvalCheck {
                name: name.to_owned(),
                passed,
            })
            .collect(),
    })
}

fn fixture_manifest(
    fixture: &EvalFixture,
) -> Result<(KnowledgeChannelDigestRecapRequested, Vec<u8>), ChannelRecapEvaluationError> {
    let mut sources = fixture
        .sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            serde_json::json!({
                "revision_ref": format!(
                    "channel-post-revision:018f0000-0000-7000-8000-{index:012}"
                ),
                "channel_ref": format!("telegram-public-channel:fixture-{:02}", source.channel),
                "channel_label": format!("Fixture {:02}", source.channel),
                "message_id": source.message_id.to_string(),
                "published_at": source.published_at,
                "content": source.content,
                "content_digest": {
                    "algorithm": "sha256",
                    "hex": sha256_hex(source.content.as_bytes())
                },
                "public_link": format!("https://t.me/fixture_{:02}/{}", source.channel, source.message_id),
                "revision": source.revision
            })
        })
        .collect::<Vec<_>>();
    if fixture.duplicate_first_source {
        let duplicate = sources
            .first()
            .cloned()
            .ok_or(ChannelRecapEvaluationError::Fixture)?;
        sources.push(duplicate);
    }
    let channel_count = sources
        .iter()
        .filter_map(|source| source.get("channel_ref"))
        .filter_map(serde_json::Value::as_str)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let source_count = sources.len();
    let manifest = serde_json::json!({
        "schema": "channel_digest_manifest.v1",
        "manifest_ref": "channel-digest-manifest:018f0000-0000-7000-8000-000000000204",
        "owner": "user:018f0000-0000-7000-8000-000000000202",
        "digest_run_id": "018f0000-0000-7000-8000-000000000203",
        "window": {
            "start_at": "2026-08-20T10:00:00Z",
            "end_at": "2026-08-21T10:00:00Z"
        },
        "sources": sources
    });
    let bytes = serde_json::to_vec(&manifest).map_err(|_| ChannelRecapEvaluationError::Fixture)?;
    let request = serde_json::from_value(serde_json::json!({
        "operation_id": "018f0000-0000-7000-8000-000000000501",
        "owner": "user:018f0000-0000-7000-8000-000000000202",
        "digest_run_id": "018f0000-0000-7000-8000-000000000203",
        "window": {
            "start_at": "2026-08-20T10:00:00Z",
            "end_at": "2026-08-21T10:00:00Z"
        },
        "output_language": fixture.output_language,
        "source_count": source_count,
        "channel_count": channel_count,
        "manifest_ref": "channel-digest-manifest:018f0000-0000-7000-8000-000000000204",
        "manifest_digest": {"algorithm": "sha256", "hex": sha256_hex(&bytes)},
        "analysis_family": "channel_digest_recap",
        "analysis_contract": "channel_digest_recap.v1"
    }))
    .map_err(|_| ChannelRecapEvaluationError::Fixture)?;
    Ok((request, bytes))
}

fn fixture_result(
    fixture: &EvalFixture,
    context: &PreparedChannelRecapContext,
    manifest_digest: &str,
) -> serde_json::Value {
    let citation = context
        .sources
        .first()
        .map(|source| source.revision_ref.as_str())
        .unwrap_or_default();
    serde_json::json!({
        "contract_version": "channel_digest_recap.v1",
        "prompt_version": "channel_digest_recap_prompt.v1",
        "context_version": "channel_digest_recap_context.v1",
        "output_language": fixture.output_language,
        "manifest_digest": {"algorithm": "sha256", "hex": manifest_digest},
        "headline": "Synthetic grounded recap",
        "overview": "The committed evidence contains a bounded synthetic update.",
        "topics": [{
            "label": "Synthetic update",
            "summary": "This summary is grounded in the cited committed fixture.",
            "citations": [citation]
        }],
        "notable_items": [],
        "coverage": {
            "selected_count": context.selected_count,
            "included_count": context.included_count,
            "omitted_count": context.omitted_count,
            "channel_count": context.channel_count
        },
        "warnings": fixture.warnings
    })
}

fn output_language(value: &str) -> Result<ChannelRecapOutputLanguage, ChannelRecapEvaluationError> {
    match value {
        "ru" => Ok(ChannelRecapOutputLanguage::Ru),
        "en" => Ok(ChannelRecapOutputLanguage::En),
        _ => Err(ChannelRecapEvaluationError::Fixture),
    }
}

fn report(fixture: &EvalFixture, passed: bool) -> ChannelRecapEvalCaseReport {
    ChannelRecapEvalCaseReport {
        case_id: fixture.id.clone(),
        checks: METRICS
            .into_iter()
            .map(|name| ChannelRecapEvalCheck {
                name: name.to_owned(),
                passed,
            })
            .collect(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
