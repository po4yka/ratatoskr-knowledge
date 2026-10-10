//! Deterministic offline article provider for fixture-mode ingestion.

use std::future::{Future, ready};

use crate::{
    GenerationRequest, LlmProvider, ProviderError, ProviderFailure, ProviderIdentity,
    ProviderResponse, ProviderUsage,
};

const ARTICLE_PROMPT: &str = "article_prompt_v1";
const SUMMARY_CHARACTERS: usize = 2_000;
const POINT_CHARACTERS: usize = 500;

/// Provider that answers the article analysis with the document's first paragraph.
///
/// The summary is the first paragraph and the one key point cites that paragraph's block. It is
/// the provider of the composed acceptance profile (`fixture` ingest mode), where a document must
/// become searchable without an inference credential. It answers no other analysis family: a
/// request with another prompt is a permanent failure rather than an invented result.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExtractiveArticleProvider;

impl LlmProvider for ExtractiveArticleProvider {
    fn identity(&self) -> ProviderIdentity {
        ProviderIdentity {
            provider: "extractive_fixture".to_owned(),
            model: "first_paragraph_v1".to_owned(),
        }
    }

    fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> impl Future<Output = Result<ProviderResponse, ProviderFailure>> + Send {
        ready(extract(&request))
    }
}

fn extract(request: &GenerationRequest) -> Result<ProviderResponse, ProviderFailure> {
    let permanent = || ProviderFailure::unclassified(ProviderError::Permanent);
    if request.prompt_version != ARTICLE_PROMPT {
        return Err(permanent());
    }
    let (index, text) = first_paragraph(&request.source_content).ok_or_else(permanent)?;
    let body = serde_json::to_vec(&serde_json::json!({
        "summary": text.chars().take(SUMMARY_CHARACTERS).collect::<String>(),
        "key_points": [{
            "text": text.chars().take(POINT_CHARACTERS).collect::<String>(),
            "source_block_indexes": [index],
        }],
    }))
    .map_err(|_| permanent())?;
    let input_tokens = u64::try_from(request.source_content.chars().count() / 4).unwrap_or(0);
    Ok(ProviderResponse {
        bytes: body,
        request_id: Some("extractive-fixture".to_owned()),
        usage: ProviderUsage {
            input_tokens,
            output_tokens: 16,
        },
    })
}

/// Finds the first rendered `block <index> paragraph: "<text>"` line of the prepared context.
fn first_paragraph(source: &str) -> Option<(u32, String)> {
    source.lines().find_map(|line| {
        let rest = line.strip_prefix("block ")?;
        let (index, rest) = rest.split_once(' ')?;
        let quoted = rest.strip_prefix("paragraph: ")?;
        let text: String = serde_json::from_str(quoted).ok()?;
        let index: u32 = index.parse().ok()?;
        (!text.trim().is_empty()).then_some((index, text))
    })
}
