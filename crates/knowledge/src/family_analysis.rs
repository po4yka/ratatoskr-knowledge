//! Family-specific contracts and deterministic contexts for social and AI-archive analysis.

use ratatoskr_ai_archive_contracts::{AiAuthorRole, AiContentPart, AiConversation, AiProject};
use ratatoskr_github_contracts::{RepositoryAnalysisAttributes, RepositoryAnalysisRequested};
use ratatoskr_social_contracts::SocialSourceSnapshot;

use crate::GenerationRequest;

const SOCIAL_SYSTEM: &str = include_str!("../../../prompts/social-analysis.v1/system.txt");
const SOCIAL_TASK: &str = include_str!("../../../prompts/social-analysis.v1/task.txt");
const ARCHIVE_SYSTEM: &str = include_str!("../../../prompts/archive-analysis.v1/system.txt");
const ARCHIVE_TASK: &str = include_str!("../../../prompts/archive-analysis.v1/task.txt");
const PROJECT_SYSTEM: &str =
    include_str!("../../../prompts/archive-project-analysis.v1/system.txt");
const PROJECT_TASK: &str = include_str!("../../../prompts/archive-project-analysis.v1/task.txt");
const REPOSITORY_SYSTEM: &str = include_str!("../../../prompts/repository-analysis.v1/system.txt");
const REPOSITORY_TASK: &str = include_str!("../../../prompts/repository-analysis.v1/task.txt");

/// Strict structured analysis of one repository revision.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct RepositoryAnalysis {
    /// Grounded explanation of the repository's purpose and current shape.
    #[schemars(length(min = 1, max = 2_000))]
    pub summary: String,
    /// Bounded technologies or domains supported by metadata or README evidence.
    #[schemars(length(max = 12))]
    pub topics: Vec<String>,
    /// Exact bounded excerpt from README or supplied metadata grounding the interpretation.
    #[schemars(length(min = 1, max = 500))]
    pub evidence_excerpt: String,
    /// Whether README evidence was available for this revision.
    pub readme_evidence: RepositoryReadmeEvidence,
}

/// README-evidence vocabulary for repository interpretation.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryReadmeEvidence {
    /// The supplied README was available to the analysis worker.
    Present,
    /// Analysis was necessarily limited to repository metadata.
    Absent,
}

/// Strict structured analysis of one normalized social source.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct SocialAnalysis {
    /// Grounded summary of this post only; linked material is not source evidence.
    #[schemars(length(min = 1, max = 2_000))]
    pub summary: String,
    /// Bounded topics explicitly grounded in the normalized post.
    #[schemars(length(max = 12))]
    pub topics: Vec<String>,
    /// Exact bounded excerpt from the normalized post itself.
    #[schemars(length(min = 1, max = 500))]
    pub evidence_excerpt: String,
    /// Whether the post itself contains enough text for a confident interpretation.
    pub confidence: SocialConfidence,
}

/// Confidence vocabulary for social-source interpretations.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SocialConfidence {
    /// The normalized post text and context support the interpretation.
    Grounded,
    /// Capture gaps or media-only content leave important uncertainty.
    Limited,
}

/// Strict structured analysis of one archived conversation.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ArchiveAnalysis {
    /// Grounded conversation-level summary.
    #[schemars(length(min = 1, max = 2_000))]
    pub summary: String,
    /// Message identities that ground the conversation-level summary.
    #[schemars(length(min = 1, max = 20))]
    pub summary_message_ids: Vec<String>,
    /// Decisions extracted only where a user or assistant message contains them.
    #[schemars(length(max = 20))]
    pub decisions: Vec<ArchiveDecision>,
}

/// One extracted decision with its exact provider message identity.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ArchiveDecision {
    /// Bounded decision statement.
    #[schemars(length(min = 1, max = 500))]
    pub text: String,
    /// Provider message identifier from the supplied conversation only.
    #[schemars(length(min = 1, max = 256))]
    pub message_id: String,
}

/// Strict structured analysis of one archived project revision.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ArchiveProjectAnalysis {
    /// Grounded project-level summary.
    #[schemars(length(min = 1, max = 2_000))]
    pub summary: String,
    /// Bounded topics explicitly grounded in project metadata.
    #[schemars(length(max = 12))]
    pub topics: Vec<String>,
    /// Exact bounded excerpt from title, description, or instructions.
    #[schemars(length(min = 1, max = 500))]
    pub evidence_excerpt: String,
}

/// Builds deterministic social evidence with a distinct prompt identity.
#[must_use]
pub fn social_context(snapshot: &SocialSourceSnapshot) -> String {
    format!(
        "family: social\nplatform: {}\nsource_id: {}\npost_text: {}\n",
        snapshot.platform.as_str(),
        snapshot.social_source_id,
        snapshot.text.as_ref().map_or("", |text| text.as_str()),
    )
}

/// Builds deterministic archive evidence preserving message identities and role boundaries.
#[must_use]
pub fn archive_context(conversation: &AiConversation) -> String {
    let mut result = format!(
        "family: ai_archive\nconversation_id: {}\n",
        conversation.ai_conversation_id
    );
    for message in &conversation.messages {
        result.push_str("message_id: ");
        result.push_str(message.external_message_id.as_str());
        result.push_str("\nrole: ");
        result.push_str(match message.author_role {
            AiAuthorRole::User => "user",
            AiAuthorRole::Assistant => "assistant",
            AiAuthorRole::System => "system",
            AiAuthorRole::Tool => "tool",
            _ => "unknown",
        });
        result.push('\n');
        for part in &message.parts {
            match part {
                AiContentPart::Text { text } => {
                    result.push_str("text: ");
                    result.push_str(text.as_str());
                    result.push('\n');
                }
                AiContentPart::Markdown { markdown } => {
                    result.push_str("markdown: ");
                    result.push_str(markdown.as_str());
                    result.push('\n');
                }
                _ => {}
            }
        }
    }
    result
}

/// Builds deterministic project evidence from normalized provider-authored fields.
#[must_use]
pub fn archive_project_context(project: &AiProject) -> String {
    format!(
        "family: ai_archive_project\nproject_id: {}\ntitle: {}\ndescription: {}\ninstructions: {}\n",
        project.ai_project_id,
        project.title.as_str(),
        project
            .description
            .as_ref()
            .map_or("", |value| value.as_str()),
        project
            .instructions
            .as_ref()
            .map_or("", |value| value.as_str()),
    )
}

/// Builds deterministic repository evidence from the requested metadata and acquired README.
#[must_use]
pub fn repository_context(request: &RepositoryAnalysisRequested, readme: Option<&str>) -> String {
    let RepositoryAnalysisAttributes {
        repository_full_name,
        description,
        primary_language,
    } = &request.repository_attributes;
    format!(
        "family: repository\nrepository_id: {}\nfull_name: {}\ndescription: {}\nprimary_language: {}\nREADME_BEGIN\n{}\nREADME_END\n",
        request.repository_id,
        repository_full_name,
        description.as_ref().map_or("", |value| value.as_str()),
        primary_language.as_ref().map_or("", |value| value.as_str()),
        readme.unwrap_or(""),
    )
}

/// Generates the strict JSON Schema for repository analysis.
///
/// # Errors
///
/// Returns a serialization error if the canonical schema cannot be encoded.
pub fn repository_analysis_schema() -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(schemars::schema_for!(RepositoryAnalysis))
}

/// Builds a versioned, provider-neutral repository analysis request.
///
/// # Errors
///
/// Returns a serialization error if the canonical output schema cannot be encoded.
pub fn repository_generation_request(
    request: &RepositoryAnalysisRequested,
    readme: Option<&str>,
) -> Result<GenerationRequest, serde_json::Error> {
    Ok(GenerationRequest {
        prompt_version: "repository_prompt_v1".to_owned(),
        system_policy: REPOSITORY_SYSTEM.to_owned(),
        task_instruction: REPOSITORY_TASK.to_owned(),
        output_schema: repository_analysis_schema()?,
        source_content: repository_context(request, readme),
    })
}

/// Validates and decodes a strict repository analysis response.
///
/// # Errors
///
/// Returns [`FamilyValidationError`] for structural, decoding, evidence, or grounding failure.
pub fn validate_repository_analysis(
    value: &serde_json::Value,
    request: &RepositoryAnalysisRequested,
    readme: Option<&str>,
) -> Result<RepositoryAnalysis, FamilyValidationError> {
    validate_schema(
        value,
        &repository_analysis_schema().map_err(|_| FamilyValidationError::Schema)?,
    )?;
    let analysis: RepositoryAnalysis =
        serde_json::from_value(value.clone()).map_err(|_| FamilyValidationError::Decode)?;
    let expected = if readme.is_some() {
        RepositoryReadmeEvidence::Present
    } else {
        RepositoryReadmeEvidence::Absent
    };
    if analysis.readme_evidence != expected {
        return Err(FamilyValidationError::Evidence);
    }
    let metadata_matches = [
        request.repository_attributes.repository_full_name.as_str(),
        request
            .repository_attributes
            .description
            .as_ref()
            .map_or("", |value| value.as_str()),
        request
            .repository_attributes
            .primary_language
            .as_ref()
            .map_or("", |value| value.as_str()),
    ]
    .into_iter()
    .any(|value| !value.is_empty() && value.contains(&analysis.evidence_excerpt));
    if !readme.is_some_and(|text| text.contains(&analysis.evidence_excerpt)) && !metadata_matches {
        return Err(FamilyValidationError::Citation);
    }
    Ok(analysis)
}

/// Generates the strict JSON Schema for social analysis.
///
/// # Errors
///
/// Returns a serialization error if the canonical schema cannot be encoded.
pub fn social_analysis_schema() -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(schemars::schema_for!(SocialAnalysis))
}

/// Builds a versioned, provider-neutral social analysis request.
///
/// # Errors
///
/// Returns a serialization error if the canonical output schema cannot be encoded.
pub fn social_generation_request(
    snapshot: &SocialSourceSnapshot,
) -> Result<GenerationRequest, serde_json::Error> {
    Ok(GenerationRequest {
        prompt_version: "social_prompt_v1".to_owned(),
        system_policy: SOCIAL_SYSTEM.to_owned(),
        task_instruction: SOCIAL_TASK.to_owned(),
        output_schema: social_analysis_schema()?,
        source_content: social_context(snapshot),
    })
}

/// Validates and decodes a strict social analysis response.
///
/// # Errors
///
/// Returns [`FamilyValidationError`] for structural, decoding, or post-grounding failure.
pub fn validate_social_analysis(
    value: &serde_json::Value,
    snapshot: &SocialSourceSnapshot,
) -> Result<SocialAnalysis, FamilyValidationError> {
    validate_schema(
        value,
        &social_analysis_schema().map_err(|_| FamilyValidationError::Schema)?,
    )?;
    let analysis: SocialAnalysis =
        serde_json::from_value(value.clone()).map_err(|_| FamilyValidationError::Decode)?;
    if !snapshot
        .text
        .as_ref()
        .is_some_and(|text| text.as_str().contains(&analysis.evidence_excerpt))
    {
        return Err(FamilyValidationError::Citation);
    }
    Ok(analysis)
}

/// Generates the strict JSON Schema for archive analysis.
///
/// # Errors
///
/// Returns a serialization error if the canonical schema cannot be encoded.
pub fn archive_analysis_schema() -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(schemars::schema_for!(ArchiveAnalysis))
}

/// Builds a versioned, provider-neutral archive analysis request.
///
/// # Errors
///
/// Returns a serialization error if the canonical output schema cannot be encoded.
pub fn archive_generation_request(
    conversation: &AiConversation,
) -> Result<GenerationRequest, serde_json::Error> {
    Ok(GenerationRequest {
        prompt_version: "archive_prompt_v1".to_owned(),
        system_policy: ARCHIVE_SYSTEM.to_owned(),
        task_instruction: ARCHIVE_TASK.to_owned(),
        output_schema: archive_analysis_schema()?,
        source_content: archive_context(conversation),
    })
}

/// Generates the strict JSON Schema for archived-project analysis.
///
/// # Errors
///
/// Returns a serialization error if the canonical schema cannot be encoded.
pub fn archive_project_analysis_schema() -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(schemars::schema_for!(ArchiveProjectAnalysis))
}

/// Builds a versioned, provider-neutral archived-project analysis request.
///
/// # Errors
///
/// Returns a serialization error if the canonical output schema cannot be encoded.
pub fn archive_project_generation_request(
    project: &AiProject,
) -> Result<GenerationRequest, serde_json::Error> {
    Ok(GenerationRequest {
        prompt_version: "archive_project_prompt_v1".to_owned(),
        system_policy: PROJECT_SYSTEM.to_owned(),
        task_instruction: PROJECT_TASK.to_owned(),
        output_schema: archive_project_analysis_schema()?,
        source_content: archive_project_context(project),
    })
}

/// Validates and decodes an archived-project response against project metadata.
///
/// # Errors
///
/// Returns [`FamilyValidationError`] for structural, decoding, or grounding failure.
pub fn validate_archive_project_analysis(
    value: &serde_json::Value,
    project: &AiProject,
) -> Result<ArchiveProjectAnalysis, FamilyValidationError> {
    validate_schema(
        value,
        &archive_project_analysis_schema().map_err(|_| FamilyValidationError::Schema)?,
    )?;
    let analysis: ArchiveProjectAnalysis =
        serde_json::from_value(value.clone()).map_err(|_| FamilyValidationError::Decode)?;
    let metadata = [
        project.title.as_str(),
        project
            .description
            .as_ref()
            .map_or("", |value| value.as_str()),
        project
            .instructions
            .as_ref()
            .map_or("", |value| value.as_str()),
    ];
    if !metadata
        .into_iter()
        .any(|text| text.contains(&analysis.evidence_excerpt))
    {
        return Err(FamilyValidationError::Citation);
    }
    Ok(analysis)
}

/// Validates and decodes an archive response, including message-level grounding.
///
/// # Errors
///
/// Returns [`FamilyValidationError`] for structural, decoding, or message-grounding failure.
pub fn validate_archive_analysis(
    value: &serde_json::Value,
    conversation: &AiConversation,
) -> Result<ArchiveAnalysis, FamilyValidationError> {
    validate_schema(
        value,
        &archive_analysis_schema().map_err(|_| FamilyValidationError::Schema)?,
    )?;
    let analysis: ArchiveAnalysis =
        serde_json::from_value(value.clone()).map_err(|_| FamilyValidationError::Decode)?;
    for decision in &analysis.decisions {
        if !conversation
            .messages
            .iter()
            .any(|message| message.external_message_id.as_str() == decision.message_id)
        {
            return Err(FamilyValidationError::Citation);
        }
    }
    if analysis.summary_message_ids.iter().any(|message_id| {
        !conversation
            .messages
            .iter()
            .any(|message| message.external_message_id.as_str() == message_id)
    }) {
        return Err(FamilyValidationError::Citation);
    }
    Ok(analysis)
}

/// Validation failure for a family-specific model response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FamilyValidationError {
    /// The generated JSON Schema could not be compiled.
    #[error("the family schema is invalid")]
    Schema,
    /// The response does not satisfy the generated JSON Schema.
    #[error("the family response violates its schema")]
    Structural,
    /// The response could not be decoded after structural validation.
    #[error("the family response could not be decoded")]
    Decode,
    /// An archive decision cites a message absent from the supplied context.
    #[error("the archive response cites an absent message")]
    Citation,
    /// The result claims a source-evidence state different from the supplied source.
    #[error("the family response claims unavailable evidence")]
    Evidence,
}

fn validate_schema(
    value: &serde_json::Value,
    schema: &serde_json::Value,
) -> Result<(), FamilyValidationError> {
    let validator = jsonschema::options()
        .should_validate_formats(true)
        .build(schema)
        .map_err(|_| FamilyValidationError::Schema)?;
    validator
        .validate(value)
        .map_err(|_| FamilyValidationError::Structural)
}
