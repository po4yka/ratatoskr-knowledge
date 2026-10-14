//! The note analysis family and the read-only reader of the notes blob root (changeset OBS-023).
//!
//! A note is one Markdown file of a connected vault. `ratatoskr-notes` owns its bytes; Knowledge
//! reads them from the notes blob root through the read group of that root and never writes there.

use std::path::Path;

use ratatoskr_identifiers::{BlobRef, DigestAlgorithm, TenantRef};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

use crate::GenerationRequest;
use crate::family_analysis::{FamilyValidationError, validate_schema};

const NOTE_SYSTEM: &str = include_str!("../../../prompts/note-analysis.v1/system.txt");
const NOTE_TASK: &str = include_str!("../../../prompts/note-analysis.v1/task.txt");

/// The owner of every note blob, and the owner context of every note source reference.
pub const NOTES_OWNER: &str = "ratatoskr-notes";

/// The most characters of a note that `note_context_v1` sends to the model.
pub const NOTE_CONTEXT_CHARACTERS: usize = 32_000;

/// The characters of a note that become the lead of its search document.
const NOTE_LEAD_CHARACTERS: usize = 280;

/// Strict structured analysis of one note revision.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct NoteAnalysis {
    /// Grounded summary of this note only.
    #[schemars(length(min = 1, max = 2_000))]
    pub summary: String,
    /// Bounded topics that the note text supports.
    #[schemars(length(max = 12))]
    pub topics: Vec<String>,
    /// Exact bounded excerpt from the note text.
    #[schemars(length(min = 1, max = 500))]
    pub evidence_excerpt: String,
}

/// One note revision that a file change names, with its text already read and verified.
#[derive(Debug, Clone)]
pub struct NoteRevision {
    /// The owner of the vault.
    pub tenant: TenantRef,
    /// The stable file identity of `ratatoskr-notes`.
    pub file_id: uuid::Uuid,
    /// The vault path of the file after the change.
    pub path: String,
    /// The bytes of the revision, owned by `ratatoskr-notes`.
    pub blob: BlobRef,
    /// The UTF-8 text of the revision.
    pub text: String,
}

impl NoteRevision {
    /// The search title: the file name without its `.md` suffix.
    #[must_use]
    pub fn title(&self) -> String {
        let name = self.path.rsplit('/').next().unwrap_or(&self.path);
        name.strip_suffix(".md").unwrap_or(name).to_owned()
    }

    /// The search lead: the first characters of the note.
    #[must_use]
    pub fn lead(&self) -> String {
        self.text.chars().take(NOTE_LEAD_CHARACTERS).collect()
    }
}

/// Whether a media type is Markdown, the only media type Knowledge analyses as a note.
#[must_use]
pub fn is_markdown(media_type: &str) -> bool {
    media_type
        .split(';')
        .next()
        .is_some_and(|essence| essence.trim().eq_ignore_ascii_case("text/markdown"))
}

/// Builds deterministic note evidence with a distinct prompt identity.
#[must_use]
pub fn note_context(note: &NoteRevision) -> String {
    format!(
        "family: note\ntitle: {}\nnote_text: {}\n",
        note.title(),
        note.text
    )
}

/// Generates the strict JSON Schema for note analysis.
///
/// # Errors
///
/// Returns a serialization error if the canonical schema cannot be encoded.
pub fn note_analysis_schema() -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(schemars::schema_for!(NoteAnalysis))
}

/// Builds a versioned, provider-neutral note analysis request.
///
/// # Errors
///
/// Returns a serialization error if the canonical output schema cannot be encoded.
pub fn note_generation_request(
    note: &NoteRevision,
) -> Result<GenerationRequest, serde_json::Error> {
    Ok(GenerationRequest {
        prompt_version: "note_prompt_v1".to_owned(),
        system_policy: NOTE_SYSTEM.to_owned(),
        task_instruction: NOTE_TASK.to_owned(),
        output_schema: note_analysis_schema()?,
        source_content: note_context(note),
    })
}

/// Validates and decodes a strict note analysis response.
///
/// # Errors
///
/// Returns [`FamilyValidationError`] for a structural or decoding failure, or an excerpt that is
/// not in the note text.
pub fn validate_note_analysis(
    value: &serde_json::Value,
    text: &str,
) -> Result<NoteAnalysis, FamilyValidationError> {
    validate_schema(
        value,
        &note_analysis_schema().map_err(|_| FamilyValidationError::Schema)?,
    )?;
    let analysis: NoteAnalysis =
        serde_json::from_value(value.clone()).map_err(|_| FamilyValidationError::Decode)?;
    if !text.contains(&analysis.evidence_excerpt) {
        return Err(FamilyValidationError::Citation);
    }
    Ok(analysis)
}

/// A note blob could not be read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NoteBlobError {
    /// The blob is absent, unreadable, or its length or digest differs from the reference. The
    /// notes service can still write it, so a later delivery may succeed.
    #[error("the note blob is not available")]
    Unavailable,
    /// The reference names a blob larger than the configured blob ceiling.
    #[error("the note blob is larger than the blob ceiling")]
    TooLarge,
    /// The bytes are not UTF-8 text.
    #[error("the note is not UTF-8 text")]
    NotUtf8,
    /// The text has more characters than the note context holds.
    #[error("the note is longer than the note context")]
    TooLong,
}

impl NoteBlobError {
    /// Whether a later delivery of the same reference can never succeed.
    #[must_use]
    pub const fn is_permanent(&self) -> bool {
        matches!(self, Self::TooLarge)
    }
}

/// Reads and verifies the bytes that `blob` names under the notes blob root `root`.
///
/// The blob is at `sha256/<first two hex digits>/<other 62 hex digits>`. The file is opened
/// read-only and never written.
///
/// # Errors
///
/// Returns [`NoteBlobError::TooLarge`] when the reference is larger than `ceiling` bytes, and
/// [`NoteBlobError::Unavailable`] when the blob is absent or unreadable, or its length or SHA-256
/// digest differs from the reference.
pub async fn read_note_blob(
    root: &Path,
    blob: &BlobRef,
    ceiling: u64,
) -> Result<Vec<u8>, NoteBlobError> {
    if blob.length_bytes > ceiling {
        return Err(NoteBlobError::TooLarge);
    }
    if blob.owner_service.as_str() != NOTES_OWNER
        || blob.digest.algorithm != DigestAlgorithm::Sha256
    {
        return Err(NoteBlobError::Unavailable);
    }
    let hex = blob.digest.hex.as_str();
    let (Some(prefix), Some(rest)) = (hex.get(..2), hex.get(2..)) else {
        return Err(NoteBlobError::Unavailable);
    };
    let file = tokio::fs::File::open(root.join("sha256").join(prefix).join(rest))
        .await
        .map_err(|_| NoteBlobError::Unavailable)?;
    let mut bytes = Vec::new();
    file.take(blob.length_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| NoteBlobError::Unavailable)?;
    if u64::try_from(bytes.len()).ok() != Some(blob.length_bytes)
        || format!("{:x}", Sha256::digest(&bytes)) != hex
    {
        return Err(NoteBlobError::Unavailable);
    }
    Ok(bytes)
}

/// Reads the note text that `blob` names under the notes blob root `root`.
///
/// # Errors
///
/// Returns the errors of [`read_note_blob`].
pub async fn read_note_text(
    root: &Path,
    blob: &BlobRef,
    ceiling: u64,
) -> Result<String, NoteBlobError> {
    let bytes = read_note_blob(root, blob, ceiling).await?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}
