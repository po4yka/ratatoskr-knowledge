//! Handlers of the two notes durables (changeset OBS-023, workspace spec `notes-analysis-intake`).
//!
//! A file change is itself the analysis request: there is no command channel from
//! `ratatoskr-notes` into Knowledge. One run exists for each `(file_id, revision)`, so a
//! redelivery resumes that run instead of creating another one.

use ratatoskr_event_envelope::EventPayload as _;
use ratatoskr_knowledge::{LlmProvider, NoteRevision, is_markdown, read_note_blob};
use ratatoskr_notes_contracts::{FileChangeKind, FileChanged, VaultConnected, VaultDisconnected};

use crate::ingest::IngestContext;
use crate::ingest_handlers::{Outcome, analysis_failure, decode};

const NOTES_PRODUCERS: &[&str] = &["ratatoskr-notes"];

/// The class of a note whose bytes cannot be read yet: absent, unreadable, of another length or
/// digest, or under a root that is not configured. The notes service can still write them.
const BLOB_UNAVAILABLE: &str = "notes_blob";

impl<P: LlmProvider> IngestContext<P> {
    /// `notes.file.changed.v1`: analyse a Markdown revision once; acknowledge anything else.
    pub(crate) async fn notes_files(&self, bytes: &[u8]) -> Outcome {
        let (envelope, tenant) = match decode(bytes, NOTES_PRODUCERS) {
            Ok(decoded) => decoded,
            Err(outcome) => return outcome,
        };
        if envelope.event_type.to_wire() != FileChanged::EVENT_TYPE {
            return Outcome::Reject("type");
        }
        let Ok(change) = envelope.payload_as::<FileChanged>() else {
            return Outcome::Reject("payload");
        };
        match change.change_kind {
            FileChangeKind::Created | FileChangeKind::Updated | FileChangeKind::Renamed => {}
            _ => return Outcome::Done,
        }
        if !is_markdown(change.media_type.as_str()) {
            return Outcome::Done;
        }
        let Some(blob) = change.blob else {
            return Outcome::Reject("payload");
        };
        let Some(root) = self.config.blobs.notes_root.as_deref() else {
            return Outcome::Retry(BLOB_UNAVAILABLE);
        };
        let bytes = match read_note_blob(root, &blob, self.config.limits.blob_bytes).await {
            Ok(bytes) => bytes,
            Err(error) if error.is_permanent() => return Outcome::Reject("note_too_large"),
            Err(_) => return Outcome::Retry(BLOB_UNAVAILABLE),
        };
        let Ok(text) = String::from_utf8(bytes) else {
            return Outcome::Reject("note_encoding");
        };
        let note = NoteRevision {
            tenant,
            file_id: change.file_id.0,
            path: change.path.as_str().to_owned(),
            blob,
            text,
        };
        match self.pipeline().execute_note(&note).await {
            Ok(_) => Outcome::Done,
            Err(error) => analysis_failure(&error, "note_analysis"),
        }
    }
}

/// `notes.vault.*`: a connected vault needs no work; its files arrive as file changes.
pub(crate) fn notes_vaults(bytes: &[u8]) -> Outcome {
    let (envelope, _tenant) = match decode(bytes, NOTES_PRODUCERS) {
        Ok(decoded) => decoded,
        Err(outcome) => return outcome,
    };
    let valid = match envelope.event_type.to_wire().as_str() {
        VaultConnected::EVENT_TYPE => envelope.payload_as::<VaultConnected>().is_ok(),
        VaultDisconnected::EVENT_TYPE => envelope.payload_as::<VaultDisconnected>().is_ok(),
        _ => return Outcome::Reject("type"),
    };
    if valid {
        Outcome::Done
    } else {
        Outcome::Reject("payload")
    }
}
