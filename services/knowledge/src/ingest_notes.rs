//! Handlers of the two notes durables (changeset OBS-023, workspace spec `notes-analysis-intake`).
//!
//! A file change is itself the analysis request: there is no command channel from
//! `ratatoskr-notes` into Knowledge. One run exists for each `(file_id, revision)`, so a
//! redelivery resumes that run instead of creating another one. A deleted file and a removed
//! connection remove the derived data, and the guard of each file keeps an older change from
//! creating it again.

use ratatoskr_event_envelope::{EventEnvelope, EventPayload as _};
use ratatoskr_identifiers::TenantRef;
use ratatoskr_knowledge::{
    LlmProvider, NoteAdmission, NoteBlobError, NoteChange, NoteRevision, NotesInbox,
    collect_pending_blobs, is_markdown, read_note_text,
};
use ratatoskr_notes_contracts::{FileChangeKind, FileChanged, VaultConnected, VaultDisconnected};

use crate::ingest::IngestContext;
use crate::ingest_handlers::{Outcome, analysis_failure, decode, inbox_failure};

const NOTES_PRODUCERS: &[&str] = &["ratatoskr-notes"];

/// The class of a note whose bytes cannot be read yet: absent, unreadable, of another length or
/// digest, or under a root that is not configured. The notes service can still write them.
const BLOB_UNAVAILABLE: &str = "notes_blob";

/// The class of a removal that failed and will be applied by a later delivery.
const REMOVAL: &str = "notes_removal";

impl<P: LlmProvider> IngestContext<P> {
    /// `notes.file.changed.v1`: analyse a Markdown revision once, remove a deleted file, and
    /// acknowledge anything else.
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
        let tenant_ref = tenant.to_string();
        let guard = NoteChange {
            tenant_ref: &tenant_ref,
            connection_id: change.connection_id.0,
            file_id: change.file_id.0,
            connection_sequence: change.connection_sequence,
        };
        let inbox = NotesInbox::new(&self.database);
        match change.change_kind {
            FileChangeKind::Created | FileChangeKind::Updated | FileChangeKind::Renamed => {}
            FileChangeKind::Deleted => {
                return match inbox.accept_deletion(&guard).await {
                    Ok(_) => self.erase_removed_blobs().await,
                    Err(error) => inbox_failure(&error, REMOVAL),
                };
            }
            _ => return Outcome::Reject("change_kind"),
        }
        match inbox.accept_change(&guard).await {
            Ok(NoteAdmission::Current) => {}
            // A stale change of a removed connection may have removed what a missed removal
            // left, so its queued blobs are erased too.
            Ok(NoteAdmission::Stale) => return self.erase_removed_blobs().await,
            Err(error) => return inbox_failure(&error, "notes_inbox"),
        }
        if !is_markdown(change.media_type.as_str()) {
            // A note renamed to another media type leaves the search.
            return match inbox.remove_derived(&guard).await {
                Ok(()) => self.erase_removed_blobs().await,
                Err(error) => inbox_failure(&error, REMOVAL),
            };
        }
        let outcome = self.analyse(tenant, change, &guard).await;
        if outcome != Outcome::Done {
            return outcome;
        }
        // The connection may have been removed while the note was analysed: the removal then
        // missed this analysis, so remove it here.
        match inbox.settle_after_analysis(&guard).await {
            Ok(NoteAdmission::Current) => Outcome::Done,
            Ok(NoteAdmission::Stale) => self.erase_removed_blobs().await,
            Err(error) => inbox_failure(&error, REMOVAL),
        }
    }

    /// Reads the bytes of a Markdown revision and runs its one analysis.
    async fn analyse(
        &self,
        tenant: TenantRef,
        change: FileChanged,
        guard: &NoteChange<'_>,
    ) -> Outcome {
        let Some(blob) = change.blob else {
            return Outcome::Reject("payload");
        };
        let Some(root) = self.config.blobs.notes_root.as_deref() else {
            return Outcome::Retry(BLOB_UNAVAILABLE);
        };
        let text = match read_note_text(root, &blob, self.config.limits.blob_bytes).await {
            Ok(text) => text,
            Err(NoteBlobError::TooLarge) => {
                return self.reject_revision(guard, "note_too_large").await;
            }
            Err(NoteBlobError::NotUtf8) => {
                return self.reject_revision(guard, "note_encoding").await;
            }
            Err(NoteBlobError::TooLong) => {
                return self.reject_revision(guard, "note_too_long").await;
            }
            Err(_) => return Outcome::Retry(BLOB_UNAVAILABLE),
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

    /// Rejects a revision that can never be analysed. The guard already holds it as the current
    /// change of its file, so the derived data of every earlier revision is removed and its text
    /// does not stay searchable. A redelivery removes again, which changes nothing.
    async fn reject_revision(&self, guard: &NoteChange<'_>, class: &'static str) -> Outcome {
        match NotesInbox::new(&self.database).remove_derived(guard).await {
            Ok(()) => match self.erase_removed_blobs().await {
                Outcome::Done => Outcome::Reject(class),
                failure => failure,
            },
            Err(error) => inbox_failure(&error, REMOVAL),
        }
    }

    /// `notes.vault.*`: a connected vault needs no work, because its files arrive as file
    /// changes; a removed vault loses the derived data of every file.
    pub(crate) async fn notes_vaults(&self, bytes: &[u8]) -> Outcome {
        let (envelope, tenant) = match decode(bytes, NOTES_PRODUCERS) {
            Ok(decoded) => decoded,
            Err(outcome) => return outcome,
        };
        match envelope.event_type.to_wire().as_str() {
            VaultConnected::EVENT_TYPE => match envelope.payload_as::<VaultConnected>() {
                Ok(_) => Outcome::Done,
                Err(_) => Outcome::Reject("payload"),
            },
            VaultDisconnected::EVENT_TYPE => self.disconnected(&envelope, &tenant).await,
            _ => Outcome::Reject("type"),
        }
    }

    async fn disconnected(&self, envelope: &EventEnvelope, tenant: &TenantRef) -> Outcome {
        let Ok(disconnected) = envelope.payload_as::<VaultDisconnected>() else {
            return Outcome::Reject("payload");
        };
        match NotesInbox::new(&self.database)
            .accept_disconnection(&tenant.to_string(), disconnected.connection_id.0)
            .await
        {
            Ok(()) => self.erase_removed_blobs().await,
            Err(error) => inbox_failure(&error, REMOVAL),
        }
    }

    /// Erases the Knowledge blobs that a committed removal queued. A failed erasure is finished
    /// by the redelivery, which drains the queue again.
    async fn erase_removed_blobs(&self) -> Outcome {
        match collect_pending_blobs(&self.database, &self.blobs).await {
            Ok(_) => Outcome::Done,
            Err(error) if error.is_permanent() => Outcome::Reject("unprocessable"),
            Err(_) => Outcome::Retry(REMOVAL),
        }
    }
}
