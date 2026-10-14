//! Ordering and removal of note file changes (changeset OBS-023, workspace spec
//! `notes-analysis-intake`).
//!
//! `knowledge.notes_files` holds one row per file with the highest connection sequence that
//! Knowledge applied and whether that change was a deletion. A change with a lower sequence is not
//! applied, and a deletion keeps its row as the guard against an older change.
//! `knowledge.notes_disconnections` holds one row per removed connection, so a late change of a
//! removed connection creates nothing. Neither table holds a path or any text.

use uuid::Uuid;

use crate::note_analysis::NOTES_OWNER;
use crate::{Database, DeletionScope, PersistenceError, SourceInboxError, execute_deletion};

/// What Knowledge does with one file change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteAdmission {
    /// The change is the current state of its file: analyse it, or remove the file.
    Current,
    /// A later change of the file, or the removal of its connection, was applied first: do
    /// nothing.
    Stale,
}

/// One file change, as the ordering guard sees it.
#[derive(Debug, Clone, Copy)]
pub struct NoteChange<'a> {
    /// The owner of the vault, in its canonical text form.
    pub tenant_ref: &'a str,
    /// The connection of the file.
    pub connection_id: Uuid,
    /// The stable file identity.
    pub file_id: Uuid,
    /// The position of the change among the events of its connection.
    pub connection_sequence: u64,
}

/// Orders note file changes and removes the derived data of removed files and connections.
#[derive(Debug)]
pub struct NotesInbox<'a> {
    database: &'a Database,
}

type Transaction<'t> = sqlx::Transaction<'t, sqlx::Postgres>;

impl<'a> NotesInbox<'a> {
    /// Creates the inbox over Knowledge-owned storage.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Claims a created, updated or renamed file change.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] when the guard cannot be read or written.
    pub async fn accept_change(
        &self,
        change: &NoteChange<'_>,
    ) -> Result<NoteAdmission, SourceInboxError> {
        let mut transaction = self.begin().await?;
        let admission = claim(&mut transaction, change, false).await?;
        commit(transaction).await?;
        Ok(admission)
    }

    /// Claims a deleted file change and removes the derived data of the file in the same
    /// transaction. A redelivery of the deletion removes again, which changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] when the guard or the deletion cannot be written.
    pub async fn accept_deletion(
        &self,
        change: &NoteChange<'_>,
    ) -> Result<NoteAdmission, SourceInboxError> {
        let mut transaction = self.begin().await?;
        let admission = claim(&mut transaction, change, true).await?;
        if admission == NoteAdmission::Current {
            remove_file(&mut transaction, change.tenant_ref, change.file_id).await?;
        }
        commit(transaction).await?;
        Ok(admission)
    }

    /// Records the removal of a connection and removes the derived data of each of its files.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] when the guard or the deletion cannot be written.
    pub async fn accept_disconnection(
        &self,
        tenant_ref: &str,
        connection_id: Uuid,
    ) -> Result<(), SourceInboxError> {
        let mut transaction = self.begin().await?;
        sqlx::query(
            "insert into knowledge.notes_disconnections (tenant_ref, connection_id)
             values ($1, $2) on conflict do nothing",
        )
        .bind(tenant_ref)
        .bind(connection_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let files: Vec<Uuid> = sqlx::query_scalar(
            "delete from knowledge.notes_files where tenant_ref = $1 and connection_id = $2
             returning file_id",
        )
        .bind(tenant_ref)
        .bind(connection_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        for file_id in files {
            remove_file(&mut transaction, tenant_ref, file_id).await?;
        }
        commit(transaction).await
    }

    /// Removes the derived data of a file whose connection was removed while its change was
    /// analysed, so that a late analysis does not outlive the removal.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInboxError`] when the guard or the deletion cannot be read or written.
    pub async fn settle_after_analysis(
        &self,
        change: &NoteChange<'_>,
    ) -> Result<NoteAdmission, SourceInboxError> {
        let mut transaction = self.begin().await?;
        let admission = if connection_removed(&mut transaction, change).await? {
            remove_file(&mut transaction, change.tenant_ref, change.file_id).await?;
            NoteAdmission::Stale
        } else {
            NoteAdmission::Current
        };
        commit(transaction).await?;
        Ok(admission)
    }

    async fn begin(&self) -> Result<Transaction<'static>, SourceInboxError> {
        Ok(self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?)
    }
}

async fn commit(transaction: Transaction<'_>) -> Result<(), SourceInboxError> {
    transaction
        .commit()
        .await
        .map_err(PersistenceError::Query)?;
    Ok(())
}

async fn connection_removed(
    transaction: &mut Transaction<'_>,
    change: &NoteChange<'_>,
) -> Result<bool, SourceInboxError> {
    Ok(sqlx::query_scalar(
        "select exists (select 1 from knowledge.notes_disconnections
                        where tenant_ref = $1 and connection_id = $2)",
    )
    .bind(change.tenant_ref)
    .bind(change.connection_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?)
}

/// Applies `change` to the guard of its file when it is newer than the applied one, and reports
/// whether it is the current state of the file.
async fn claim(
    transaction: &mut Transaction<'_>,
    change: &NoteChange<'_>,
    deleted: bool,
) -> Result<NoteAdmission, SourceInboxError> {
    if connection_removed(transaction, change).await? {
        return Ok(NoteAdmission::Stale);
    }
    let sequence =
        i64::try_from(change.connection_sequence).map_err(|_| PersistenceError::ValueOverflow)?;
    sqlx::query(
        "insert into knowledge.notes_files
             (tenant_ref, file_id, connection_id, connection_sequence, deleted)
         values ($1, $2, $3, $4, $5)
         on conflict (tenant_ref, file_id) do update set
             connection_id = excluded.connection_id,
             connection_sequence = excluded.connection_sequence,
             deleted = excluded.deleted,
             updated_at = now()
         where excluded.connection_sequence > knowledge.notes_files.connection_sequence",
    )
    .bind(change.tenant_ref)
    .bind(change.file_id)
    .bind(change.connection_id)
    .bind(sequence)
    .bind(deleted)
    .execute(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    let (applied, applied_deleted): (i64, bool) = sqlx::query_as(
        "select connection_sequence, deleted from knowledge.notes_files
         where tenant_ref = $1 and file_id = $2",
    )
    .bind(change.tenant_ref)
    .bind(change.file_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(PersistenceError::Query)?;
    Ok(if applied == sequence && applied_deleted == deleted {
        NoteAdmission::Current
    } else {
        NoteAdmission::Stale
    })
}

/// Removes every analysis, search entry and embedding of one file, every revision included.
async fn remove_file(
    transaction: &mut Transaction<'_>,
    tenant_ref: &str,
    file_id: Uuid,
) -> Result<(), SourceInboxError> {
    execute_deletion(
        transaction,
        &DeletionScope::Source {
            tenant_ref: tenant_ref.to_owned(),
            owner_context: NOTES_OWNER.to_owned(),
            source_document_id: file_id.to_string(),
        },
    )
    .await?;
    Ok(())
}
