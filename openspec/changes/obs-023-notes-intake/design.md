## Context

The workspace change `add-notes-vault-connections` is the contract (design D9 and D12, spec
`notes-analysis-intake`). This file records the choices local to Knowledge.

## Decisions

### The run identity is the inbox key

A note revision is registered as a source reference with the owner context `ratatoskr-notes`, the
document id `file_id` and the content digest `revision`, and the source blob is the `BlobRef` of the
event. The source reference identity and the run identity are unique, so a redelivery of the same
`(file_id, revision)` resumes the one run and never creates a second one. Knowledge stores no copy
of the note bytes: the source blob belongs to `ratatoskr-notes`, and the deletion path reports such
a blob as external and never erases it.

### Order by connection sequence, guard by a head row

`knowledge.notes_files` holds one row per file: the owner, the connection, the highest connection
sequence that Knowledge applied and whether that change was a deletion. A change with a lower or
equal sequence than the row is not applied (an equal sequence is a redelivery and resumes the run).
A deletion keeps its row as the guard against an older event. The row holds no path and no text.

`knowledge.notes_disconnections` holds one row per removed connection. A file event of a removed
connection creates nothing. Connection ids are not reused, so the row is final.

### One deletion path

A deleted file and a removed connection use `execute_deletion` with the source scope of each file,
in the transaction that writes the guard. The rows, the queued Knowledge blobs and the audit rows
commit together, and the erasure queue is drained after the commit, as for a social removal.

### Bytes are read, never trusted

The reader resolves `sha256/<2 hex>/<62 hex>` under the configured root. It refuses a blob larger
than `RATATOSKR__LIMITS__BLOB_BYTES` before it reads it, and it checks the length and the SHA-256
digest after it reads it. A missing blob, a wrong length and a wrong digest are one transient class,
`notes_blob`, because the notes service can still write the blob. A root that is not configured is
the same class.

### The note analysis family

`note_analysis_v1` has a summary, at most 12 topics and one exact excerpt from the note. The
validator refuses an excerpt that is not in the note text. The search title is the file name
without the `.md` suffix, the lead is the first 280 characters of the note and the body is the note
text.

## Risks / Trade-offs

- An updated note keeps the search entry of its earlier revision until a deletion, because the
  supersession of search projections reads `source_analysis_heads`, which holds only the social and
  AI-archive families. A follow-up change adds the notes head to that rule.
- A tenant deletion does not yet remove the rows of `knowledge.notes_files` and
  `knowledge.notes_disconnections`. They hold identifiers and digests, no text.
