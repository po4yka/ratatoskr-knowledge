# Privacy Deletion Specification

## Purpose

Defines how Knowledge erases every derived trace of one tenant or one logical source - analyses, attempts, outputs, search documents, embedding chunks, failure records, and owned raw-response bytes - atomically, auditable, and verifiably, exceeding purge-and-reconcile erasure guarantees.

## ADDED Requirements

### Requirement: Deleting a tenant removes every derived row

A delete-by-tenant operation SHALL remove, in one database transaction, every `source_refs` revision of that tenant together with all dependent `analysis_runs`, `analysis_attempts`, `analysis_outputs`, `search_projection_inputs`, `search_documents`, `embedding_chunks`, and `embedding_failures` rows, and SHALL leave every other tenant's rows untouched.

#### Scenario: a seeded tenant disappears whole while a survivor stays intact

- **WHEN** a database holds sources for two tenants where each tenant's sources carry accepted indexed runs, invalid attempts with stored raw responses, search documents, embedding chunks under more than one model identity, and failure entries, and the delete-by-tenant operation runs for one tenant
- **THEN** a count query returns zero rows in each of the eight linked tables for the deleted tenant's identifiers, and the survivor tenant retains exactly the rows it held before, including its projection inputs, search documents, chunks, and failure entries

### Requirement: Deleting a source removes every revision of the document

A delete-by-source operation scoped by tenant, owner context, and source document identifier SHALL remove every immutable revision of that document - not only the latest digest - together with all derived rows of every revision, and SHALL NOT touch other documents of the same tenant.

#### Scenario: all digest revisions of one document vanish while sibling documents survive

- **WHEN** one logical document exists under two distinct content digests with separate runs, outputs, and vectors, a sibling document of the same tenant also carries derived rows, and the delete-by-source operation runs for the first document
- **THEN** no rows referencing either revision of the deleted document remain in any linked table, and the sibling document's rows are unchanged

### Requirement: Row deletion and its audit record commit together

The system SHALL execute all row deletions and the insertion of one `deletion_records` audit row inside a single transaction, so an audit row exists if and only if the deletion committed. The audit row SHALL record the scope kind, tenant, optional owner context and source document identifier, per-table deleted row counts, the removed blob digest count, and the completion time. Until the transaction commits, a separate connection SHALL still observe all pre-existing rows and no audit row.

#### Scenario: a concurrent reader sees all-or-nothing visibility

- **WHEN** the delete operation has executed its statements inside an open but uncommitted transaction and a second connection counts the target rows and audit rows
- **THEN** the second connection observes the original row counts and zero audit rows before commit, and zero target rows plus exactly one audit row with matching per-table counts after commit

#### Scenario: the receipt equals independently counted facts

- **WHEN** a delete operation completes and returns its receipt
- **THEN** each per-table count in the receipt equals the number of rows that a count query run immediately beforehand had reported for that scope, and the persisted audit row equals the receipt values

### Requirement: Owned raw-response bytes are collected without harming survivors

After the row-deletion transaction commits, the system SHALL remove each stored raw-response blob file whose digest is no longer referenced by any remaining `analysis_attempts` or `analysis_outputs` row, SHALL keep every blob still referenced by any surviving row, and SHALL never remove bytes addressed by `source_refs.source_blob`, which are owned by the source-owning service. Removal SHALL be idempotent when the file is already absent.

#### Scenario: a private blob dies with its source while a shared digest survives

- **WHEN** one source's attempts reference a blob digest that no other row uses, another source's output shares a byte-identical digest with one of the deleted source's rows, and the first source is deleted
- **THEN** the privately-referenced blob file is gone from the blob root afterwards, the shared-digest file remains readable, and both survivor rows still resolve their stored references successfully

#### Scenario: external provenance references are never treated as owned bytes

- **WHEN** deleted sources carried `source_blob` references whose owning service differs from knowledge
- **THEN** the deletion reports them as out of scope, performs no file removal for those digests, and the blob root contains no path addressing them

### Requirement: Deletion is verifiable against an enumerated inventory

The system SHALL verify after each deletion that zero rows remain for the deleted scope in `source_refs`, `analysis_runs`, `analysis_attempts`, `analysis_outputs`, `search_projection_inputs`, `search_documents`, `embedding_chunks`, and `embedding_failures`, and SHALL fail loudly rather than report success if any row survives. A test SHALL seed every row kind above plus referenced blob files, run the deletion, and enumerate each location in its assertions.

#### Scenario: the exhaustive inventory assertion passes after deletion

- **WHEN** the enumeration test seeds all seven tables for the scope plus attempt-referenced and output-referenced blob files, executes the deletion, and asserts emptiness location by location
- **THEN** every assertion holds and the operation is reported successful

### Requirement: The aggregate spend ledger survives deletion

Delete operations SHALL NOT modify `provider_usage` rows: the ledger carries no tenant or source attribution, backs durable budget ceilings, and records spend evidence that must outlive content erasure.

#### Scenario: usage rows outlive a tenant deletion

- **WHEN** provider usage rows exist from requests made for a tenant's analyses and the tenant is deleted
- **THEN** all usage rows remain present with identical values after the deletion

### Requirement: Repeated deletion is safe

Running a delete operation for a scope with no remaining rows SHALL succeed, change nothing except recording its audit row with zero counts, and return a zero-count receipt.

#### Scenario: deleting an already-deleted tenant is a quiet no-op

- **WHEN** a completed deletion runs a second time for the same tenant
- **THEN** the operation exits successfully, every count in the receipt is zero, no data changed, and a second audit row with zero counts is recorded

### Requirement: Orphaned blobs are reclaimed by later deletions

If a process stops between committing row deletion and removing blob files, leaving unreferenced files in the blob root, a subsequent delete operation SHALL detect and remove such unreferenced digests before processing its own scope, so the crash window self-heals without manual tooling.

#### Scenario: a leftover orphan from an interrupted removal is swept later

- **WHEN** a blob file exists whose digest is referenced by no database row, and any delete operation runs next
- **THEN** the sweep phase removes that file, and the operation's report accounts for it separately from its own scope's removals
