# Privacy Deletion Specification

## Purpose

Defines complete, auditable erasure of Knowledge-owned derived material for one tenant or logical source while preserving unrelated data and external source ownership.

## Requirements

### Requirement: Deleting a tenant removes every derived row

A delete-by-tenant operation SHALL remove in one transaction the tenant's `source_refs`, `analysis_runs`, `analysis_attempts`, `analysis_outputs`, `search_projection_inputs`, `search_documents`, `embedding_chunks`, and `embedding_failures`, while leaving other tenants untouched.

#### Scenario: a seeded tenant disappears while a survivor stays intact

- **WHEN** two tenants each hold fully derived source data and one is deleted
- **THEN** all eight linked row kinds are absent for that tenant and unchanged for the survivor

### Requirement: Deleting a source removes every revision of the document

A delete-by-source operation scoped by tenant, owner context, and source document identifier SHALL remove every immutable revision and its derived rows without changing sibling sources.

#### Scenario: all revisions vanish while a sibling survives

- **WHEN** a logical document has multiple content digests and a sibling source exists
- **THEN** all rows for each selected revision are gone and the sibling rows remain unchanged

### Requirement: Row deletion and audit commit together

The deletion and insertion of one `deletion_records` row SHALL occur in the same transaction. The audit SHALL include scope, identifiers, per-table counts, removed blob count, and completion time.

#### Scenario: a concurrent reader sees all-or-nothing visibility

- **WHEN** deletion statements have run but their transaction has not committed
- **THEN** another connection sees the original rows and no audit, then sees zero target rows and one matching audit after commit

### Requirement: Owned response bytes are reference-safe

After commit, the system SHALL remove only unreferenced owned raw-response files, retain shared digests still referenced by surviving attempts or outputs, and never remove externally owned `source_blob` bytes.

#### Scenario: private and shared blobs diverge safely

- **WHEN** deletion includes one private response blob, one shared response blob, and an external source blob reference
- **THEN** only the private response file is removed, the shared file remains readable, and the external reference is reported without file removal

### Requirement: Deletion is verified and repeatable

The system SHALL verify no selected row remains in every linked table and SHALL return an audit-backed zero-count receipt when a deletion scope is run again. Later deletion executions SHALL reclaim leftover unreferenced owned blob files separately from scope removals.

#### Scenario: exhaustive inventory and rerun succeed

- **WHEN** all linked row kinds and blob references are seeded, deleted, and the same deletion runs again
- **THEN** the inventory is empty, the first receipt equals its audit, and the second receipt and audit have zero row counts

### Requirement: The aggregate spend ledger survives deletion

Delete operations SHALL NOT modify `provider_usage` because its durable aggregate budget evidence lacks tenant/source attribution.

#### Scenario: usage rows outlive tenant erasure

- **WHEN** provider usage exists before a tenant deletion
- **THEN** the ledger rows retain identical values after the deletion
