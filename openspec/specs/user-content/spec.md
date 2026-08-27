# user-content Specification

## Purpose

Defines tenant-scoped organization, state, annotations, and feedback over immutable Knowledge evidence.

## Requirements

### Requirement: Tenant-scoped user content

Knowledge SHALL isolate tags, collections, analysis state, highlights, and feedback by tenant.

#### Scenario: foreign target is hidden

- **WHEN** a tenant addresses another tenant's user-content target
- **THEN** the operation reports the same scoped absence as a missing target

### Requirement: Tags and collections preserve durable organization

Knowledge SHALL enforce tenant-local normalized tag uniqueness, atomically merge tags without duplicate taggings, and keep collection items in stable explicit order.

#### Scenario: tag merge and collection move

- **WHEN** a tenant merges overlapping tags or moves one collection item
- **THEN** duplicate taggings collapse and unaffected collection items retain order

### Requirement: Analysis state and feedback retain immutable evidence

Knowledge SHALL persist idempotent read/favorite state and typed bounded feedback only for tenant-owned accepted analyses without changing evidence.

#### Scenario: repeated state and feedback

- **WHEN** a tenant repeats a state transition or records feedback
- **THEN** one state row remains and the accepted analysis result is unchanged

### Requirement: Highlights are immutable text anchors

Knowledge SHALL validate a highlight against supplied immutable Document IR block text using Unicode-scalar offsets and persist only the anchor metadata.

#### Scenario: invalid anchor is rejected

- **WHEN** a block is absent or an offset falls outside its Unicode-scalar length
- **THEN** no highlight is persisted

### Requirement: Deferred capabilities remain absent

Knowledge SHALL not introduce collaboration, public links, goals, saved searches, legacy import, or highlight rebasing in this capability.

#### Scenario: legacy import is unavailable

- **WHEN** an internal caller seeks a legacy user-content import operation
- **THEN** Knowledge exposes no such operation
