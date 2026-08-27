## Purpose

Creates reproducible, grounded analysis of immutable GitHub repository metadata and README evidence, preserving the exact source revision and producing a tenant-scoped searchable projection.

## ADDED Requirements

### Requirement: Repository evidence yields a versioned structured analysis

Knowledge SHALL build deterministic repository context from the command's typed metadata and `ReadmeRevision`; a present README MUST be resolved only through its immutable `BlobRef`, and a missing or unauthorized reference MUST be rejected or recorded as an explicit absent state. Knowledge SHALL never query Catalog tables or fetch README URLs. It SHALL invoke only the configured provider policy and validate output against the repository analysis schema before accepting it.

#### Scenario: Fixture repository becomes structured analysis

- **WHEN** a repository observation fixture includes valid metadata and README evidence
- **THEN** a versioned repository analysis with grounded fields and source provenance is persisted

#### Scenario: Invalid provider output is not accepted

- **WHEN** a provider response violates the repository schema or cites absent evidence
- **THEN** the failed attempt and validation diagnostic are recorded and no accepted repository analysis is persisted

### Requirement: Repository analyses share enforced spend limits

Knowledge SHALL reserve and settle repository provider usage against the shared daily ledger before execution, so the configured global and family limits apply across article and repository runs.

#### Scenario: Exhausted shared budget blocks repository execution

- **WHEN** earlier accepted or reserved family work exhausts the applicable daily budget
- **THEN** a repository run is retained as budget-blocked and the provider is not called

### Requirement: Accepted repository analysis is projected for search

Knowledge SHALL create a tenant-scoped searchable document from the accepted repository analysis and source evidence; a replay or older source revision MUST NOT regress a newer projection.

#### Scenario: Accepted repository analysis is searchable

- **WHEN** a repository run passes validation and persistence
- **THEN** search returns its repository projection only to the owning tenant with stable source and revision identity
