# Embedding Search Specification

## Purpose

Defines how accepted article analyses become versioned dense vectors, how those vectors persist with explicit model identity inside the Knowledge-owned PostgreSQL schema, and how retrieval fuses lexical and semantic rankings into one deterministic, tenant-scoped result page.

## ADDED Requirements

### Requirement: Chunking is deterministic and versioned

The system SHALL derive embedding input chunks from the accepted analysis projection with a documented policy identified by an immutable chunking version, configurable target size, and configurable overlap. Applying the policy to the same projected text SHALL always yield the same chunk sequence with stable ordinals, regardless of execution order or repetition.

#### Scenario: the same projection chunks identically on repeated runs

- **WHEN** the chunking policy is applied twice to the same title, lead, and body of one accepted analysis
- **THEN** both applications return an equal number of chunks with equal texts and equal ordinals

#### Scenario: configuration changes chunk boundaries

- **WHEN** the chunking policy runs with a larger configured target size than a second run against the same text
- **THEN** the first run yields fewer or equal chunks than the second, and each chunk respects its configured size bound

### Requirement: Every vector records complete identity and provenance

The system SHALL persist each embedding with its source revision identity, owning tenant, accepted output reference, chunk ordinal, chunk text digest, chunking version, provider identifier, model identifier, declared dimensions, and embedding prompt version. A vector row without this identity SHALL NOT be written.

#### Scenario: a persisted vector exposes its identity fields

- **WHEN** the indexing step stores vectors for one accepted analysis under a configured provider, model, dimensions, chunking version, and prompt version
- **THEN** each stored row reports that exact provider, model, dimensions, chunking version, prompt version, ordinal, chunk digest, tenant, source revision identity, and accepted output reference

### Requirement: Query time never mixes model versions

Retrieval SHALL bind exactly one explicit embedding model identity per query and SHALL only consider vectors whose stored identity equals that binding. Vectors produced under any other provider, model, dimensions, chunking version, or prompt version combination SHALL be invisible to that query until an explicit reindex adopts them.

#### Scenario: vectors from a superseded model version stay invisible

- **WHEN** vectors exist for one source under an older model version while the active configuration names a newer version, and a hybrid query runs
- **THEN** no result derives from the older version's vectors

### Requirement: Vectors live in the owned schema with similarity indexing

The system SHALL store embeddings inside the Knowledge-owned PostgreSQL schema as fixed-dimension vector columns with an index supporting cosine-distance ordering, and schema application SHALL remain idempotent. The stored dimensionality SHALL match the declared dimensions of the active embedding configuration, and a write whose dimensionality differs SHALL fail validation rather than coerce.

#### Scenario: schema application is repeatable

- **WHEN** the schema definition is applied to a fresh database and then applied again unchanged
- **THEN** the second application succeeds without error and leaves one extension, table set, and similarity index in place

#### Scenario: a dimension-mismatched vector is rejected

- **WHEN** an embedding write supplies a vector whose dimension count differs from the declared storage dimensionality
- **THEN** the write fails with a validation failure and no partial vector row persists

### Requirement: Indexing is a bounded background step driven by durable state

The system SHALL embed accepted analyses through a background step that selects analyses whose runs reached `persisted`, generates vectors under the active identity, persists them atomically with a single guarded transition of that run to `indexed`, and repeats selection until quiet. The step SHALL respect configured bounds on batch size, per-request deadline, request spacing, input size, and spend ceilings before each provider call, and SHALL shut down within the process drain bound without losing recorded results.

#### Scenario: a persisted analysis becomes indexed exactly once

- **WHEN** an accepted analysis sits in `persisted` and the background step completes one pass with a working scripted provider
- **THEN** the run reaches `indexed`, the vectors are stored once, and a further pass changes nothing

#### Scenario: indexing survives process restart

- **WHEN** the process stops mid-work and starts again with the same database
- **THEN** analyses still in `persisted` are selected again and reach `indexed` exactly once, with no duplicated vector rows

### Requirement: Indexing failure is explicit and never destructive

When embedding generation or validation fails, the system SHALL leave the accepted analysis result untouched, leave the run in its pre-indexing state, and record a bounded failure entry carrying the error class and attempt count. Repeated failures SHALL stop after the configured attempt bound and require either recovery of the provider or an explicit reindex to retry.

#### Scenario: a provider failure leaves the analysis intact

- **WHEN** the embedding provider returns a permanent failure for an accepted analysis
- **THEN** the analysis output remains present and accepted, the run remains `persisted`, and a failure entry records the error class with an incremented attempt count

#### Scenario: attempts stop at the configured bound

- **WHEN** the provider fails more times than the configured attempt bound for one analysis
- **THEN** the background step makes no further provider calls for that analysis until the attempt count is reset by an explicit action

#### Scenario: an exhausted budget refuses provider calls

- **WHEN** the durable spend ledger reports the configured ceiling exhausted before an embedding call
- **THEN** no provider request is sent, the analysis stays `persisted`, and a budget-refusal failure entry is recorded

### Requirement: Hybrid ranking fuses lexical and semantic legs deterministically

For a non-blank query against a tenant with an embeddings provider configured, retrieval SHALL produce candidate orderings from the weighted lexical score and from cosine vector similarity under the active model identity, fuse them with Reciprocal Rank Fusion using the fixed constant k=60, apply identical tenant scoping to both legs, and order the fused page by descending fused score with deterministic tiebreakers. Pagination SHALL be stable across identical repeated queries.

#### Scenario: fixture ordering follows the fusion rule

- **WHEN** a hybrid query runs against fixtures where the lexical and semantic legs rank documents differently
- **THEN** the returned order equals the order computed from Reciprocal Rank Fusion at k=60 over those two leg orderings, with ties broken by recency then document identity

#### Scenario: both legs enforce the tenant scope

- **WHEN** a hybrid query runs for one tenant while matching vectors and text exist for another tenant
- **THEN** neither leg contributes the other tenant's documents to the response

#### Scenario: repeated queries return identical pages

- **WHEN** the same hybrid query with the same pagination runs twice against unchanged data
- **THEN** both responses contain the same results in the same order with the same snippets

### Requirement: Retrieval degrades gracefully without embeddings

When no embeddings provider is configured, retrieval SHALL behave exactly as the lexical-only path: recent browse for blank queries and lexical ranking otherwise, with no embedding-related errors surfaced.

#### Scenario: an offline process still serves lexical search

- **WHEN** the process starts without an embeddings credential and a lexical query runs
- **THEN** the response contains lexically ranked results and no provider-dependent failure

### Requirement: Model version changes reindex explicitly

A change to the chunking version or embedding model identity SHALL NOT rewrite existing vectors as a side effect of startup, analysis, or search. Regeneration SHALL happen only when the explicit reindex command runs: it re-chunks and re-embeds affected sources under the newly active identity, replaces superseded identity rows only after their replacements succeed, leaves historical analyses and outputs byte-identical, and reports what it did. Running it again after success SHALL make no further changes.

#### Scenario: reindex converges and is idempotent

- **WHEN** the reindex command runs to completion after a model version change, and then runs a second time
- **THEN** after the first run every affected source carries vectors only under the new identity, the old identity rows are gone, historical analyses are unchanged, and the second run performs no provider calls and changes nothing

#### Scenario: startup never mutates vectors

- **WHEN** the service starts with a changed embedding model identity and serves only search traffic without the reindex command
- **THEN** all previously stored vector rows retain their original identity values
