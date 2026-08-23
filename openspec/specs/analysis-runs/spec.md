# Analysis Runs Specification

## Purpose

Defines immutable source references and the durable, idempotent state and attempt history of one
Knowledge analysis execution.

## Requirements

### Requirement: Source references preserve immutable identity

A source reference SHALL contain its tenant, owning bounded context, source document identifier,
Document IR content digest, and source provenance blob reference. A changed digest SHALL create a new
source revision rather than overwrite prior evidence.

#### Scenario: one document arrives with a new digest

- **WHEN** a stored source identifier is registered with a different content digest
- **THEN** both revisions remain addressable and analysis uniqueness treats them as different inputs

### Requirement: Run identity is deterministic and idempotent

Run uniqueness SHALL include tenant, source revision, analysis contract, prompt, context builder, and
model policy. Repeating the same request SHALL return the existing run and SHALL NOT create another
provider attempt or terminal result.

#### Scenario: one request is delivered twice

- **WHEN** the same complete analysis identity is created twice
- **THEN** one run identifier and one execution history exist

### Requirement: State transitions are explicit and monotonic

Runs SHALL persist only documented transitions through queued, context prepared, model requested,
response received, schema validated or repaired, persisted, and completed states. Failure SHALL be
explicit. A terminal completed or failed run SHALL NOT return to a non-terminal state.

#### Scenario: a completed run is replayed

- **WHEN** a worker repeats an earlier transition for a completed run
- **THEN** the run remains completed and no new result or attempt is created

### Requirement: Every provider attempt is durable

Each attempt SHALL record its ordinal, reason, provider and model policy, request identifier when
known, raw-response reference when received, bounded usage, and safe outcome. Attempt ordinals SHALL be
unique and increasing within a run.

#### Scenario: a repair follows invalid output

- **WHEN** the first response is invalid and one repair is attempted
- **THEN** two ordered attempt records preserve both outcomes and raw-response references
