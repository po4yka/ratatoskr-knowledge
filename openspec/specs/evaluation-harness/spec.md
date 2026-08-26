# Evaluation Harness Specification

## Purpose

Defines how Knowledge measures structured analysis quality deterministically against committed fixture cases, comparing labeled recorded-response sets without live provider access.

## Requirements

### Requirement: Fixture cases pair sources with expected qualities

The repository SHALL carry committed, non-sensitive evaluation cases pairing a title/block source projection with machine-checkable summary bounds, key-point cardinality bounds, and required citation block indexes. The loader SHALL reject unknown fields or missing expectations.

#### Scenario: the committed case set loads and validates

- **WHEN** the harness loads every committed case file
- **THEN** each case parses to its typed form and malformed unknown or missing fields are rejected with an identifying error

### Requirement: Scoring is deterministic

Scoring one recorded response SHALL be a pure function of the case and response JSON, with no clock, randomness, environment, provider client, or network access. Labels and cases SHALL sort deterministically, and rendering SHALL contain no timestamp or host facts.

#### Scenario: permuted scoring produces identical artifacts

- **WHEN** the same cases and responses are supplied in different input orders
- **THEN** the rendered reports are byte-identical

### Requirement: Checks evaluate validity, grounding, and bounds without wording equality

The scorer SHALL validate the article contract and independently evaluate summary length, key-point cardinality, citation grounding, and required evidence coverage. It SHALL NOT compare wording to an expected answer; a cited block index absent from the source projection SHALL fail grounding with the offending index named.

#### Scenario: a conforming response passes every check

- **WHEN** a recorded response meets the contract, bounds, coverage, and source-block requirements
- **THEN** every reported check passes

#### Scenario: independent failures remain visible

- **WHEN** a structurally valid response exceeds a bound and cites an absent block
- **THEN** the bound and grounding failures report observed-versus-allowed facts while unrelated checks still run

### Requirement: Response sets compare under explicit labels

A run SHALL group outcomes under recorded provider/prompt labels and present every label's per-case results and aggregate totals side by side.

#### Scenario: two labels remain separated

- **WHEN** two labeled response sets cover the same fixture cases
- **THEN** each label has its own ordered case results and totals, with no result assigned to the other label

### Requirement: The harness is offline by default

The committed runner SHALL load only fixture files, require no credential, construct no provider transport, and make no network request.

#### Scenario: a credential-less run completes

- **WHEN** the harness runs over the committed response sets without provider configuration
- **THEN** it emits the complete report artifact and exits successfully without network access
