# Evaluation Harness Specification

## Purpose

Defines how Knowledge measures structured analysis quality deterministically against committed fixture cases - scoring contract validity, grounding, bounds, and cardinality of any response without wording equality, comparing labeled provider and prompt-version response sets, and producing a byte-stable report artifact entirely offline by default.

## ADDED Requirements

### Requirement: Fixture cases pair sources with expected qualities

The repository SHALL carry a committed set of evaluation cases, each pairing one Document IR source with machine-checkable expectations: required result fields, summary length bounds, key-point cardinality bounds, and the block identifiers a grounded key point may cite. The loader SHALL reject a case file with unknown fields, missing expectations, or an invalid document rather than score it.

#### Scenario: the committed case set loads and validates

- **WHEN** the harness loads every committed case file
- **THEN** each case parses to its typed form with its document and expectations intact, and a deliberately corrupted case file with an unknown field or missing expectation is rejected with an error naming the violation

### Requirement: Scoring is deterministic

Scoring one response against one case SHALL be a pure function of the response bytes, the case, and the prepared context: no clock, randomness, map iteration order, or environment state may influence the outcome. Scoring the same input twice SHALL yield byte-identical check outcomes.

#### Scenario: repeated scoring produces identical results

- **WHEN** the full committed case set is scored twice against the same recorded response set and both report artifacts are rendered
- **THEN** both artifacts are byte-identical, including check order, aggregate counts, and rendered labels

### Requirement: Checks evaluate validity, grounding, and bounds without wording equality

The scorer SHALL verify each response against the analysis schema, the case's field presence, summary length bound, key-point cardinality bounds, and citation grounding where every cited block identifier must exist in the supplied context. The scorer SHALL NOT compare response text to expected text, and a response whose claims cite blocks absent from the supplied context SHALL fail its grounding check.

#### Scenario: a well-formed grounded response passes every check

- **WHEN** a recorded response that satisfies the schema, the bounds, the cardinalities, and cites only supplied blocks is scored against its case
- **THEN** every check passes and the case's aggregate result is a pass

#### Scenario: a fabricated citation fails grounding

- **WHEN** a response otherwise valid cites a key point with a block identifier that the prepared context does not contain
- **THEN** the grounding check for that key point fails, the failure names the offending reference, and the case's aggregate result is a fail

#### Scenario: out-of-bounds output fails its bound

- **WHEN** a response exceeds the case's summary length bound or carries more key points than the cardinality bound allows
- **THEN** the corresponding bound check fails with observed versus allowed values while unrelated checks still evaluate normally

### Requirement: Response sets compare under explicit labels

A scored run SHALL group results under a caller-supplied label naming the response-set origin, including the provider identity and prompt version it represents, and the report SHALL present each label's per-case outcomes and aggregates side by side so two sets over the same cases compare directly.

#### Scenario: two labeled sets appear side by side in one report

- **WHEN** the harness scores two differently labeled response sets over the same case set into one report
- **THEN** the report contains one section per label with that label's per-case outcomes and totals, and no case appears under the wrong label

### Requirement: The default harness run is offline

The harness SHALL score only recorded responses in its default mode: loading them requires no credentials, performs no network request, and completes on a machine with no outbound access. Reaching a live provider SHALL require explicitly supplying credentials through the environment and is never part of the default gate.

#### Scenario: a credential-less run scores recorded sets and stops there

- **WHEN** the harness runs with no provider credentials configured over the committed recorded sets
- **THEN** it produces the complete report artifact without performing any network request and exits zero
