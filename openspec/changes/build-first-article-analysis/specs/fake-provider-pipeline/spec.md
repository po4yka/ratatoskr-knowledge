## Purpose

Defines deterministic provider execution, raw-response preservation, validation, retry, repair, and
accepted-result persistence without a production inference dependency.

## ADDED Requirements

### Requirement: The provider boundary is narrow and scripted in tests

The analysis pipeline SHALL call a provider through one JSON-generation boundary that returns raw
response bytes plus safe request and usage facts or a typed failure. Default tests SHALL use a
hand-written scripted fake and SHALL make no external request.

#### Scenario: two scripted responses are configured

- **WHEN** one analysis uses the scripted fake provider
- **THEN** responses are consumed in order and the test can inspect each bounded generation request

### Requirement: Raw output is protected before validation

Each received raw response SHALL be size-limited and stored under the Knowledge-owned
content-addressed root before JSON parsing. The attempt SHALL reference it with a `BlobRef`; raw bytes,
paths, and response text SHALL NOT enter ordinary logs or events.

#### Scenario: malformed JSON is received

- **WHEN** a provider returns malformed JSON within the byte limit
- **THEN** its owned blob reference and validation outcome are recorded before the run fails or repairs

### Requirement: Validation is independent and ordered

The pipeline SHALL validate raw JSON against the schema generated from the canonical typed result
before deserialization, then apply string, cardinality, uniqueness, and source-block checks. It SHALL
not silently coerce an invalid value.

#### Scenario: structurally valid output has a bad citation

- **WHEN** JSON Schema accepts a response whose block index is absent from the source
- **THEN** semantic validation rejects it and no accepted result is stored

### Requirement: Retry and repair share one finite extra attempt

The first attempt MAY be followed by at most one additional attempt. An eligible transient provider
failure uses it as a retry. A repairable validation failure uses it as a repair. Permanent provider
failures, raw-size failures, and a second invalid response SHALL end the run without another call.

#### Scenario: transient failure then success

- **WHEN** the first provider call returns an eligible transient failure and the second returns valid
  output
- **THEN** the run completes after exactly two attempts

#### Scenario: invalid repair response

- **WHEN** an invalid first response is followed by another invalid response
- **THEN** the run fails after exactly two attempts and preserves both outcomes

### Requirement: Accepted output and completion are atomic

The validated typed result and the transition to persisted SHALL commit together. Completion SHALL
occur only after that commit. A replay SHALL return the existing result without another provider call.

#### Scenario: a completed request is repeated

- **WHEN** the same complete analysis identity is submitted after completion
- **THEN** the stored result is returned and the provider call count does not change
