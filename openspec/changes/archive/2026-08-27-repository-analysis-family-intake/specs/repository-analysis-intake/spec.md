## ADDED Requirements

### Requirement: Repository-analysis request delivery is durably idempotent

Knowledge SHALL persist a repository-analysis request as one pending record identified by its
published idempotency digest. It SHALL acknowledge an exact redelivery without adding a second
pending record and SHALL reject a digest reused for different immutable request data.

#### Scenario: Exact redelivery keeps one pending request

- **WHEN** Knowledge receives the same valid repository-analysis request twice
- **THEN** the first delivery creates one pending record and the second is acknowledged as a
  duplicate without creating another record

#### Scenario: Digest collision is not treated as a duplicate

- **WHEN** Knowledge receives a request with an existing idempotency digest but different immutable
  repository-analysis input
- **THEN** it rejects the delivery and preserves the original pending record

### Requirement: Terminal repository-analysis outcomes link the immutable request once

Knowledge SHALL create a terminal completion or failure fact only when the supplied request exactly
matches a pending record. A terminal transition SHALL retain the completion result reference or safe
failure code and SHALL not be emitted again after the record is terminal.

#### Scenario: Completion requires the pending source revision

- **WHEN** a completion is attempted with a different source revision
- **THEN** the pending request remains unresolved and no completion fact is created

#### Scenario: Final failure is terminal and result-free

- **WHEN** Knowledge records a final repository-analysis failure for an exact pending request
- **THEN** it creates one failure fact with its retryability and no analysis-result reference, and a
  repeated failure attempt creates no second fact
