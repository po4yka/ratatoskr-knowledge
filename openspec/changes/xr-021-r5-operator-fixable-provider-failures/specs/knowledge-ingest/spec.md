## ADDED Requirements

### Requirement: An operator-fixable provider failure is waited out

A provider answer that an operator can fix (401, 402, 403, 404) SHALL be classified `OperatorAction`, SHALL NOT be permanent, SHALL NOT count against the run's call budget, and SHALL be redelivered with the standard delay until the fault clears. A 400, 422 or other client error SHALL stay permanent.

#### Scenario: The status table

- **WHEN** the OpenRouter adapter classifies 401, 402, 403, 404, 400 and 422
- **THEN** 401 and 403 are `auth_error`, 402 is `payment_required` and 404 is `model_unavailable`, all `OperatorAction`, and 400 and 422 are `request_invalid` and permanent

#### Scenario: A run survives a rejected credential

- **WHEN** the provider answers 401 twenty times in a row and then succeeds
- **THEN** the run completes and no permanent failure was recorded on it

#### Scenario: Each family survives eight 401 deliveries

- **WHEN** the provider answers 401 for the first eight deliveries of a document, a social capture and an AI-archive conversation and then recovers
- **THEN** each is applied once, none is terminated and `knowledge_ingest_redeliveries_total` is above zero

#### Scenario: A malformed request is still rejected

- **WHEN** the provider answers 400
- **THEN** the delivery is terminated at its first delivery
