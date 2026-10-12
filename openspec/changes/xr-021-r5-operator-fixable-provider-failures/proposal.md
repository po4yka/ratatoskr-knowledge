## Why

Round 5 of changeset XR-021 (CONTRACTS.md R5-08 rule 1) verified that `classify_error` maps 401, 403 and every other 4xx (402 and 404 included) to a permanent provider error. A rejected credential, an exhausted credit balance or a withdrawn model Terms the delivery at its first attempt and records a permanent failure on the run, although the operator can fix the cause and the content is lost. Round 4 already made a spend-budget refusal recoverable for the same reason.

## What Changes

- `ProviderError` gains `OperatorAction`: not permanent, redelivered exactly like `BudgetExhausted`.
- `classify_error`: 401 and 403 are `AuthError` plus `OperatorAction`; 402 is the new class `PaymentRequired` plus `OperatorAction`; 404 is the new class `ModelUnavailable` plus `OperatorAction`; 400, 422 and any other 4xx stay `RequestInvalid` and `Permanent`; 408, 429 and 5xx stay transient.
- The two classes are spelled `payment_required` and `model_unavailable` and are listed in the three `failure_class` CHECK lists of `schema.sql`.
- An `OperatorAction` failure is a non-spent attempt, the run is reopened on the next delivery, the delivery is redelivered with the Round 4 delay, a WARN names family, class and delivered count from the second delivery on, and `knowledge_ingest_redeliveries_total` counts it.
- Deliberate break: a 401, 402, 403 or 404 is no longer a permanent failure; a development database is recreated for the CHECK lists.

## Capabilities

### Modified Capabilities

- `knowledge-ingest`: operator-fixable provider failures are waited out.

## Impact

`provider.rs`, `openrouter.rs`, `run_driver.rs`, `controlled.rs`, `embeddings.rs`, the ingest outcome mapping, `schema.sql`, the tests and `README.md`.
