## Why

Round 6 of changeset XR-021 (CONTRACTS.md R6-01) verified that `IngestContext::repository` returns `Outcome::Retry` only while `delivered < TERMINAL_DELIVERY` (5) and then settles a retryable `knowledge.repository_analysis.failed.v1` fact. `classify_repository_failure` treats a `FamilyPipelineError::Provider(_)` of any class as a transient fault, so a rejected credential, an empty balance, a withdrawn model (`ProviderError::OperatorAction`, R5-08) and a spend-budget refusal (`ProviderError::BudgetExhausted`) give up after about 30 seconds. GitHub closes the request on that fact and nothing re-queues it, so the analysis is lost although the operator can fix the cause. The documents, social and AI-archive families have no budget and wait. The cross-repository rule is defined once in CONTRACTS.md R6-01; this change cites it.

## What Changes

- A named decision `RepositoryDisposition` (`Wait`, `RetryWithinBudget`, `Settle(code, retryable)`) decides what `repository` does after a failed `execute_repository`: `Provider(OperatorAction)` and `Provider(BudgetExhausted)` wait at every delivery (never settle, never consult `TERMINAL_DELIVERY`); every other transient failure retries within the 5-delivery budget and then settles; a permanent source or integrity failure settles `contract_invalid` at once.
- The log class of a waiting delivery is `provider_operator_action` for an operator failure and `repository_analysis` for a budget refusal.
- `classify_repository_failure` stays the single mapping of a settled failure.
- The doc comments of `TERMINAL_DELIVERY` and `Outcome::Retry` say that the budget covers only plain transient faults. `README.md`, `DEVELOPMENT.md` and `docs/TESTING.md` say the same.
- Deliberate break: a repository request whose provider is refused for an operator-fixable reason or a spend budget no longer settles a failed fact after the fifth delivery; it stays pending until the cause is fixed.

## Capabilities

### Modified Capabilities

- `knowledge-ingest`: the repository family's delivery budget.

## Impact

`services/knowledge/src/ingest_handlers.rs`, `services/knowledge/src/ingest.rs`, the ingest bus tests and three documents. No contract version, no migration, no shim, no GitHub re-queue.
