## Context

XR-021 CONTRACTS.md R5-08 rule 1 is the contract. This file records the choices local to Knowledge.

## Decisions

### A variant that behaves exactly like the spend-budget refusal

`OperatorAction` is matched wherever `BudgetExhausted` is matched: `is_permanent` is false, `ProviderFailure::is_recoverable` is true, the run driver records the attempt as not spent and does not retry inside the same drive, the controlled and embeddings adapters pass it through, and the ingest outcome mapping turns it into `Retry`. The spend refusal stays a distinct variant because its meaning (a wait for a window to clear) differs from an operator fixing a credential.

### Two new classes, not one

A rejected key, an empty balance and a withdrawn model are different operator actions, so `PaymentRequired` and `ModelUnavailable` are separate words from `AuthError`. They are stored in the `failure_class` column, hence the three CHECK lists.

### Visibility

The WARN carries family, class and delivered count and never content. The existing label-free `knowledge_ingest_redeliveries_total` counts the redelivery; no label is added.
