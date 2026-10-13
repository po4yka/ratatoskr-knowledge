## Context

XR-021 CONTRACTS.md R6-01 is the contract (extends R5-08 rule 1). This file records the choices local to Knowledge.

## Decisions

### Wait, not re-queue

R5-08 solved the same loss in the other lanes by waiting. A request that waits is lossless and visible; a GitHub re-queue would need a new state machine, a retry policy and a second terminal path. GitHub keeps the request `pending` until a terminal fact, so a later completed fact resolves it.

### A named decision beside the mapping

`repository_disposition(failure, delivered)` returns `Wait`, `RetryWithinBudget` or `Settle(code, retryable)`. The table is a pure function so a unit test covers it without a broker. `classify_repository_failure` keeps mapping a settled failure to the fact vocabulary. `Wait` carries the log class (`provider_operator_action` or `repository_analysis`), because the class is part of the decision.

### The run stays recoverable

R5-08 records an `OperatorAction` failure as a non-spent attempt and reopens the run on the next delivery; `BudgetExhausted` is recoverable since R4-05. The repository execution shares that driver. The end-to-end tests prove it (nine transport requests, two run rows, redeliveries counted) instead of assuming it.

### The budget that stays

A transient provider failure, a timeout, a held run, an invalid response and an unavailable README retry within five deliveries and then settle the retryable fact. `TERMINAL_DELIVERY` keeps its value.
