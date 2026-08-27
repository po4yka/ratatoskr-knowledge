## Context

The cross-repository `repository-analysis-intake` specification requires GitHub Catalog to request
analysis of an immutable repository revision and Knowledge to make all inference, budget, retry and
terminal-outcome decisions. The published contract revision carries the stable Catalog repository
identity, numeric GitHub identity, bounded metadata, README state, requested family, and a SHA-256
idempotency digest.

Knowledge already has PostgreSQL-backed spend accounting and controlled providers. It has no event
transport or repository-analysis worker in this slice, so the intake boundary must remain durable
and provider-independent.

## Decision

Create `knowledge.repository_analysis_requests` as the durable local inbox and use the contract's
idempotency digest as its unique key. The row keeps the immutable identity and input references,
plus `pending`, `completed`, and `failed` terminal linkage state.

On redelivery, the consumer compares the stored immutable request with the supplied one. It returns
`Duplicate` only for an exact match; a reused digest for changed input is an explicit safe error.
Completion and failure use an expected-state update over the complete request identity and source
revision. Only a changed pending row produces the corresponding typed terminal fact, so retries
cannot emit a second outcome.

The separate repository-analysis worker will select pending rows and compose its provider through
`ControlledProvider`/`BudgetLedger`. Therefore queue delivery never spends a GitHub-owned budget or
calls a model in the intake transaction.

## Consequences

- Metadata synchronization remains independent of Knowledge/provider availability.
- Knowledge owns request lifecycle state and budget enforcement; GitHub only creates requests.
- A future transport adapter can publish the returned terminal fact transactionally with its outbox
  implementation. This change does not invent a second event system.
