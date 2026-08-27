# Repository-analysis family intake

## Why

GitHub Catalog can now publish a repository-analysis request, but Knowledge has no durable,
idempotent intake or way to construct the terminal result linkage. This leaves repository watches
without a safe, observable pending state.

The producer/consumer contract is defined by the workspace store spec
`repository-analysis-intake`; this change implements only Knowledge's side of that contract.

## What changes

- Pin the published GitHub analysis-contract package and add a Knowledge-owned PostgreSQL request
  record.
- Persist an immutable request once by its idempotency digest, rejecting a digest collision with
  different immutable inputs.
- Transition only an exact pending request to one typed completion or final-failure fact.
- Document the internal request and terminal-fact interfaces.

## Impact

- Affected code: `crates/knowledge`, `schema.sql`, and Knowledge interface documentation.
- Affected external contract: `knowledge.repository_analysis.*.v1`, owned by the workspace contract
  store. No new HTTP route or provider integration is introduced.
