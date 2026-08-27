## 1. Durable request intake

- [x] 1.1 Pin the workspace `ratatoskr-github-contracts` package and add the current-schema request
  record. No failing test applies to dependency pinning/schema declaration; schema behavior is
  covered by the PostgreSQL integration tests below.
- [x] 1.2 Add `conflicting_request_with_the_same_idempotency_digest_is_rejected` in
  `crates/knowledge/tests/repository_analysis.rs`; observe it fail because the old intake returned
  `Duplicate` for changed immutable input.
- [x] 1.3 Compare a conflicting digest's stored immutable input and return the explicit safe error.

## 2. Pending and terminal linkage

- [x] 2.1 Add PostgreSQL integration coverage for exact request redelivery, mismatched completion,
  successful completion, and final failure in
  `crates/knowledge/tests/repository_analysis.rs`.
- [x] 2.2 Persist only one pending request and use expected-state terminal transitions to construct
  the completion or failure fact once.

## 3. Documentation and verification

- [x] 3.1 Document the consumed request and emitted terminal facts, citing the workspace
  `repository-analysis-intake` specification.
- [x] 3.2 Run the complete `DEVELOPMENT.md` gate against PostgreSQL 17 with pgvector.
