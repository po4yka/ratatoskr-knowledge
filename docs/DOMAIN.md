# Knowledge domain model

## Terms

- **Source revision:** immutable authorized input identified by content hash and authority.
- **Analysis family:** article, repository, social, or AI-conversation contract.
- **Prompt version:** reviewed template/instructions identity.
- **Context build:** deterministic selected content and provenance.
- **Analysis run:** durable execution with contract, provider/model policy, attempts, usage, and output.
- **Claim/citation:** derived statement linked to source spans.
- **Embedding set:** vectors produced by one model/chunking/version policy.
- **Search document/index generation:** authorized retrieval projection.

## Run lifecycle

`queued -> context_prepared -> model_requested -> response_received -> schema_validated -> repaired? -> persisted -> indexed -> completed | failed`

## Invariants

1. Derived analysis never becomes source authority.
2. Immutable run identity includes all material versions.
3. Unknown/unvalidated output is not published as structured analysis.
4. Provider retry does not duplicate persisted effects.
5. Retrieval authorization precedes result disclosure.
6. Reindexing is explicit and does not silently alter historical analyses.
