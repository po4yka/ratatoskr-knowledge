# Knowledge interfaces

## Inbound

Versioned analysis commands and source-upsert events referencing authorized immutable document/social/repository/archive content; reindex/reconcile commands; authorized public queries through Platform.

## Outbound

Analysis completed/failed events, safe progress, analysis/search projections, usage/cost audit, and index-generation status.

## Internal interfaces

- `ContextBuilder`: deterministic bounded context plus provenance.
- `LlmProvider`: structured generation request/response and usage.
- `OutputValidator`: JSON Schema/typed validation and bounded repair.
- `EmbeddingProvider`: model/version-aware batches.
- `SearchIndex`: authorized FTS/vector upsert/delete/query.

## Rules

Commands include owner, source revision, family/contract, policy, operation, and idempotency. Provider raw responses are stored as protected blobs where policy permits. Errors distinguish policy, unavailable source, provider transient/permanent, budget, invalid output, indexing, and privacy deletion. No provider-specific response shape leaks into public contracts.
