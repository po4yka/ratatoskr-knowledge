## Why

Repository metadata and README revisions cannot currently enter Knowledge, even though Catalog records them and legacy behavior produced structured repository analysis under a bounded daily LLM budget. This change establishes the reusable durable intake path with a real Catalog event.

## What Changes

- Consume contract-validated `knowledge.repository_analysis.requested.v1` commands through a durable inbox and create idempotent repository analysis runs from immutable source references.
- Add a repository-specific typed output contract, JSON Schema, versioned prompt, deterministic README/metadata context builder, validation, and a search-document projection; README evidence is an immutable BlobRef, not an event body.
- Charge the existing shared budget ledger before provider execution and preserve replay/gap state without duplicate runs or spend.

## Capabilities

### New Capabilities

- `repository-analysis`: Repository observations yield versioned, grounded structured analyses and searchable projections.
- `analysis-event-intake`: Knowledge durably validates, deduplicates, and replays source facts without accessing producer tables.

### Modified Capabilities

- None.

## Impact

- Affects Knowledge dependencies, current schema, event inbox, run identity, prompts/schemas, provider pipeline, budget ledger, search/indexer projection, and integration fixtures.
- Depends on the published repository-analysis request contract and the Catalog producer change (including README acquisition) being deployed first.
