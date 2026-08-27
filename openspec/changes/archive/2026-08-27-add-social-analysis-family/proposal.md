## Why

Normalized social-source snapshots already have a published contract but are not interpreted or searchable in Knowledge. Legacy behavior analyzed X posts; this change makes the same capability durable and provenance-preserving without treating linked articles as post evidence.

## What Changes

- Consume `social.source.captured.v1` and `social.source.updated.v1` through the shared inbox using `ratatoskr-social-contracts`.
- Add a social-specific typed contract, schema, prompt, context builder, validation and search projection tied to the snapshot digest and post provenance.
- Apply the common ledger budget and idempotent replay/gap handling introduced by repository analysis.

## Capabilities

### New Capabilities

- `social-analysis`: A normalized social snapshot yields one grounded, versioned analysis and searchable document per immutable content digest.

### Modified Capabilities

- None.

## Impact

- Affects Knowledge's social-contract dependency, event decoding, analysis run specialization, prompt/schema resources, fixture events, and search projection.
- Requires the repository-analysis intake foundation and the already published social contracts; it does not change social producers or clients.
