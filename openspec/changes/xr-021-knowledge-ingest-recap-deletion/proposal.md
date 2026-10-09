## Why

Changeset XR-021 fixes cross-repository integration defects found while composing the fleet. Knowledge carried four of them: the channel-recap chain asks a model for values only the system can know and cannot resume a run killed mid-call, the service starts no ingestion consumer so documents, social facts, AI-archive facts and repository requests never reach analysis, privacy deletion sweeps the whole shared blob root and removes other tenants' blobs while leaving content-bearing inbox state behind, and Knowledge has no pinned bus identity or authorized-broker proof. The cross-repository behaviour is defined once in XR-021 CONTRACTS.md sections S01 to S04, S08, S09 and S12; this change cites it and does not restate it.

## What Changes

- Move every `ratatoskr-*` contracts dependency to the XR-021 contracts commit and verify the AI-archive content digest on intake (S07, S13).
- Conform the channel-recap manifest client to the contract path, headers and shared manifest types, make the recap pipeline crash-resumable behind a per-run advisory lock, and compose the deterministic recap fields (language, manifest digest, coverage) in Knowledge instead of asking the model for them (S08).
- Add the ingestion worker: four supervised durable consumers (documents, social sources, AI archive, repository requests), `DocumentIntake`, social removal with tombstone ordering, a transactional completion outbox with a publisher, a GitHub README client, ingest configuration, readiness and a shared provider runtime (S01, S02, S04, S09).
- Replace the global orphan blob sweep with a durable erasure queue, collect Knowledge-owned source snapshots with their source, and delete inbox snapshots, heads, object inbox payloads, repository requests and the new XR-021 tables per scope (S12).
- Pin the Knowledge NATS identity stanza and prove the recap and ingest workers run under it against an authorization-enabled broker (S03, S04).
- Deliberate breaks, named in the commits that make them: `DeletionReceipt.orphan_digests_removed` becomes `recovered_digests_removed` and the job output `orphan_blobs=` becomes `recovered_blobs=`; the recap provider structured-output schema becomes the model-output shape; the manifest client no longer sends `x-ratatoskr-owner` or `x-ratatoskr-manifest-ref`.

## Capabilities

### New Capabilities

- `knowledge-ingest`: durable ingestion of extracted documents, social facts, AI-archive facts and repository requests with a transactional completion outbox.

### Modified Capabilities

- `channel-digest-recap-analysis`: contract-conformant manifest reads, crash-resumable runs and deterministic composition of non-model fields.
- `privacy-deletion`: queue-based blob collection scoped to the deleted data, and deletion of content-bearing inbox state.

## Impact

Affected surfaces are `schema.sql` (edited in place, no migration), the recap chain, the deletion module, the ingest worker in `services/knowledge`, configuration validation, readiness, the deploy NATS fragment and the documentation. No new contract version, no migration file and no compatibility shim is introduced. Cross-repository behaviour is defined in XR-021 CONTRACTS.md.
