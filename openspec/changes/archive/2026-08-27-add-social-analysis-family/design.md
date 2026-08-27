## Context

See proposal.md. This change follows the repository-analysis intake foundation and consumes the already published social contracts rather than adding a producer-side abstraction.

## Goals / Non-Goals

**Goals:** map social snapshots to a dedicated analysis contract, preserve capture authority and post-vs-link provenance, share durable intake/budget/search semantics.

**Non-Goals:** social acquisition, linked-article fetching, social event publication, embeddings changes, or client features.

## Decisions

### D1. Decode the published snapshot at the edge

Captured and updated event payloads are parsed by `ratatoskr-social-contracts`; only normalized snapshot fields and references enter Knowledge's deterministic builder.

### D2. Keep post and external context separate

The builder labels post text, media references, and any link metadata distinctly. The schema's citations target only supplied post spans; linked pages require their own Extractor/Document pipeline.

### D3. Content digest defines semantic replay identity

The snapshot's published digest joins family contract/prompt/context/model identity for run deduplication. Updated content creates a distinct revision, while duplicate event delivery only resolves its inbox receipt.

## Risks / Trade-offs

- [Partial snapshot is misrepresented as complete] → carry contract completeness/warnings into prompt and result provenance.
- [Contract revision changes] → pin the published git revision and create new immutable analysis identities.
- [Budget starvation] → enforce family/global reservations with deferred, replayable receipts.

## Migration Plan

1. Merge and deploy repository intake foundation.
2. Pin the published social-contract revision and deploy schema/resources.
3. Begin consuming retained social outbox events; replay is idempotent.
4. Pause consumer on rollback; source evidence and receipts remain unchanged.
