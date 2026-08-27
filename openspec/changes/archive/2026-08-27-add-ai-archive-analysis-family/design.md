## Context

See proposal.md. Archive contracts model graph nodes and parser provenance; the repository intake foundation supplies durable event receipt and budget admission but not the privacy scope for conversation content.

## Goals / Non-Goals

**Goals:** select one explicit archive graph scope deterministically, preserve provider/parser/source provenance, validate scoped citations, and project accepted output safely.

**Non-Goals:** parsing raw exports, importing assets, cross-conversation retrieval, provider account synchronization, or user-facing archive browsing.

## Decisions

### D1. Analyze one selected immutable item scope

The event selects a conversation or message revision; parent/child context is included only when the contract references it and the builder records every selected node. Broad account-level aggregation is rejected.

### D2. Privacy boundary precedes prompt rendering

The builder constructs a minimum necessary bounded context from already normalized contract evidence. Raw export blobs, credentials, unrelated projects, and hidden message branches never reach the provider.

### D3. Archive citations name graph evidence

The contract uses provider, parser revision, conversation/message IDs and selected spans. Validation rejects a citation to a node absent from the builder's selected scope.

## Risks / Trade-offs

- [Provider export changes graph shape] → published contract/parser version and source digest are part of identity.
- [Sensitive conversation leakage] → schema/prompt fixtures contain synthetic data only; normal logs store IDs and validation codes, never bodies.
- [Large conversations] → deterministic token-bounded selection records omitted node IDs and truncation; no silent whole-conversation truncation.

## Migration Plan

1. Deploy repository intake and social-safe shared ledger changes.
2. Pin published archive contracts and apply current-schema changes.
3. Consume retained archive events after release; resume from durable inbox on interruption.
4. Disable consumption for rollback without deleting raw evidence, receipts, or accepted analyses.
