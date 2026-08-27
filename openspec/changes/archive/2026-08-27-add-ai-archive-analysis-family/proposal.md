## Why

Normalized ChatGPT and Claude archive graphs have a published contract but no durable Knowledge interpretation. Archived conversations need message/conversation provenance and stricter privacy boundaries than article or social material.

## What Changes

- Consume published AI-archive import/conversation facts through the shared inbox using `ratatoskr-ai-archive-contracts`.
- Add archive-item-specific typed output, JSON Schema, versioned prompt/context builder, validation, shared-budget accounting, and a searchable projection referencing the exact conversation/message revision.
- Preserve at-least-once delivery, sparse replay, and out-of-order updates without duplicate analysis or accidental cross-conversation context.

## Capabilities

### New Capabilities

- `ai-archive-analysis`: Immutable ChatGPT and Claude archive items yield versioned, provenance-grounded analysis and searchable projections.

### Modified Capabilities

- None.

## Impact

- Affects Knowledge's archive-contract dependency, durable event handling, typed prompt/schema resources, budget ledger integration, and search projection.
- Requires repository-analysis intake foundation and the published archive contracts; no archive producer, raw-export parser, or client surface changes.
