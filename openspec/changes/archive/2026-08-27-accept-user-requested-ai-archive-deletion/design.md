## Context

See `proposal.md` for motivation. Knowledge's current AI-archive inbox resolves deletion scope from the tombstone subject and owner, then invokes the existing transactional deletion service. It does not branch on the reason, so the runtime path is already appropriately generic; the current contract pin rejects the new token before that path is reached.

## Goals / Non-Goals

**Goals:**

- Prove the additive token deserializes and reaches the existing scoped, replay-safe deletion path.
- Keep tenant authorization and subject scope unchanged.

**Non-Goals:**

- Add another deletion engine, schema change, source acquisition, reanalysis, or provider behavior.
- Interpret `user_requested` as provider-side account deletion.

## Decisions

### 1. Advance the contract pin and add an integration fixture

The RED test first uses a `user_requested` tombstone against the old pin and fails contract deserialization. GREEN advances to the published contract commit; the same test then exercises inbox deduplication and the existing deletion transaction.

### 2. Keep reason-independent runtime routing

The subject and owner determine deletion scope. Matching on reason would duplicate logic and risk different privacy behavior for semantically equivalent authoritative removals.

## Risks / Trade-offs

- [Dependency commit is not yet reachable] → contracts merge and push before the Knowledge pin changes or its gate runs.
- [Fixture proves parsing but not deletion] → seed analysis, search, and embedding state for target and sibling, deliver twice, and assert complete scoped inventory.

## Migration Plan

After contracts publish, advance the exact git revision, run the full Knowledge gate, merge, and push before ChatGPT enables production. Once deployed, do not roll the consumer back below this pin while facts using the token can replay.
