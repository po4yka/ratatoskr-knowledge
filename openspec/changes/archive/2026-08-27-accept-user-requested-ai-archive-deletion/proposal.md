## Why

ChatGPT Archive cannot safely publish owner-requested privacy tombstones until Knowledge proves that the additive reason is accepted and removes exactly the derived subject state under replay. Changeset `AIARCH-009` establishes that consumer compatibility before producer rollout.

## What Changes

- Pin the contracts revision that adds `AiArchiveTombstoneReason = user_requested`.
- Add an owner-requested tombstone fixture and an integration test proving inbox deduplication plus scoped deletion of analysis, search, and embedding state.
- Preserve the existing reason-independent deletion path: reason authorizes the fact but does not change which subject is removed.
- Add no source acquisition, raw archive storage, provider interaction, or database migration.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `ai-archive-analysis`: Accept user-requested archive tombstones and remove only the derived state named by their tenant and subject.

## Impact

- Affects the `ratatoskr-ai-archive-contracts` pin, AI-archive inbox fixture/tests, and no schema or runtime API shape.
- Contracts must publish first; this consumer gate must pass second; ChatGPT production of the reason is third.
- Rollback before producer enablement is a normal revert. Once facts may exist, keep the compatible consumer deployed; disabling future production does not justify rejecting replayed deletions.
