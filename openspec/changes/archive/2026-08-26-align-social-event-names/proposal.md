# Align social-source event names with the published contract

## Why

`docs/ARCHITECTURE.md` lists `social.source.upserted.v1` among inbound events, but the published contract defines only `social.source.captured.v1` and `social.source.updated.v1` (now plus `social.source.removed.v1`). Knowledge's own plan defers the social analysis family to separate changesets, so today this is a documentation truthfulness fix plus a pointer to the newly agreed cross-repository interface.

## What changes

- Section 16.2 names the three published social-source events instead of the never-published upserted name.
- A pointer sentence cites the `ratatoskr-workspace` store spec `social-analysis-intake`, which owns the producer/analyser boundary behaviour; this change cites rather than restates it.

skip_specs is set because no spec-level behaviour changes: documentation alignment only. The chatgpt/claude event names in the same list are out of scope — their contracts were not verified in this change.

## Tasks

Documentation cannot start from failing tests.

- [x] 1.1 Section 16.2 of `docs/ARCHITECTURE.md` lists exactly `social.source.captured.v1`, `social.source.updated.v1`, `social.source.removed.v1` for the social family and cites store spec `social-analysis-intake`. Verification: `grep -rn "social.source" docs/` shows no `upserted` spelling.
- [x] 2.1 `openspec validate --all --strict` passes; archive with `--skip-specs`; `openspec validate --archived` stays green.
