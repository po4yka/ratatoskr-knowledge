## Why

The first article-analysis slice proves its controls only against a scripted fake. Production analysis
needs one real inference path whose latency, failure modes, spend, and privacy behavior are bounded by
the repository's controls before further analysis families rely on it.

## What Changes

- Add an OpenRouter-compatible chat-completions HTTP adapter (`crates/knowledge`) implementing the
  existing narrow provider seam; OpenRouter first because one key covers many upstream models.
- Wrap the adapter in hard controls: per-call timeout, streaming response byte cap, jittered retry for
  transient transport failures only, a fixed-spacing rate limiter, and a durable daily/monthly token
  and estimated-cost budget ledger in the owned `knowledge` schema.
- Enforce budgets before each call with a conservative request projection; record actual usage and
  estimated cost after each response.
- Augment durable attempt records with adapter facts: provider identity, concrete model, latency,
  HTTP status, and a closed failure-class vocabulary.
- Add strict environment-only configuration for the adapter and its limits, including an API key
  type that redacts itself and is never logged or persisted.
- Keep every default test offline: wire-format contract tests run against recorded fixtures and a
  local hand-written fake transport; a live API smoke check ships as a manually-run example outside
  the gate.
- No additional adapters, prompt changes, search/embeddings work, or public endpoints in this change.

## Capabilities

### New Capabilities

- `real-model-providers`: the OpenRouter wire contract, transport controls (timeout, size cap,
  transient-only retry, rate limiting), the durable budget ledger, cancellation-consistent execution,
  structured attempt facts, and privacy rules for credentials and content.

### Modified Capabilities

None. Existing scripted-pipeline requirements keep their behavior; the adapter composes behind the
same provider seam and the pipeline keeps its two-attempt budget.

## Impact

- `crates/knowledge`: provider trait gains provider identity and a structured failure type (one-time
  seam change so later adapters need none); new adapter, control-wrapper, budget-ledger, and
  rate-limiter modules; new recorded fixtures and a fake HTTP transport for tests.
- `schema.sql`: edited in place (no migrations) — `knowledge.analysis_attempts` gains model, latency,
  status, and failure-class columns; new `knowledge.provider_usage` ledger table.
- Workspace dependencies: one pinned `reqwest` (rustls) HTTP client; no SDKs.
- `services/knowledge`, prompts, contracts, search paths: unchanged.
- Default tests and CI remain credential-free; the live example requires an operator-supplied key.
