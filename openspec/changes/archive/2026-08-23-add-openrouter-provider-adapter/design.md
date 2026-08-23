## Context

Items 1-4 delivered the durable article-analysis pipeline behind a narrow `LlmProvider` seam with a
scripted fake, two-attempt retry/repair budget, protected raw output, and atomic persistence. The
development status forbids migrations and second versions: `schema.sql` is edited in place. The
legacy monolith ran OpenRouter plus direct vendor adapters with per-call telemetry; this change
deliberately starts with one adapter (OpenRouter, one key over many upstream models) and designs so a
second adapter needs no further trait change.

## Goals / Non-Goals

**Goals:**

- One real HTTP adapter with hard, testable controls: deadline, byte cap, transient-only jittered
  retry, spacing rate limiter, durable token/cost budget.
- Durable evidence per attempt: identity, model, latency, status, closed failure class.
- Credential privacy by construction; content-free ordinary logs.
- All gate tests offline against recorded fixtures and a local fake transport.

**Non-Goals:**

- Direct Anthropic/Ollama adapters, prompt changes, search/embeddings, public endpoints, event
  publication, streaming responses, provider-side tool use.

## Decisions

### Evolve the seam once, now

`LlmProvider` gains `identity()` (`ProviderIdentity { provider, model }`) and returns
`Result<ProviderResponse, ProviderFailure>` where `ProviderFailure` bundles the existing
`ProviderError`, a closed `ProviderFailureClass` (`timeout`, `network`, `rate_limited`,
`server_error`, `auth_error`, `request_invalid`, `size_limit`, `budget_exhausted`), and the observed
HTTP status. This is the one breaking seam edit; later adapters implement it unchanged.
`ScriptedProvider` converts plain `ProviderError` scripts through `From`.

Alternative: keep bare `ProviderError` and side-channel diagnostics. Rejected because attempt rows
need the class and status at the point the pipeline already handles failures.

### Layer controls as composition, not inheritance

New modules in `crates/knowledge`: `openrouter` (wire serialization/parsing/classification + HTTP),
`budget` (ledger + projection), `rate_limit` (spacing), `controlled` (the `LlmProvider` wrapper that
orders limiter -> budget check -> inner call -> ledger record). The pipeline keeps owning attempts,
timeouts (outer), blobs, and validation; the wrapper owns spend/spacing facts it uniquely knows.
`OpenRouterProvider` itself stays thin: request build, capped body read, envelope parse.

Alternative: put budget SQL inside the pipeline. Rejected because every future adapter needs the same
spend policy and the pipeline must stay provider-agnostic.

### Wire contract pinned by recorded fixtures

`crates/knowledge/tests/fixtures/openrouter/*.json` hold sanitized envelopes recorded from the
documented OpenAI-compatible chat-completions shapes (success with `usage.prompt_tokens` /
`completion_tokens` and top-level `id`; error bodies as `{"error": {...}}`). Contract tests assert
exact body structure (model, two-role messages, `response_format: {"type":"json_object"}`,
`max_tokens`) and parse results; no live API in tests. Request identity prefers a response header,
falling back to the envelope id.

Alternative: an SDK crate. Rejected: one pinned endpoint, no SDK exists worth the dependency.

### Transport client

One pinned `reqwest` with `default-features = false, rustls-tls`, redirects disabled, connect
timeout from config, per-request total timeout applied to each try. Body bytes accumulate chunk-wise
and abort past `raw_response_bytes`. Base URLs must be https unless the host is loopback (tests).
Plain `tokio::TcpListener` serves the fake transport in tests - no extra dependency.

Alternative: hand-rolled HTTPS on tokio. Rejected as unsafe duplication of established crates.

### Retry policy

Bounded tries (default 3) with full-jitter exponential backoff inside `[0, min(cap, base << try)]`;
jitter derives from `RandomState`-hashed attempt counters, no new RNG dependency. Retries apply only
to `network`, `rate_limited`, `server_error` classes; `timeout` (own deadline spent), size limits,
and all permanent classes return immediately. Delay bounds default to 200 ms base / 5 s cap; tests
use zero-base policies or assert bounds only.

### Budget ledger and projection

`knowledge.provider_usage` stores one row per real response (provider, model, tokens, estimated
micro-US-dollar cost, `recorded_at`) with a `(provider, recorded_at)` index. Pre-call check sums
UTC-day and UTC-month windows per provider and refuses when
`recorded + projected > ceiling`, where projected input tokens are supplied-context characters / 4
(documented heuristic constant) and projected output equals the configured output bound. Cost uses
configured micro-US-dollars-per-million-token prices (default 0) with u128 math and ceiling
rounding. Exhaustion surfaces as `ProviderError::BudgetExhausted`, which the pipeline treats as
permanent. Ceilings default finite (2M daily / 20M monthly tokens; $5 / $50).

Alternative: post-call enforcement only. Rejected: budgets must bound work before spend, not report
overruns after it.

### Rate limiting

Fixed-spacing limiter: shared next-admission instant, admission reserves `max(now, next)` and advances
by the interval; waiting sleeps asynchronously (cancel-safe). Default 60 requests/minute.

### Configuration

New keys under `RATATOSKR__PROVIDER__OPENROUTER__{API_KEY,BASE_URL,MODEL,INPUT_MICRO_USD_PER_MTOKEN,OUTPUT_MICRO_USD_PER_MTOKEN}`
and `RATATOSKR__LIMITS__PROVIDER_{MAX_OUTPUT_TOKENS,REQUESTS_PER_MINUTE,DAILY_TOKEN_BUDGET,MONTHLY_TOKEN_BUDGET,DAILY_COST_MICRO_USD,MONTHLY_COST_MICRO_USD}`.
The key lives in a `ProviderSecret` newtype with redacting `Debug` and skipped `Serialize`; loader
errors keep naming only key + rule. Model required when a credential is present.

### Schema edits (in place)

`analysis_attempts` gains nullable `model`, `duration_ms`, `http_status`, `error_class` with bounded
check constraints; existing scripted flows leave them null except model/latency where known. New
`provider_usage` table as above. Test databases rebuild from `schema.sql`; no migration tooling
appears.

## Risks / Trade-offs

- [Characters/4 misprojects tokens for some languages] → Conservative ceiling-rounding plus
  post-call actuals keep the ledger truthful; recalibration is a versioned constant change.
- [Adapter-internal retries multiply latency under the pipeline's outer timeout] → Outer deadline
  still governs; internal retries only consume transport tries within it.
- [UTC windows differ from operator-local accounting] → Documented; window boundaries are code not
  config until a consumer needs otherwise.
- [Fake transport diverges from real OpenRouter] → Fixtures pin documented wire shapes; the manual
  live smoke example remains the operator check before spending real credit.

## Migration Plan

Recreate disposable databases from the edited `schema.sql`; nothing backfills. Rollback is revert +
recreate. Operators enable the adapter purely via environment; absence of `API_KEY` preserves
today's offline behavior.

## Open Questions

None blocking; price defaults are placeholders pending a real spend policy change.
