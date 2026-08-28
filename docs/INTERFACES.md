# Knowledge interfaces

## Current process interface

The loopback admin listener exposes only:

- `GET /live`;
- `GET /ready`;
- `GET /metrics`;
- `GET /version`.
- `GET /internal/search?tenant=<tenant>&q=<query>&read_state=read|unread&limit=<n>&offset=<n>`;
- `POST /internal/user-content/command`, including the narrow `set_read_state` operation;
- `GET /internal/user-content/collection`.

Every response uses `Cache-Control: no-store`. Search returns accepted analysis identity, effective
read state, and bounded page facts under an explicit tenant; the read-state command preserves
favorite and hides foreign targets as missing. These routes remain loopback-only and are consumed
through Platform's authenticated public facade defined by workspace contract
`library-search-read-state`. There is no public Knowledge HTTP route. The process accepts strict
`RATATOSKR__` configuration and a `check-config` command that does not bind a socket.

## Current library interfaces

- `prepare_context` consumes canonical Document IR and keeps complete blocks in source order.
- `build_generation_request` separates fixed policy, task text, untrusted source content, and the
  generated output schema.
- `LlmProvider` is the narrow structured-generation seam. Implementations: `ScriptedProvider` (tests
  and offline default), the `OpenRouter` chat-completions adapter, and the `ControlledProvider`
  wrapper that orders rate limiting, budget enforcement, execution, usage recording, and bounded-fact
  logging. Every implementation declares a stable provider/model identity.
- `OpenRouterProvider` enforces a per-try deadline, streams responses under a byte cap, retries only
  network, rate-limit, and server faults with jittered backoff, and accepts HTTPS or loopback
  plain-text base URLs. Its credential exists only inside the authorization header and redacts itself
  in every diagnostic surface.
- `BudgetLedger` records one durable `knowledge.provider_usage` row per real response and refuses
  calls whose conservative projection would exceed the daily or monthly token or cost ceiling in UTC
  windows.
- `RepositoryAnalysisConsumer` durably records
  `knowledge.repository_analysis.requested.v1` deliveries, preserves one pending request per
  immutable idempotency digest, and constructs a matching terminal completion or failure fact once.
  It never calls a model: the future worker composes provider execution through the existing shared
  rate and budget controls.
- `ArticlePipeline` records attempts with adapter identity, model, latency, HTTP status, and failure
  class; stores raw bytes before parsing; validates the result; and persists one accepted output.
- `BlobStore` owns content-addressed raw responses and returns contract `BlobRef` values.

There are no public analysis routes, event transport adapters, or additional analysis workers in
this slice. A second real adapter implements the same seam without further trait changes.
