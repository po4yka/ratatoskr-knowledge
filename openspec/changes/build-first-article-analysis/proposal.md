## Why

Knowledge has architecture documents but no executable boundary for source identity, durable analysis
state, structured output, or provider behavior. The first slice must prove those controls with a fake
provider before a real inference API or search path adds cost and failure modes.

## What Changes

- Scaffold a Rust service with typed finite configuration, safe telemetry, operator health routes,
  strict lint gates, and one editable `knowledge` schema definition with no migration tooling.
- Store immutable source references and explicit idempotent analysis-run and attempt state.
- Define one strict article-analysis result, its generated schema, and a deterministic versioned
  prompt/context builder over canonical Document IR.
- Add a hand-written scripted fake provider, raw-response content-addressed storage, independent
  structural and semantic validation, and finite transient retry and repair budgets.
- Keep real model providers, message-bus publication, search, embeddings, indexing, and reprocessing
  outside this change.

## Capabilities

### New Capabilities

- `service-foundation`: Process configuration, operator health, telemetry, schema bootstrap, and gates.
- `analysis-runs`: Immutable source references and the durable idempotent run and attempt state machine.
- `article-analysis`: Strict result shape plus deterministic prompt and context preparation from
  Document IR.
- `fake-provider-pipeline`: Scripted provider execution, raw evidence, validation, retry, repair, and
  accepted-result persistence.

### Modified Capabilities

None.

## Impact

This creates the first Rust code, PostgreSQL schema definition, owned blob directory, tests, CI, and
deployment documentation in `ratatoskr-knowledge`. It consumes the existing shared Document IR and
identifier crates at an exact commit and requires no API key in default tests or CI.
