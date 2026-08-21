## Context

The repository has no code. The first implementation must consume the already published Document IR
and `BlobRef` types, persist owned state in PostgreSQL 17, and prove provider policy with no external
inference service. The development rule forbids migrations: one editable `schema.sql` creates a
disposable database. See `proposal.md` and the four delta specs for behavior.

## Goals / Non-Goals

**Goals:**

- Establish the smallest independently buildable Rust service and library boundary.
- Make source identity, state transitions, attempts, raw evidence, and accepted output durable.
- Keep provider output untrusted until structural and semantic checks pass.
- Run every default gate without an API key or external model request.

**Non-Goals:**

- Message-bus commands or result events.
- A real provider SDK, production token cost, or provider-specific retry policy.
- FTS, pgvector, indexing, authorization-aware queries, backfill, or reanalysis jobs.
- Generic analysis-family or workflow frameworks.

## Decisions

### Use two workspace packages

Create `crates/knowledge` for configuration, article analysis, provider orchestration, owned blob
storage, and PostgreSQL access, plus `services/knowledge` for the process and admin listener. Modules,
not extra crates or single-implementation traits, separate concerns inside the library. The provider
trait is the one required seam because tests need a fake and a later real adapter will replace it.

Alternative: copy the seven-crate Extractor layout. Rejected because this first slice has one analysis
family, one database, and no bus; the extra package boundaries would carry no independent policy.

### Pin the existing shared contracts and schema tooling

Pin `ratatoskr-identifiers` and `ratatoskr-document-contracts` to the published contracts commit.
Keep `schemars = 1.2.2` exact and use its generated schema as the source for JSON Schema validation.
Use the workspace's existing Rust, Serde, Tokio, SQLx, Axum, Figment, SHA-256, tracing, and metrics
patterns. Add no model SDK or mocking framework.

Alternative: copy Document IR types locally. Rejected because that creates a second wire authority.

### Keep one editable owned schema

`schema.sql` creates `source_refs`, `analysis_runs`, `analysis_attempts`, and `analysis_outputs`
under `knowledge.*`. A natural unique key on the complete analysis identity makes request creation
idempotent. Check constraints hold state and attempt vocabularies. A partial unique index allows one
accepted output per run. SQL updates include the expected current state so replays cannot regress a
terminal run.

Alternative: add migration tooling because SQLx supports it. Rejected by the binding development rule.

### The first result stays small

`ArticleAnalysis` contains only `summary` and `key_points`; each `KeyPoint` contains text and source
block indexes. Run metadata holds source and version identity instead of asking the model to repeat it.
Serde rejects unknown fields. Generated JSON Schema checks shape first; Rust validation then enforces
Unicode character counts, cardinality, unique citations, and membership in supplied block indexes.

Alternative: add entities, topics, confidence, and recommendations now. Rejected because no current
consumer requires them and each adds grounding and evaluation policy.

### Context preparation preserves complete blocks

The version-one builder serializes title, language, and supported blocks in order into a distinct
source field. It adds a block only when the whole normalized block fits the character budget, then
records all omitted indexes. Fixed system policy and task text are separate fields and identify
themselves as version one. The provider request also carries the generated output schema.

Alternative: cut the final block at the byte limit. Rejected because silent partial statements can
change meaning and break grounding.

### Raw response storage precedes parsing

The provider returns bounded raw bytes and safe metadata. The library stores the bytes under its own
SHA-256 content address and constructs an owner `ratatoskr-knowledge` `BlobRef` before parsing. The
attempt row is updated with the reference and response-received state. Validation errors store only
bounded codes and JSON paths, never response text.

Alternative: store only invalid output. Rejected because successful output is also evidence needed to
reproduce and diagnose a result.

### One additional call covers retry or repair

Each run allows two provider calls total. A typed transient failure may consume the second call as a
retry. A structurally or semantically repairable response may consume it with bounded validation codes
added to the repair task. A permanent failure, raw-size failure, or second invalid response ends the
run. Provider execution uses one finite timeout per call; no sleep or backoff is needed for the fake.

Alternative: independent retry and repair budgets. Rejected because they can multiply calls before a
real provider, cost model, or observed failure distribution exists.

### Accepted persistence is one transaction

The accepted typed JSON insert and transition to `persisted` share one transaction. A following
idempotent transition marks `completed`. If process shutdown happens between them, a replay completes
the persisted run without another provider call.

## Risks / Trade-offs

- [Two packages can grow too broad] → Split only after a dependency or deployment boundary becomes
  real; keep current module and file length gates enforced.
- [The simple context budget omits useful later blocks] → Record omissions; add selection strategies
  only with an evaluation set in a later change.
- [The fake does not prove real transport behavior] → The real-provider change must add provider
  timeout, rate, cancellation, and safe error tests before deployment.
- [Schema and typed state can drift] → Exercise the real PostgreSQL schema and all legal and illegal
  transitions in integration tests.

## Migration Plan

Create the disposable Knowledge database from `schema.sql`, start the admin-only process, then run
the fake-provider analysis tests. No bus subscribes and no client consumes results. Roll back by
stopping the process, reverting the commit, and recreating the development schema and owned blob root.
