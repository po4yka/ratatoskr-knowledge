# Ratatoskr Knowledge Agent Instructions

## Scope

These instructions apply to the `ratatoskr-knowledge` repository.

This repository owns structured interpretation and retrieval over source material already acquired by other bounded contexts. It does not own source acquisition or provider synchronization.

## Repository mission

`ratatoskr-knowledge` is responsible for:

- versioned analysis contracts;
- article, repository, social, and AI archive analyses;
- summaries, entities, topics, relations, and extracted decisions;
- embeddings and chunk/index lifecycle;
- PostgreSQL full-text search, pgvector, and hybrid ranking;
- explicit durable LLM execution state;
- provenance from every analysis result back to stable source content.

Knowledge must remain reproducible, inspectable, and replaceable. It is not a generic agent framework and must not hide critical state transitions inside an opaque orchestration library.

## Current phase

The repository is in architecture bootstrap. Do not assume prompts, model adapters, migrations, evaluation datasets, search indices, or CI commands exist unless they are present in the checkout.

When creating initial implementation:

- start with explicit state machines and typed contracts;
- keep model-provider adapters narrow;
- persist enough evidence to reproduce or diagnose every result;
- avoid introducing a workflow framework merely to reproduce the legacy architecture.

### Development status

Ratatoskr is in development. No database holds data that has to survive a schema change. While this
status holds, these rules are binding, and they override anything else in this repository that
plans otherwise, including the rest of this file:

- **One version only.** The API, the database, and the contracts keep their first version. Do not
  add a `v2` or a later major version, and do not add version negotiation, deprecation windows, or
  parallel-major routing.
- **No database migrations.** Do not add a migration file, and do not add migration tooling. A
  schema change edits the current schema definition in place, and a test database is created from
  that definition.
- **The product is `Ratatoskr`.** It is not "Ratatoskr Next". Do not write that name in code,
  documentation, identifiers, comments, or commit messages.

Only the repository owner changes this status. Ask before you write anything these rules forbid.

## How a change starts

Every non-trivial change begins as an OpenSpec change rather than as an edit. In your assistant that
is `/opsx:propose <what you want to build>`, or `/opsx:explore` first when the shape is not clear
yet. The command writes `openspec/changes/<id>/` holding a proposal, the spec deltas, a design and a
task list, and you read that plan before any code is written. `/opsx:apply` builds it and
`/opsx:archive` folds the deltas into `openspec/specs/`.

`openspec/specs/` holds the behaviour that is true today, and it starts empty on purpose. A spec here
grows from a change that needed it. Do NOT convert `docs/REQUIREMENTS.md`, `docs/INTERFACES.md`,
`docs/DOMAIN.md` or `docs/DATA_MODEL.md` into specs in bulk. Those documents stay where they are, as
material an exploration reads. A spec set produced by bulk conversion is large, stale on the day it
lands, and trusted by nobody.

Behaviour that more than one repository can see — the shape of a contract, the meaning of a field, the
order in which repositories must receive a change — belongs in the `ratatoskr-workspace` store, not
here. `openspec/config.yaml` references it, so `openspec instructions` in this repository lists the
store's specs with the exact command that fetches one. Cite that spec from a local proposal instead
of restating it.

### Tests come first

The task list carries one pair per behaviour. The first task adds a test that fails. The second makes
it pass. Never one task that does both.

- Run the new test before you write the implementation, and confirm it fails for the reason the task
  states — not for a compile error or a typo.
- A refactor task comes after the tests are green. It adds no test and changes no behaviour.
- A task that cannot start from a failing test says why in one line. Configuration, documentation and
  generated files are the usual reasons.
- Do not tick a task whose test has not been run.

Nothing can check the order in which the two were written. What CI does check is
`openspec validate --archived`, which fails when a change was archived with a task left unticked, and
the step in `fleet.yml` that fails when a repository holds a manifest and a `ci.yml` that never runs
a test. `ratatoskr-workspace/docs/QUALITY_GATES.md` states that limit rather than implying it is
covered.

## The Rust skill catalogue

`.agents/skills/` holds eighteen Rust skills, and `.claude/skills/` symlinks to them, so Claude Code
and Codex read one copy. Each is a reference sheet rather than a tutorial: the commands, flags,
thresholds and triage tables for one Rust concern. Your assistant reads the descriptions and opens a
skill only when the task matches one, so the set costs almost nothing until it is needed.

`rust-tdd` is the Rust form of the task pair above. `rust-lints` owns `clippy.toml`, which is where
this repository's size limits live. `rust-security` answers a `RUSTSEC` advisory.
`rust-async-internals` covers `tokio::select!` cancel safety and shutdown. `rust-database` covers
pool budgets and transaction ownership. `rust-compiler-errors` is the entry point when the build
fails and the cause is not obvious.

`rust-database` also carries a section on deploying migrations in compatible phases. The Development
status above overrides it: while that status holds, this product has no migrations at all. Read the
rest of that skill and skip that section.

The eighteen are identical in every Ratatoskr repository whose stack is Rust, and
`ratatoskr-workspace/.github/workflows/drift.yml` fails when one copy stops matching the others. Do
not edit a file under `.agents/skills/`. A correction belongs upstream in `po4yka/rust-skills` and
reaches this repository through `npx skills update`.

The catalogue holds forty-four skills and eighteen are vendored here.
`ratatoskr-workspace/docs/QUALITY_GATES.md` records which were left out and why. They are vendored
under BSD-3-Clause, (c) 2026 Nikita Pochaev, who also owns this repository; each `SKILL.md` keeps its
`license` field, and the full text is in that repository's `LICENSE`.

## Sources of truth

Use this precedence order:

1. active task/changeset and accepted ADRs;
2. `README.md`;
3. analysis and source contracts from `ratatoskr-contracts`;
4. versioned prompt/contract definitions;
5. evaluation fixtures and repository tests;
6. implementation details.

A model response is never a source of truth about its own contract. The typed schema and validation rules are authoritative.

## Hard bounded-context rules

### Knowledge owns

- analysis run and revision state;
- prompt and contract version references;
- raw model response references;
- parsed and validated structured results;
- token/cost/latency metadata;
- embedding/chunk/index metadata;
- search documents and ranking configuration;
- analysis-specific failure and retry records;
- links between normalized sources and knowledge outputs.

### Knowledge does not own

- URL fetching or browser rendering;
- raw provider account synchronization;
- GitHub stars, X bookmarks, Instagram/Threads capture authority;
- ChatGPT/Claude raw export authority;
- Git mirrors or backup snapshots;
- provider OAuth credentials unrelated to inference;
- client collections or Telegram interaction state;
- source service database tables.

Knowledge consumes stable contracts and references. Never read another service's tables as a runtime API.

## Source integrity and provenance

Every analysis must be tied to immutable source identity, including where applicable:

- source ID and owning bounded context;
- normalized content hash;
- raw/source blob reference or stable document reference;
- source schema/version;
- extraction/archive revision;
- selected blocks/chunks and provenance spans;
- analysis contract version;
- prompt version;
- model/provider configuration.

Do not overwrite a previous analysis when the source, prompt, contract, or model changes. Create a new version/revision and preserve historical results according to retention policy.

A summary without a source hash and provenance is incomplete.

## Explicit durable state machine

Implement analysis execution as explicit persisted state. A representative lifecycle is:

```text
queued
  -> context_prepared
  -> model_requested
  -> response_received
  -> schema_validated
  -> repaired, when eligible
  -> persisted
  -> indexed
  -> completed
```

Failure, cancellation, retry, and superseded states must be explicit.

Rules:

- state transitions are idempotent and repeatable;
- at-least-once command delivery must not duplicate terminal results;
- external request IDs and idempotency keys are persisted;
- retries distinguish transient provider/network failures from permanent contract/content failures;
- raw responses are persisted before destructive parsing when policy permits;
- repair attempts are bounded and separately recorded;
- indexing failure does not erase a valid analysis result;
- progress events are monotonic or sequence-aware;
- a terminal state cannot silently regress.

Do not represent the workflow only as in-memory graph state.

## Analysis contract rules

Each analysis family has its own typed contract. Do not force articles, repositories, social posts, and AI conversations into one vague summary object.

A contract definition must include:

- purpose and source types;
- required and optional fields;
- grounding/provenance expectations;
- maximum lengths and cardinalities;
- enum and unknown behavior;
- confidence or uncertainty semantics where used;
- validation and repair policy;
- compatibility/versioning policy.

Prefer structured fields over free-form blobs when downstream behavior depends on them. Preserve a human-readable narrative only where it is a product requirement.

Do not add fields such as `data`, `insights`, or `metadata` without a precise schema and ownership.

## Prompt management

Prompts are versioned production artifacts.

- Store prompt templates in reviewable files or typed definitions.
- Assign stable prompt IDs and versions.
- Separate system policy, task instructions, source content, and tool/result data.
- Record the exact prompt version and model settings for each run.
- Do not build prompts through uncontrolled string concatenation.
- Keep provider-specific formatting in adapters.
- Treat source content as untrusted data, not instructions.
- Include explicit grounding and output-schema requirements.
- Never embed secrets or private infrastructure details in prompts.

A prompt change that can alter persisted output requires evaluation and a new prompt version. Do not mutate history by reusing an old version identifier.

## Prompt-injection and untrusted-content rules

Articles, repositories, social posts, and archived chats may contain instructions aimed at models or agents.

- Delimit source content clearly.
- Tell the model that source text is evidence, not executable instruction.
- Do not expose privileged tools to an analysis request unless a separately reviewed use case requires them.
- Do not let source content select provider credentials, internal URLs, files, or commands.
- Validate all structured output independently of model claims.
- Never allow model output to directly perform external writes.
- Record suspicious instruction patterns as diagnostics only when useful and privacy-safe.

Knowledge analysis is not an authorization boundary.

## Model provider adapters

Provider adapters should expose a narrow interface equivalent to:

```rust
trait LlmProvider {
    async fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> Result<serde_json::Value, ProviderError>;
}
```

Adapters own:

- provider request/response translation;
- authentication to inference APIs;
- provider request IDs;
- rate-limit and retry metadata;
- safe error normalization;
- usage extraction;
- supported structured-output capabilities.

Adapters do not own analysis business policy, prompts, persistence, or search indexing.

Never let a provider SDK type leak into public/domain contracts.

## Structured output validation

- Generate or maintain JSON Schema from the canonical typed contract.
- Validate raw output before deserialization into trusted domain types.
- Reject extra/unknown fields when the contract requires strictness; preserve them only through an explicit extension mechanism.
- Bound strings, arrays, nesting, and numeric ranges.
- Validate citations/provenance references against supplied source context.
- Record validation errors without exposing private content in logs.
- Permit at most the documented number of repair attempts.
- A repaired response is a separate attempt with its own usage and diagnostics.

Do not silently coerce materially invalid output into a successful result.

## Grounding and citation rules

When a result makes source-dependent claims:

- link claims or sections to source blocks/chunks where the contract supports it;
- distinguish source facts, model inference, and recommendation;
- do not cite content absent from the supplied source revision;
- preserve provider/source attribution;
- make unsupported or uncertain fields explicit rather than fabricated;
- keep quoted text within product and copyright limits where applicable.

Repository analysis should reference README/code evidence; article analysis should reference Document IR spans; social analysis should distinguish the post from linked external articles; AI archive analysis should identify the originating conversation/message revision.

## Context preparation

Context preparation is deterministic and versioned.

- Select source blocks/chunks using documented rules.
- Record selected IDs, order, token estimates, truncation, and omitted sections.
- Preserve titles, authorship, timestamps, and provenance separately from body text.
- Do not silently truncate in a way that changes meaning.
- Prefer hierarchical or map/reduce strategies for long material only when their merge semantics are explicit and tested.
- Cache prepared context by source hash plus preparation version.

Do not re-fetch the source from the Internet. Request the owning service's stable source representation.

## Embeddings and indexing

Default search architecture is PostgreSQL FTS plus pgvector with hybrid ranking.

Rules:

- embedding rows reference source/analysis content hashes and embedding model/version;
- changed content creates a new index revision or deterministic replacement, never an ambiguous partial update;
- chunk boundaries and normalization are versioned;
- failed indexing is recoverable from persisted source/analysis data;
- deletion/tombstone handling respects the owning source's authority and retention policy;
- embedding model migration supports coexistence or controlled backfill;
- search results include stable source IDs and authorization filters;
- ranking configuration is testable and not hidden in ad hoc SQL.

Do not add Qdrant or another external index to the default path without benchmark and operational evidence plus an adapter/consistency design.

## Search and authorization

Search is not allowed to bypass source ownership.

- Apply user/tenant/resource authorization before returning results.
- Ensure vector and FTS paths use equivalent filters.
- Do not leak titles/snippets from unauthorized sources through counts, autocomplete, or diagnostics.
- Keep result ranking separate from access control.
- Return source type, provenance, and revision so clients can render truthful results.
- Define staleness when an index lags source updates.

Cross-provider linking must remain explainable and must not merge distinct source records into one authoritative object.

## Cost, rate, and resource control

Every inference path must have explicit budgets:

- model allowlist;
- input/output token limits;
- per-run timeout;
- retry/repair limits;
- per-user/account/global concurrency;
- daily/monthly cost or usage policy where required;
- batch size and backpressure;
- cancellation behavior.

Record usage and estimated/actual cost without placing sensitive prompt bodies in metrics.

A cheaper or faster model change still requires contract-validity and quality evaluation.

## Persistence and migrations

Knowledge writes only its owned schema.

- No cross-schema foreign keys or writes.
- Store stable references to source objects rather than copying entire provider records without a projection rationale.
- Preserve raw response blobs separately from validated projections.
- Migrations must support rolling deployment and replay.
- Prompt/contract/model versions are immutable identifiers.
- Destructive cleanup must not remove the only evidence required to reproduce an analysis.

If a source is removed upstream, follow the source service's tombstone/retention event; do not infer deletion from a missing partial sync.

## Events

Typical outputs may include:

```text
knowledge.analysis.requested.v1
knowledge.analysis.completed.v1
knowledge.analysis.failed.v1
knowledge.index.updated.v1
```

Use canonical contracts, transactional outbox, inbox deduplication, correlation/causation IDs, and idempotent consumers.

Events should carry result references and user-safe metadata rather than full private prompts or raw model responses.

## Security and privacy

- Inference credentials remain encrypted and never enter events, logs, fixtures, or user-visible errors.
- Raw prompts/responses and archived user conversations require explicit access control and retention.
- Do not send more source content to a provider than the analysis requires.
- Respect provider/model data-handling configuration and document it.
- Redact secrets detected in source material when a contract permits; do not silently alter canonical source records.
- Never execute code, tool calls, URLs, or shell instructions produced by a model.
- Treat model output as untrusted until validated.
- Separate operational diagnostics from user content.

## Observability

Required telemetry should cover:

- queue and state-transition latency;
- provider/model request latency and failure class;
- token usage and cost;
- schema validation and repair rate;
- context truncation and chunk counts;
- indexing lag and failures;
- search latency and ranking-path usage;
- correlation IDs and source/analysis IDs in non-sensitive form.

Metrics must use bounded labels. Prompt and response text do not belong in ordinary logs or metrics.

## Evaluation and testing

When implementation exists, relevant changes should include:

- state-machine and idempotency tests;
- contract/schema validation tests;
- provider adapter tests with recorded/synthetic safe fixtures;
- prompt snapshot tests;
- prompt-injection and malformed-output tests;
- deterministic context preparation tests;
- citation/provenance validation;
- retry, timeout, cancellation, and cost-budget tests;
- embedding/chunk version and backfill tests;
- FTS/vector authorization-equivalence tests;
- hybrid ranking evaluation on representative corpora;
- migration and event replay tests;
- quality evaluations for prompt/model changes.

Do not use exact wording equality as the sole LLM quality assertion. Evaluate contract validity, grounding, missing fields, unsupported claims, and task-specific usefulness.

Never include real private user conversations or provider exports in public test fixtures.

## Cross-repository change rules

Use a workspace changeset when changing:

- analysis contracts;
- source/Document IR expectations;
- event payloads;
- search API/result contracts;
- provider/model behavior that affects persisted output;
- authorization filters consumed by clients;
- reanalysis or reindexing requirements.

The changeset must list source producers, clients/consumers, rollout order, backfill/reprocessing plan, cost impact, and rollback.

## Git and PR workflow

- Keep prompt/contract changes separate from unrelated infrastructure refactors.
- State source types and analysis families affected.
- Include evaluation evidence for prompt, model, ranking, or chunking changes.
- Document expected reanalysis/reindexing and cost.
- Do not rename versions in place.
- Do not commit secrets, raw private prompts, responses, or user archives.
- Do not claim a model is "better" without task-relevant evaluation.
- Update README/ADRs when architecture or ownership changes.

## Completion criteria

A task is complete only when:

- responsibility belongs to Knowledge;
- source identity, hash, revision, and provenance are preserved;
- analysis state is durable and idempotent;
- prompt, contract, context preparation, and model versions are explicit;
- output is independently schema-validated;
- prompt-injection and external-write boundaries remain intact;
- token, cost, timeout, retry, and cancellation budgets are enforced;
- search authorization is equivalent across FTS/vector paths;
- relevant evaluations and repository checks pass;
- migrations/backfills and cross-repository rollout are documented;
- no source-provider or scraping responsibility leaked into this repository.
