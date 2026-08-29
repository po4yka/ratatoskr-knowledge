## Context

See `proposal.md` and the workspace contract. Knowledge already has immutable analysis runs, bounded
provider execution, raw-response evidence, versioned prompts, typed structured-output validation,
transactional inbox/outbox patterns, and additional source-family analysis. It does not currently
have an authorized resolver for channel-digest manifests or a recap result family.

## Goals / Non-Goals

**Goals:**

- Admit each typed recap request once and bind it to verified immutable source evidence.
- Reuse the finite provider pipeline while giving channel recaps their own prompt, contract, state,
  citations, and evaluation corpus.
- Publish exactly one owner/run-bound completion or safe failure without source text on the bus.

**Non-Goals:**

- Provider acquisition, subscription/window decisions, public result APIs, notifications, or models
  selected by clients.
- Indexing channel posts into general Knowledge search in this change.

## Decisions

### Add a dedicated recap intake beside existing analysis families

A typed JetStream consumer admits `knowledge.channel_digest_recap.requested.v1` into an inbox unique
by event and semantic request identity. The durable worker creates or resumes an analysis run keyed by
owner, digest run, manifest digest, contract version, prompt version, context-policy version, and
language. Completion/failure outbox rows are committed with terminal state.

Mapping this request onto article analysis was rejected because a multi-post/window result has
different evidence, bounds, output, and citation semantics. A generic workflow engine was rejected as
unnecessary abstraction.

### Resolve and verify the manifest before provider execution

Knowledge calls the configured loopback digest API with service identity, owner, run, manifest
reference, and expected digest. It uses a finite connect/request/body budget, refuses redirects, and
verifies canonical bytes, digest, closed-open window, unique revision IDs, per-post digests, channel
and source counts, source timestamps, and reference ownership before persisting the accepted context.

Copying source bodies into the event was rejected for privacy and bus size. Reading the digest
database was rejected because it violates schema ownership. Using external post links as the source
was rejected because they are mutable and would reintroduce provider access.

### Build deterministic bounded context from complete post revisions

The builder orders channels and revisions deterministically, includes at most 100 revisions across 20
channels, preserves each revision as a complete unit, and records every omitted identity/reason. Fixed
system policy, task instruction, output schema, source labels, and untrusted source content are
separate request fields. If the token/character budget cannot include all selected revisions, the
builder drops complete lowest-priority revisions and reports reduced coverage.

Truncating individual posts was rejected because partial statements undermine citation meaning.
Following links or source instructions is forbidden because channel content is adversarial evidence.

### Define one strict recap output contract

`ChannelDigestRecap` contains the bounded headline, overview, one to five topic groups, zero to five
notable items, coverage, and warnings from the delta spec. Citations are opaque source-revision
identities during inference; Knowledge verifies membership and uniqueness, then preserves them in the
stored result. JSON Schema validation runs before semantic grounding checks and rejects unknown fields.

Free-form Markdown was rejected because Telegram cannot safely bound or ground it. Model-provided URLs
are rejected; the owning source service/Platform maps validated citation IDs to approved links.

### Reuse the finite provider budget with recap-specific identity

The recap family uses the existing bounded provider interface and raw-response-first evidence policy,
with one shared extra attempt for transient retry or schema repair. Prompt, output contract, context
policy, provider, and model identities are durable. Redelivery of a completed run returns the stored
result and emits no new provider call.

An independently configurable unlimited retry path was rejected because it can multiply spend and
prevent deterministic replay.

### Evaluate grounding, coverage, and injection resistance

Synthetic fixtures cover empty/partial/full multi-channel windows, edits, repeated posts, conflicting
claims, long posts, malformed manifests, and instruction-like content. Evaluation reports schema pass,
citation precision, unsupported-claim count, coverage, deterministic context digest, and budget use;
fixtures contain no real channel text or credentials.

## Risks / Trade-offs

- [The manifest changes after request] → verify canonical digest and persist the accepted manifest
  identity; any byte change is a terminal integrity failure.
- [Context limits omit important posts] → deterministic priority plus explicit omissions/coverage;
  tune only against the versioned evaluation set.
- [Prompt injection produces unsupported prose] → separate fixed policy/source fields, strict schema,
  citation membership validation, and fail closed on unsupported citations.
- [Digest API outage holds JetStream work] → persist retry state, nack with bounded backoff, and emit a
  safe failure only after the configured attempt/deadline budget.
- [Existing generic analysis tables cannot express manifest identity cleanly] → edit the current
  schema in place with recap-owned tables/constraints rather than overloading article source rows.

## Migration Plan

1. Pin the published Contracts revision and add RED contract/intake tests.
2. Edit `schema.sql` in place for recap inbox/run/result/outbox linkage and prove fresh-schema replay.
3. Add the dormant source client, context builder, prompt/output validator, pipeline, and evaluations.
4. Deploy the consumer before any digest producer publishes requests; readiness reports its source
   dependency separately.
5. Rollback stops producers first. Existing completed recap evidence remains readable; the old binary
   receives no new subject traffic.
