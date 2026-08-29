## Why

The new channel-digest service can acquire and select public-channel post revisions, but it must not perform LLM summarization. Knowledge needs a distinct, provenance-grounded recap family so digest results are reproducible, schema-validated, and safe against untrusted channel content.

## What Changes

- Add a `channel_digest_recap` analysis family conforming to workspace change `add-channel-digest-system-contract` and the published channel-digest contract crate.
- Consume replay-safe recap requests, retrieve the bounded immutable source manifest through the authenticated digest-service boundary, and verify owner, manifest digest, post identities, per-post content digests, count, and closed-open UTC window before inference.
- Persist an explicit idempotent state machine keyed by digest run, manifest digest, analysis family/version, prompt version, and language; redelivery cannot duplicate a run or model spend.
- Produce a structured recap with a bounded headline, overview, topic groups, notable items, exact source citations, coverage counts, omissions/warnings, and a stable owner-scoped result reference.
- Add a versioned prompt and deterministic context preparation that treats every post as untrusted evidence, enforces source and token budgets, and never follows instructions or URLs found in channel content.
- Emit typed completion or safe failure facts and preserve raw model output only under existing Knowledge privacy/retention rules.
- Keep MTProto acquisition, channel subscriptions, schedule execution, Bot API delivery, public API routes, provider membership writes, custom model commands, and general multi-source aggregation outside Knowledge.

## Capabilities

### New Capabilities

- `channel-digest-recap-analysis`: Durable intake, context verification, prompt/contract execution, grounding, coverage, completion/failure, cost bounds, and evaluation for channel recaps.

### Modified Capabilities

None.

## Impact

Affected surfaces include the current Knowledge schema, analysis registry/state machine, prompt resources, context retrieval client, provider-independent structured-output pipeline, outbox/inbox handlers, telemetry, fixtures, and evaluations. The schema is edited in place; no migration or second contract version is added.

The Contracts change lands first. Knowledge then deploys the dormant consumer before the digest producer emits traffic. On rollback the producer is stopped first; existing completed recap evidence remains readable, while a rolled-back Knowledge build ignores the new subject. If nothing has shipped, deleting the unmerged change is the complete rollback.
