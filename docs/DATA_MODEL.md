# Knowledge data model

## Current owned schema

[`../schema.sql`](../schema.sql) is the one editable and idempotent schema definition. It creates
only `knowledge.*` objects:

- `source_refs` stores an immutable tenant-scoped Document IR revision, its digest, and the
  source-owner `BlobRef`;
- `analysis_runs` stores the complete source, contract, prompt, context-builder, and model-policy
  identity plus its monotonic state;
- `analysis_attempts` stores at most two ordered calls with a closed reason and outcome vocabulary,
  safe request metadata, token counts, validation code, adapter identity, concrete model, measured
  latency, HTTP status, closed failure class, and a Knowledge-owned raw-response `BlobRef`;
- `analysis_outputs` stores the accepted typed JSON and its raw-response reference;
- `provider_usage` records one row per real provider response with provider, model, input and output
  tokens, estimated cost in micro-US dollars, and recording time, indexed for UTC day and month
  windows that back the pre-call spend ceilings.
- `channel_recap_inbox` deduplicates transport and semantic owner/run/manifest requests;
- `channel_recap_runs` carries monotonic manifest, context, provider, persistence, and terminal
  states plus the immutable analysis identity;
- `channel_recap_manifests` stores the verified canonical manifest projection and its exact digest;
- `channel_recap_attempts` records at most two raw-response-first provider attempts;
- `channel_recap_results` stores one grounded typed recap and exact coverage;
- `channel_recap_outbox` stores one completion or failure until JetStream acknowledges publication.

## Constraints

The complete analysis identity is unique. Attempt ordinals are unique per run and limited to one or
two. A partial unique index permits one accepted output per run. Expected-state updates prevent a
replay from moving a terminal run backwards.

The accepted output insert and transition to `persisted` use one transaction. A replay changes a
persisted run to `completed` and returns its existing output without a provider call.

No table writes another schema or has a foreign key to another service. Channel post bodies remain
owned by the digest service; the verified manifest is bounded analysis evidence, not a cross-schema
copy or acquisition authority.
