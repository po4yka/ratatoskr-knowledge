# Knowledge data model

## Current owned schema

[`../schema.sql`](../schema.sql) is the one editable and idempotent schema definition. It creates
only `knowledge.*` objects:

- `source_refs` stores an immutable tenant-scoped Document IR revision, its digest, and the
  source-owner `BlobRef`;
- `analysis_runs` stores the complete source, contract, prompt, context-builder, and model-policy
  identity plus its monotonic state;
- `analysis_attempts` stores at most two ordered calls with a closed reason and outcome vocabulary,
  safe request metadata, token counts, validation code, and a Knowledge-owned raw-response
  `BlobRef`;
- `analysis_outputs` stores the accepted typed JSON and its raw-response reference.

## Constraints

The complete analysis identity is unique. Attempt ordinals are unique per run and limited to one or
two. A partial unique index permits one accepted output per run. Expected-state updates prevent a
replay from moving a terminal run backwards.

The accepted output insert and transition to `persisted` use one transaction. A replay changes a
persisted run to `completed` and returns its existing output without a provider call.

No table writes another schema or has a foreign key to another service. Search, embeddings,
entities, topics, outbox, inbox, deletion propagation, and retention automation are not implemented.
