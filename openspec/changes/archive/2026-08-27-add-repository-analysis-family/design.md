## Context

See proposal.md. Article pipeline types are currently hard-coded to `Document` and `ArticleAnalysis`; the database already has durable runs, attempts, budget, search projection, and embedding mechanics, but no inbox or family-neutral source projection.

## Goals / Non-Goals

**Goals:** introduce a small typed family abstraction over durable run identity, one transactional inbox, repository prompt/schema/context resources, and a search projection that works for non-Document evidence.

**Non-Goals:** source retrieval, broker transport, embedding model changes, client APIs, or changing accepted article output.

## Decisions

### D1. Family-specific typed contracts remain separate

Repository analysis receives its own Rust type, strict schema, prompt directory, and deterministic builder. A generic summary object is rejected because fields and citation semantics differ by source family.

### D2. One inbox receipt drives idempotency

The database stores delivery ID, subject, source identity/digest, causal metadata, and outcome in the same transaction that creates a run. A unique delivery ID blocks redelivery; a run identity including family, source digest, contract, prompt, context and model policy blocks semantically equivalent replay.

### D3. Generalize internal evidence, not external source ownership

Family-neutral internal `PreparedSource`/projection inputs carry immutable IDs, tenant, owner context, title/body and citation spans. Repository event payloads are decoded at the edge; an authorized BlobStore resolver loads the event's immutable README reference, while Knowledge never queries Catalog tables or fetches README URLs.

### D4. Budget is reserved before model work

The existing ledger becomes the common admission point. A reservation records family and run identity before `model_requested`; settle/release occurs from stored usage so retry/replay cannot double charge.

## Risks / Trade-offs

- [Repository contract publication is delayed] → do not merge the Knowledge dependency until its immutable revision is published.
- [Generic refactor changes article behavior] → characterize existing article tests before extraction and retain article schema/prompt bytes unchanged.
- [Out-of-order events] → latest projection ordering is source revision-aware and independent from inbox order.

## Migration Plan

1. Publish the repository-analysis request contract and deploy Catalog README acquisition plus its outbox producer.
2. Edit the current Knowledge schema in place and deploy the inbox/run extensions.
3. Deploy Knowledge consumer after Catalog; replay retained outbox rows.
4. Roll back by pausing consumption; receipts and source/run evidence remain available for deterministic resumption.
