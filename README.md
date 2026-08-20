# Ratatoskr Knowledge

`ratatoskr-knowledge` is the interpretation and retrieval bounded context for Ratatoskr. It turns already captured, provenance-preserving source material into versioned structured analyses, searchable projections, embeddings, entities, topics, and cross-source relationships.

> **Status:** architecture bootstrap. The analysis state machines, provider adapters, database schema, and search indexes described below are planned and are not implemented yet.

> [!IMPORTANT]
> **Ratatoskr is in development.** No database holds data that has to survive a schema change.
> While this status holds, these two rules replace what the documents below plan:
>
> - the API and the database keep their first version. There is no `v2` and no later major
>   version.
> - the database has no migrations. One schema definition exists, and a schema change edits it in
>   place.
>
> Only the repository owner changes this status.

## Role in Ratatoskr

Knowledge starts **after** a source-owning service has produced a stable input:

- Extractor publishes a canonical document.
- GitHub publishes repository metadata and requests repository analysis.
- X, Instagram, and Threads publish normalized social sources.
- ChatGPT and Claude publish normalized archive snapshots.
- Telegram and Platform provide interaction and operation context, not source ownership.

Knowledge does not fetch arbitrary web pages, run Chromium, synchronize provider accounts, execute Git commands, or own provider credentials.

## Core responsibilities

- versioned analysis contracts and prompt sets;
- article, repository, social, and AI-conversation analyses;
- summaries, key ideas, entities, facts, statistics, topics, and relationships;
- deterministic validation and bounded repair of structured model output;
- full-text search;
- vector embeddings and semantic search;
- hybrid ranking and domain-aware filters;
- cross-provider linking and deduplication;
- index reconciliation and reprocessing based on content hashes;
- storage of raw model responses and reproducibility metadata.

## Explicit state machines

Ratatoskr intentionally avoids rebuilding framework-driven graph orchestration. Each analysis type uses an explicit, durable state machine:

```text
queued
  -> context_prepared
  -> model_requested
  -> response_received
  -> schema_validated
  -> repaired, if required
  -> persisted
  -> indexed
  -> completed
```

Failure and cancellation states are explicit, retryable transitions are recorded, and every step can be inspected independently.

Each analysis run records:

- source identity and content hash;
- analysis contract name and version;
- prompt version;
- provider and model identifiers;
- generation parameters;
- raw response blob;
- parsed structured result;
- validation and repair errors;
- token usage and estimated cost;
- attempt reason and retry lineage;
- creation, completion, and indexing timestamps.

## LLM provider boundary

LLM providers are adapters inside this bounded context, not independent Ratatoskr microservices:

```rust
pub trait LlmProvider {
    async fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> Result<serde_json::Value, ProviderError>;
}
```

Initial adapters may target OpenAI-compatible endpoints, Anthropic, OpenRouter, or local inference. Provider-specific transport behavior remains behind the interface; the analysis contract and validation pipeline remain provider-independent.

Model output is treated as untrusted data:

1. request a structured response where supported;
2. validate the returned JSON against the versioned schema;
3. deserialize into the typed result;
4. perform bounded semantic and structural checks;
5. permit a small, explicit repair budget;
6. persist both raw evidence and accepted result.

## Analysis families

### Documents and articles

Document analysis consumes the canonical Document IR emitted by `ratatoskr-extractor`. It may produce:

- concise and extended summaries;
- key arguments and ideas;
- named entities and relationships;
- notable facts and statistics;
- topics and language;
- reading-time estimates;
- claims linked to source blocks;
- related-source suggestions.

Source block references preserve provenance and allow clients to navigate from an analysis result back to the exact document segment.

### GitHub repositories

Repository analysis consumes GitHub-owned metadata and README material without becoming the owner of starred state or backup policy. A versioned result may include:

- project purpose;
- target audience and use cases;
- technology stack;
- architectural summary;
- key concepts and patterns;
- important dependencies;
- maturity and maintenance signals;
- confidence and hallucination-risk indicators.

The resulting analysis is returned to `ratatoskr-github` by event and indexed for search.

### Social content

Social analysis consumes the normalized `SocialSource` contract and respects acquisition authority. It may analyze:

- post text and long-form content;
- quoted, replied-to, or reposted relationships;
- linked external documents;
- media metadata;
- user notes and local capture context.

When a post links to an article, the service can create a composite analysis that clearly separates what the post says from what the linked article contains.

### ChatGPT and Claude archives

AI-archive analysis operates on normalized projects, conversation graphs, messages, artifacts, and attachments. Planned capabilities include:

- full-text and semantic search across providers;
- topic clustering;
- decision and action-item extraction;
- linking conversations to GitHub repositories and documents;
- detecting duplicate or closely related discussions;
- project-level digests;
- local summaries that never modify the original archived snapshot.

Raw provider exports and normalized source records remain owned by the respective archive service.

## Data ownership

Knowledge owns a `knowledge.*` PostgreSQL schema. Expected tables include:

```text
analysis_contracts
prompt_versions
analysis_runs
analysis_attempts
analysis_results
entities
entity_mentions
topics
source_topic_links
relationships
search_documents
embeddings
index_jobs
outbox_events
inbox_events
```

Large raw responses and optional derived artifacts are stored in the content-addressed BlobStore. Knowledge references source records by opaque identifiers and content hashes; it does not read another service's schema directly.

## Search architecture

The default self-hosted profile uses:

- PostgreSQL full-text search;
- `pgvector` embeddings;
- hybrid lexical and semantic ranking;
- source, provider, language, date, owner, project, topic, and collection filters.

Qdrant remains a replaceable optional adapter rather than a mandatory deployment dependency. The first implementation should prefer one authoritative persistence and indexing path until real scale measurements justify a separate vector database.

### Hybrid retrieval

A query may combine:

- PostgreSQL text rank;
- vector similarity;
- recency and activity signals;
- source authority;
- exact metadata filters;
- domain-specific boosts;
- minimum-quality thresholds.

Hydration always rechecks tenant ownership in PostgreSQL, even when candidate IDs originate from a vector index.

## Reprocessing and idempotency

A deterministic key identifies a derived result:

```text
source_id
source_content_hash
analysis_contract_version
prompt_version
model_policy_version
```

Unchanged inputs do not trigger unnecessary model calls. New prompt or contract versions create new immutable analysis runs rather than overwriting historical evidence.

Consumers are idempotent under at-least-once event delivery. Index reconciliation repairs missing or stale projections without treating the vector index as the source of truth.

## Commands and events

Expected contracts include:

```text
knowledge.analysis.requested.v1
knowledge.analysis.completed.v1
knowledge.analysis.failed.v1
knowledge.index.requested.v1
knowledge.index.completed.v1
knowledge.reconcile.requested.v1
knowledge.relationship.observed.v1
```

Source-specific request events may carry a typed analysis kind while sharing the same operation and causality model.

## Security and privacy

1. Knowledge never receives upstream OAuth tokens or browser sessions.
2. Tenant ownership is enforced on every source lookup and search result.
3. Raw model responses are encrypted or access-controlled as sensitive derived data.
4. Prompts do not include unrelated account data.
5. External model calls use explicit provider policy, redaction policy, and cost limits.
6. Local-only inference can be selected for sensitive analysis classes.
7. Model output is not trusted until schema and domain validation pass.
8. Search indexes never bypass authoritative access checks.

## Observability

Core metrics include:

```text
analysis_duration
analysis_failures
analysis_validation_failures
analysis_repair_attempts
analysis_token_usage
analysis_estimated_cost
analysis_queue_lag
embedding_duration
index_reconciliation_lag
search_duration
search_result_count
search_hybrid_fallbacks
```

Traces link source events, model attempts, validation, persistence, indexing, and user-facing operations.

## Non-goals

- Fetching or rendering web pages.
- Provider OAuth or account synchronization.
- Git cloning, backup retention, or restore verification.
- Owning source-of-truth projects, repositories, bookmarks, or conversations.
- Replacing raw archived content with a generated summary.
- A generic autonomous-agent platform.
- Hiding model cost or validation failures behind a graph framework.

## Initial milestones

1. Define the analysis-run and prompt-version schemas.
2. Implement provider-independent structured generation and validation.
3. Add the first article analysis contract.
4. Add PostgreSQL full-text indexing.
5. Add `pgvector` and hybrid retrieval.
6. Add repository analysis.
7. Add social-source analysis and linked-document composition.
8. Add ChatGPT and Claude archive indexing.
9. Add reconciliation, cost reporting, and shadow comparison with the legacy system.

## Workspace integration

`ratatoskr-workspace` pins Knowledge with compatible contracts, producers, and client projections. Contract changes use cross-repository changesets. The service remains independently buildable and testable with fixtures for documents, repositories, social sources, and AI archives.

## Project status

This README defines the intended Knowledge bounded context. It does not claim that any model integration, prompt, search index, or database schema has already been implemented.
