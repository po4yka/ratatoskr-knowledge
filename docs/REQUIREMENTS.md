# Knowledge requirements

## Goals

1. Produce versioned structured analyses from stable authorized source material.
2. Preserve source, prompt, context, contract, provider/model, and output identity.
3. Ground claims and citations in provided sources.
4. Index authorized content for full-text, vector, and hybrid retrieval.
5. Make quality, cost, latency, and failure observable and evaluable.

## Non-goals

Scraping, provider-account synchronization, ownership of raw provider archives, hidden autonomous agents, or authorization by search filtering after retrieval.

## Requirements

- Analysis executes as an explicit durable state machine.
- Structured output is schema-validated; repair is bounded and recorded.
- Prompt/context/model changes create new run identities rather than overwrite history.
- Citations resolve to immutable source/provenance locations.
- Retrieval applies authorization before candidate disclosure.
- Embedding and index revisions are versioned and support explicit backfill and forward repair.
- Provider calls obey privacy and per-user/global budgets.

Implemented first slice: extracted Document IR -> deterministic context -> scripted or `OpenRouter`
structured summary -> validation -> persisted analysis. FTS, vector indexing, and authorized search
remain later plan items.
