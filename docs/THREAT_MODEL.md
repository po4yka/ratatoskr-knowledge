# Knowledge threat model

## Assets

Private documents/conversations, model credentials, prompts, derived insights, embeddings, search authorization, budgets, and audit records.

## Threats and controls

- **Prompt injection:** separate trusted instructions from quoted source data; no implicit tools; explicit allowlists.
- **Cross-user retrieval leak:** owner filters at storage/query boundaries before ranking, plus adversarial tests.
- **Sensitive provider disclosure:** minimization, policy routing, encryption, redaction, and outbound audit.
- **Hallucinated/unsupported claims:** structured contracts, citations, grounding evaluation, and UI provenance.
- **Index poisoning:** source authority/version validation and immutable provenance.
- **Cost/availability abuse:** quotas, bounded context, retries, concurrency, cancellation, and circuit breakers.
- **Raw response/log leak:** protected BlobStore, strict access, safe errors, no content labels.
- **Embedding inversion risk:** treat vectors as sensitive and delete/authorize like source text.

Re-review for tools/agents, new providers, external search, shared collections, local models, or cross-user collaboration.
