# Knowledge data model

## Owned schema: `knowledge.*`

- `source_refs`: owner, source authority/type, external reference, content/provenance hash.
- `analysis_runs` and `analysis_attempts`: state, all versions, timestamps, provider/model, usage, safe errors.
- `analysis_outputs`: validated typed JSON, raw-response blob reference, quality metadata.
- `claims`, `citations`, `entities`, `topics`, and relations where contracts require them.
- `embedding_sets`, `chunks`, `vectors`, `search_documents`, `index_generations`.
- `prompt_versions`, model/provider policies, evaluations, outbox/inbox.

## Constraints

Run uniqueness includes owner, source revision, analysis family/contract, prompt, context builder, and policy/model identity. Historical outputs are immutable. Authorization scope is stored on every searchable root. Raw private text stays in authorized blobs or bounded text fields; telemetry never stores it.

Retention and deletion propagate from source authority through analyses, chunks, vectors, caches, and blobs with auditable status.
