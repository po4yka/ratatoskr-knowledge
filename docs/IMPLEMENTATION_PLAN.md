# Knowledge implementation plan

- [x] 1. Scaffold the Rust service, finite config, telemetry, errors, health routes, and editable
  `schema.sql`.
- [x] 2. Implement immutable source references and the explicit idempotent analysis-run state
  machine.
- [x] 3. Define the first article-analysis contract with a versioned deterministic prompt and
  context builder.
- [x] 4. Add the scripted provider, strict validation, durable attempts, bounded retry and repair,
  protected raw output, atomic persistence, and replay.
- [x] 5. Add one real provider adapter behind privacy, timeout, rate, cancellation, and budget
  controls.
- [ ] 6. Implement PostgreSQL FTS and authorization-aware search documents.
- [ ] 7. Add pgvector embeddings, chunk and model versioning, and hybrid ranking.
- [ ] 8. Add an evaluation harness, privacy deletion, and explicit reindex jobs.
- [ ] 9. Add repository, social, and AI archive analysis families through separate changesets.
- [ ] 10. Define legacy import only if real legacy data exists and has measured value.

Items 1 through 5 are the implemented first article-analysis slice: the scripted provider proved the
pipeline and the `OpenRouter` adapter now carries real inference behind the same seam. Later items
are not part of the current runtime.
