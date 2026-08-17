# Knowledge implementation plan

1. Scaffold Rust service, config, telemetry, errors, health, and `knowledge` migrations.
2. Implement source references and explicit analysis-run state machine.
3. Define first article-analysis contract with versioned prompt/context builder.
4. Add fake provider, structured validation, attempts, bounded retry/repair, and protected raw output.
5. Add one real provider adapter behind policy/budget controls.
6. Implement PostgreSQL FTS and authorization-aware search documents.
7. Add pgvector embeddings, chunk/model versioning, and hybrid ranking.
8. Add citations, evaluation harness, privacy deletion, and reindex jobs.
9. Add repository/social/AI archive analysis families incrementally.
10. Backfill legacy summaries and compare quality/cost before cutover.

Definition of Done: contracts, state, grounding, evaluation gates, authorization, budgets, migrations, telemetry, and workspace integration pass. Deferred: broad autonomous tools and unbounded chat-over-everything.
