# Local measurements

Measured on 2026-08-26 in the dedicated change worktree. Wall-clock values include Cargo process
startup and any shared build-cache lock wait; they are evidence for this local machine, not a
throughput promise.

| Operation | Command | Wall clock | Outcome |
| --- | --- | ---: | --- |
| Offline evaluation corpus | `build-gate -- cargo run --locked -p ratatoskr-knowledge --example eval_harness >/dev/null` | 1.76 s | two labels, four recorded responses, all 20 checks passed |
| Tenant deletion plus idempotent rerun | `KNOWLEDGE_TEST_DATABASE_URL=postgres://knowledge:knowledge@127.0.0.1:15435/knowledge build-gate -- cargo nextest run --locked -p ratatoskr-knowledge --test deletion deleting_a_tenant_leaves_the_survivor_and_ledger_intact` | 2.31 s | pass; test includes the zero-count rerun and audit receipt |
