## 1. Published contract intake

- [x] 1.1 Add failing fixture-driven tests for conversation added/updated envelope admission and provenance persistence; run them and confirm the envelope consumer is absent.
- [x] 1.2 Implement typed archive lifecycle envelope admission through the durable inbox; make 1.1 pass.

## 2. Analysis linkage

- [x] 2.1 Add a failing end-to-end test that executes an admitted archive conversation and asserts the completion contract round-trips to the exact subject and digest; run it and confirm no completion exists.
- [x] 2.2 Implement the completion-linkage builder and make 2.1 pass.

## 3. Explicit deletion propagation

- [x] 3.1 Add failing tests that a conversation tombstone removes derived search data and that snapshot absence does not; run them and confirm tombstones are not consumed.
- [x] 3.2 Implement idempotent tombstone consumption and derived-data deletion; make 3.1 pass.

## 4. Validation and lifecycle

- [x] 4.1 Update the workspace change task state and local specs; no behavior test, because this synchronizes verified implementation evidence.
- [x] 4.2 Run the repository gate and OpenSpec validation, archive this change, and integrate only after both repositories are green.
