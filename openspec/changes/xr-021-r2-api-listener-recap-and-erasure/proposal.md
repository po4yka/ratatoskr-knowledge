## Why

Round 2 of changeset XR-021 re-verified the integration defects that Round 1 left in Knowledge against `origin/main`. Platform calls the library search, read-state and capability probe on `127.0.0.1:8091` but Knowledge serves them only on its operator listener (default 9081), so every call is refused; an operator who enables the channel recap and sets nothing else serves a fixed scripted text to real users; the recap worker redelivers a failing message every 2 seconds forever, prefetches 32 messages against a 30 second ack wait and never acknowledges progress; tenant deletion leaves the six channel recap tables behind; blob registration and blob erasure race on a shared digest; and no operator example enables ingest and recap. The cross-repository behaviour is defined once in XR-021 CONTRACTS.md sections R2-01, R2-05 (c and d), R2-06, R2-07, R2-10 and R2-13; this change cites it and does not restate it.

## What Changes

- Add a dedicated API listener (`RATATOSKR__API__LISTEN_ADDRESS`, default `127.0.0.1:8091`); the operator listener keeps `/live`, `/ready`, `/metrics` and `/version` only, and readiness requires both listeners bound (R2-01).
- Default the recap provider mode to `openrouter`, require a loopback anonymous broker for `scripted` mode and warn at startup when scripted (R2-13).
- Bound recap redelivery with a growing delay and a counter, default the recap fetch batch to one and acknowledge progress while a message is being processed (R2-05 c and d).
- Make tenant deletion remove the channel recap tables and queue their raw-response digests (R2-06).
- Add one digest lock protocol shared by blob registration and blob erasure (R2-07).
- Ship `deploy/systemd/knowledge.conf.example`, a loader test for it, and refresh the stale README and DEVELOPMENT text (R2-10).
- Deliberate breaks, named in the commits that make them: `/v1/capabilities`, `/internal/search` and `/internal/user-content/*` move from the operator listener to the API listener; the recap provider mode default changes from `scripted` to `openrouter`; the recap fetch batch default changes from 32 to 1.

## Capabilities

### Modified Capabilities

- `service-foundation`: a second, API listener and an operator plane that serves only health, metrics and build identity.
- `channel-digest-recap-analysis`: provider default, bounded redelivery and progress acknowledgement.
- `privacy-deletion`: channel recap deletion and the digest lock protocol.

## Impact

Affected surfaces are `schema.sql` (edited in place, no migration), configuration, the service's HTTP surface and recap worker, the deletion module, the blob registration paths, the deploy example and the documentation. No new contract version, no migration file and no compatibility shim is introduced. Cross-repository behaviour is defined in XR-021 CONTRACTS.md.
