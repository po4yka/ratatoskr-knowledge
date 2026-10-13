## Why

Round 6 of changeset XR-021 (CONTRACTS.md R6-02 rule 5) makes the channel-digests manifest route answer `410 Gone` once the manifest text is minimized by retention or withdrawal. `DigestSourceClient::fetch_manifest` maps every non-success status to `Unavailable`, and `attempt_digest_manifest` schedules one restart-safe retry for it, so a recap request would spend a retry on a manifest that never comes back. The cross-repository rule is defined once in CONTRACTS.md R6-02; this change cites it.

## What Changes

- `DigestSourceClientError` gains `Gone`; `fetch_manifest` returns it for status 410. `404` and `503` still return `Unavailable`.
- `attempt_digest_manifest` settles `ManifestUnavailable` at once on `Gone`, beside `InvalidConfiguration` and `InvalidRequest`, without the retry that `Unavailable` and `Timeout` get.
- `docs/INTERFACES.md` names the `410` case.

## Capabilities

### Modified Capabilities

- `channel-digest-recap-analysis`: manifest retrieval.

## Impact

`crates/knowledge/src/channel_digest_recap.rs`, its manifest tests and `docs/INTERFACES.md`. No contract version, no migration, no shim. Knowledge's own stored manifest copy is not reached by this change (out of owner scope, R6-05).
