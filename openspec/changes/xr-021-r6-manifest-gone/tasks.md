## 1. Change

- [x] 1.1 Write this change and run `openspec validate --all --strict`. Documentation cannot start from a failing test.

## 2. The client maps 410 (R6-02 rule 5)

- [ ] 2.1 RED: in `crates/knowledge/tests/channel_digest_recap_manifest.rs`, a transport that answers `410` makes `fetch_manifest` return `Err(DigestSourceClientError::Gone)`, while `404` and `503` still return `Unavailable`; add the `Gone` variant first as a signature-only change that is never produced, and confirm the test fails on the assertion.
- [ ] 2.2 GREEN: `fetch_manifest_once` returns `Gone` for status 410, before the generic non-success arm; the variant doc comment says the source no longer holds the text.

## 3. The attempt settles at once

- [ ] 3.1 RED: in the same file, with the restart-safe run store, the first `attempt_digest_manifest` against a `410` transport returns `Failed`, the run is `Failed` with `ManifestUnavailable`, the recap failure fact is queued once and `schedule_manifest_retry` was not called (the run is not in `ManifestRetry` and its attempt count is unchanged); the existing first-`503` test that returns `RetryScheduled` still passes. Confirm it fails before the fix.
- [ ] 3.2 GREEN: add `Gone` to the settle arm of `attempt_digest_manifest` beside `InvalidConfiguration` and `InvalidRequest`, outside the retry arm.

## 4. Documentation

- [ ] 4.1 `docs/INTERFACES.md` (the `DigestSourceClient` paragraph) names the `410` case. Documentation cannot start from a failing test.
