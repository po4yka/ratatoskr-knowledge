## Context

XR-021 CONTRACTS.md R6-02 rule 5 is the contract. This file records the choices local to Knowledge.

## Decisions

### A distinct error, not a flag

`Gone` is a new variant of the `non_exhaustive` `DigestSourceClientError`, because the attempt must tell a manifest that will never come back from one that may. A 404 stays `Unavailable`: the route answers it for an absent manifest, another owner's manifest and wrong claims, which a retry cannot tell apart from a race.

### Settle outside the retry arm

`attempt_digest_manifest` already settles `InvalidConfiguration` and `InvalidRequest` at once. `Gone` joins that arm, so `schedule_manifest_retry` is never called for it and the run's attempt count is unchanged. The recap failure fact is the existing `ManifestUnavailable`, which is retryable as a new run.
