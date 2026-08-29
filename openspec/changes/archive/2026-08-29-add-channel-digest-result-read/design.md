## Context

See `proposal.md` for motivation. Knowledge already stores each accepted recap as closed typed JSON
with a SHA-256 digest and publishes that row's identifier as `analysis_ref`. The existing admin
listener is loopback-only, but its search/user-content routes are not an authority model suitable for
result export. The channel-digest service owns owner/run/result linkage; Knowledge owns recap bytes.

## Goals / Non-Goals

**Goals:**

- Preserve Knowledge as the sole owner of recap narrative while enabling an integrity-checked
  read-through projection.
- Authenticate the single service consumer independently from the reverse digest-source credential.
- Revalidate stored bytes/digest and enforce one finite response/error surface.

**Non-Goals:**

- A public Knowledge analysis API, owner lookup, search endpoint, recap mutation, model selection, or
  direct Telegram/Platform integration.
- Copying recap JSON into channel-digest, Platform, or Telegram persistence.

## Decisions

### Add one fixed loopback route beside the existing admin API

The route is `GET /internal/channel-digest-results/{analysis_id}` and accepts only a UUID path. It
uses the existing loopback listener, body/HTTP lifecycle, and `Cache-Control: no-store`. A generic
analysis route was rejected because it would widen authority and force unrelated family contracts
into one decoder.

### Authenticate with a dedicated redacted service secret

Configuration gains one optional secret value used only by the result-reader middleware. Startup
fails when result reads are enabled without a non-empty bounded secret. Comparison is constant-time,
and neither effective configuration nor `Debug` exposes the value. Reusing the digest-source secret
was rejected because the two directions have different authority and rotation paths.

### Read and validate the owned result atomically as a projection

The repository query joins a completed recap run to `channel_recap_results` by result identity and
returns the stored JSON and digest. The handler re-encodes canonical JSON, verifies SHA-256, decodes
the closed `ChannelDigestRecap` type, and then returns an explicit `{analysis_id, result_digest,
recap}` projection. This avoids exporting provider attempts, source context, or storage metadata.
Reading another service's tables or including recap bytes in the completion event were rejected as
ownership violations.

### Treat every unusable identity as scoped absence

Missing, incomplete, failed, and non-recap identities all return the same 404 body/status. Storage
unavailability and integrity failure use stable 503/502 classes without partial content. Logs carry
only finite route/outcome classes and safe correlation data.

## Risks / Trade-offs

- [The result is corrupted after initial validation] → verify the stored digest and closed type on
  every read; fail closed rather than repairing or truncating.
- [The service secret is leaked through configuration diagnostics] → keep it in the existing
  redacted secret type and add captured-debug/error tests.
- [Read-through adds a dependency hop] → pool the caller, enforce short finite time/body budgets, and
  surface retryable unavailability rather than copying results.

## Migration Plan

1. Deploy Knowledge with the new route disabled until the dedicated secret is present.
2. Configure the same secret only in the channel-digest service secret source and enable result reads.
3. Deploy the digest-service read-through projection, then Platform and Telegram consumers.
4. Roll back consumers before disabling the Knowledge route; persisted recap results remain intact.
