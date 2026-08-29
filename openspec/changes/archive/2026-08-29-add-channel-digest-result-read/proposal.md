## Why

The channel-digest service receives only a durable Knowledge result reference, while Platform and
Telegram need a bounded recap projection. Copying recap ownership into another schema would violate
the bounded-context boundary, so Knowledge needs a narrow authenticated read surface for the result
it already owns.

## What Changes

- Add a loopback-only, service-authenticated read for one persisted channel-digest recap by opaque
  analysis reference.
- Return the existing validated recap contract and integrity metadata under a finite response budget.
- Make absent, foreign-kind, incomplete, corrupt, and unavailable results fail closed without source
  text, provider evidence, internal storage locations, or cross-result existence disclosure.
- Document the result-source dependency used by the channel-digest service; the workspace
  channel-digest contract remains the cross-repository source of truth.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `channel-digest-recap-analysis`: add authenticated bounded retrieval of a durable validated recap
  for the owning channel-digest service.

## Impact

Affected surfaces are the Knowledge admin/loopback HTTP composition, channel recap result store,
configuration validation, readiness, interface documentation, synthetic lifecycle fixtures, and the
channel-digest service dependency. No public client route, provider credential, prompt, model
selection, source acquisition, schema migration, or new contract major is introduced.
