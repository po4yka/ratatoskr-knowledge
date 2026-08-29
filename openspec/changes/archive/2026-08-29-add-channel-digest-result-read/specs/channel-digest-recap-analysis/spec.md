## ADDED Requirements

### Requirement: Durable recap results are retrievable only through the authenticated service boundary

Knowledge SHALL expose a loopback-only read for one completed channel-digest recap addressed by its
opaque analysis identity. The read SHALL require the dedicated channel-digest result-reader service
credential, SHALL accept no owner, tenant, provider, model, prompt, or source selector, and SHALL
return only the already validated typed recap plus its exact stored result digest. The response SHALL
be bounded, non-cacheable, and shall contain no raw provider response, prompt, source post body,
credential, internal storage location, or unrelated analysis metadata.

Absent identifiers, identifiers of another analysis family, and recap runs without a durable
completed result SHALL return the same scoped absence response. Invalid stored result bytes, a
digest mismatch, an unavailable store, or an oversized response SHALL fail closed without returning
a partial recap. Authentication failures and ordinary telemetry SHALL not contain the credential,
recap narrative, source content, or identifier supplied by the caller.

#### Scenario: Owning service reads a completed recap

- **WHEN** the authenticated channel-digest service requests the analysis identity from a published completion fact
- **THEN** Knowledge returns the exact validated recap and stored digest under the finite response bound

#### Scenario: Foreign analysis and absent recap are indistinguishable

- **WHEN** the same service requests an identifier from another analysis family and a random identifier
- **THEN** both requests receive the same non-cacheable scoped absence response with no family, owner, run, or timing facts

#### Scenario: Credential or stored result is invalid

- **WHEN** the credential does not match or the stored recap no longer validates against its digest
- **THEN** Knowledge returns no recap and ordinary telemetry contains neither secret nor recap/source content
