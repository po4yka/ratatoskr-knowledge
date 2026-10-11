## ADDED Requirements

### Requirement: The API listener serves the library surface and the operator listener does not

Knowledge SHALL serve `GET /v1/capabilities`, `GET /internal/search`, `POST /internal/user-content/command`, `GET /internal/user-content/collection` and, only when the result reader secret is configured, the channel-digest result route on a dedicated loopback API listener (`RATATOSKR__API__LISTEN_ADDRESS`, default `127.0.0.1:8091`) that differs from the operator listener, and SHALL serve only `/live`, `/ready`, `/metrics` and `/version` on the operator listener. Readiness SHALL require both listeners bound.

#### Scenario: Routes are split between the two listeners

- **WHEN** the real service is started on two ephemeral loopback ports
- **THEN** the capability document, search and user-content routes answer on the API port, `/metrics` and `/ready` answer 404 there, the same library routes answer 404 on the operator port, and `/ready` is 503 until both listeners are bound

#### Scenario: Unsafe API addresses are refused

- **WHEN** the API address is non-loopback or equals the operator address
- **THEN** configuration loading fails naming `RATATOSKR__API__LISTEN_ADDRESS`
