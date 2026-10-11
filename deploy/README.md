# Knowledge deployment

## NATS identity

`nats/identity.conf` is the Knowledge stanza of the fleet's single reviewed NATS ACL. It is a byte-equal copy of the `KNOWLEDGE` stanza of `ratatoskr-platform/deploy/nats/ratatoskr.conf`; edit it only together with Platform's copy. One seed, `/etc/ratatoskr/knowledge.nkey`, serves the ingest worker and the channel recap worker.

## Operator example

`systemd/knowledge.conf.example` is the `EnvironmentFile` of the `ratatoskr-knowledge` unit (install it as `/etc/ratatoskr/knowledge.conf`, owned by root and readable by the service group, mode 0640). It states every key a deployment must set because the flow is not enabled by default:

- ingest is **dormant unless `RATATOSKR__INGEST__ENABLED=true`**, and the example sets it, together with the GitHub README source (`http://127.0.0.1:8092/`) and its service secret;
- the channel recap is dormant unless `RATATOSKR__CHANNEL_RECAP__ENABLED=true`, and the example sets it with the `openrouter` provider, the digest source (`http://127.0.0.1:8098/`) and the result-reader secret that channel-digests presents;
- both bus workers read the same seed, `/etc/ratatoskr/knowledge.nkey`, through `RATATOSKR__CHANNEL_RECAP__BUS_CREDENTIALS_FILE` and `RATATOSKR__INGEST__BUS_CREDENTIALS_FILE`;
- the domain API listener is `127.0.0.1:8091`, where Edge routes `/v1/k`, and the operator listener is `127.0.0.1:9081`, host only.

Every secret in the file is the placeholder `CHANGE-ME`. Replace each one before starting the unit; the configuration loader refuses an enabled flow whose secret is absent but does not recognise a placeholder, so a left-over `CHANGE-ME` is a credential the peer will refuse, not a safe default. `services/knowledge/tests/deployment_profile.rs` loads the file through the service's own configuration loader and fails when a key is renamed or a flow stops being enabled, and the workspace `port_allocation.rs` checks its binds against `docs/DEPLOYMENT_TARGET.md`.

Start order on a fresh broker: reload NATS with the new ACL, start Edge (it provisions the durables), then Knowledge.
