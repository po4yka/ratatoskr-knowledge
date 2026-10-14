# Knowledge deployment

## PostgreSQL

Knowledge connects to the one `ratatoskr` database as its own role, `ratatoskr_knowledge`, with the URL `postgres://ratatoskr_knowledge:CHANGE-ME@127.0.0.1:5432/ratatoskr`. `postgres/role.sql` creates the role without a password, grants it `connect` and `create` on the database, grants it `usage` (never `create`) on schema `public`, and installs the `vector` extension into `public`. It must be run by a PostgreSQL administrator (a superuser): pgvector is not a trusted extension, so the unprivileged service role cannot run the `create extension if not exists vector` statement in `schema.sql`, and without this step the first start fails with `permission denied to create extension "vector"`. Installing the extension first makes that statement a no-op. The `usage` grant on `public` is just as necessary: Platform's `01-database-and-roles.sql` revokes every right on `public` from `public`, the `vector` type lives in `public`, and without `usage` the role cannot resolve it, so `schema.sql` fails with `type "vector" does not exist`. Set the role's password separately with `\password ratatoskr_knowledge`; no credential belongs in this repository.

Start order on a fresh host: Platform's `01-database-and-roles.sql` (creates the `ratatoskr` database), then `postgres/role.sql` as an administrator, then the first Knowledge start, which creates the `knowledge` schema from `schema.sql`.

## NATS identity

`nats/identity.conf` is the Knowledge stanza of the fleet's single reviewed NATS ACL. It is a byte-equal copy of the `KNOWLEDGE` stanza of `ratatoskr-platform/deploy/nats/ratatoskr.conf`; edit it only together with Platform's copy. One seed, `/etc/ratatoskr/knowledge.nkey`, serves the ingest worker and the channel recap worker. The identity may subscribe only to its own inbox prefix, `_INBOX_knowledge`, and every connection the service opens sets that prefix, so no other service's deliveries are readable from here and a build without the prefix cannot complete a JetStream request under this ACL. Reload NATS and deploy the services of one snapshot together.

## Operator example

`systemd/knowledge.conf.example` is the `EnvironmentFile` of the `ratatoskr-knowledge` unit (install it as `/etc/ratatoskr/knowledge.conf`, owned by root and readable by the service group, mode 0640). It states every key a deployment must set because the flow is not enabled by default:

- ingest is **dormant unless `RATATOSKR__INGEST__ENABLED=true`**, and the example sets it, together with the GitHub README source (`http://127.0.0.1:8092/`) and its service secret;
- the channel recap is dormant unless `RATATOSKR__CHANNEL_RECAP__ENABLED=true`, and the example sets it with the `openrouter` provider, the digest source (`http://127.0.0.1:8098/`) and the result-reader secret that channel-digests presents;
- both bus workers read the same seed, `/etc/ratatoskr/knowledge.nkey`, through `RATATOSKR__CHANNEL_RECAP__BUS_CREDENTIALS_FILE` and `RATATOSKR__INGEST__BUS_CREDENTIALS_FILE`;
- the domain API listener is `127.0.0.1:8091`, where Edge routes `/v1/k`, and the operator listener is `127.0.0.1:9081`, host only.

`systemd/ratatoskr-knowledge.service` is the unit (changeset OBS-023). It runs `ratatoskr-knowledge-service` as `ratatoskr-knowledge` with the supplementary groups `ratatoskr` (for `/etc/ratatoskr` and the log directory) and `ratatoskr-notes-blobs`, the read group of the notes blob root. The unit has the Knowledge blob root in `ReadWritePaths` and the notes blob root `/mnt/nvme/ratatoskr/blobs/ratatoskr-notes` in `ReadOnlyPaths`; the example names that root in `RATATOSKR__BLOBS__NOTES_ROOT`. `RATATOSKR__BLOBS__NOTES_ROOT` must be an absolute path. The workspace deploy role creates the group, the membership and the root, and the unit depends on all three: if the group `ratatoskr-notes-blobs` does not exist, systemd refuses to start the unit with status `216/GROUP`, and if the notes blob root does not exist, the `ReadOnlyPaths=` entry (it has no `-` prefix, on purpose) fails the start with status `226/NAMESPACE`. Both are host state that only the deploy role repairs. A blob that the group cannot read, because the notes service wrote it with another mode, does not stop the unit: that note change is redelivered with the class `notes_blob`.

Every secret in the file is the placeholder `CHANGE-ME`. Replace each one before starting the unit; the configuration loader refuses an enabled flow whose secret is absent but does not recognise a placeholder, so a left-over `CHANGE-ME` is a credential the peer will refuse, not a safe default. `services/knowledge/tests/deployment_profile.rs` loads the file through the service's own configuration loader and fails when a key is renamed or a flow stops being enabled, and the workspace `port_allocation.rs` checks its binds against `docs/DEPLOYMENT_TARGET.md`.

The ack waits are not tunable: `RATATOSKR__INGEST__ACK_WAIT_SECONDS` must be 120 and `RATATOSKR__CHANNEL_RECAP__ACK_WAIT_SECONDS` must be 30, the values Edge fixes on the durables; the keys exist only to state them, and the example states them.

The channel-digests API being down does not make `/ready` fail: it flips the gauge `knowledge_channel_recap_source_up` to 0 and the recap commands wait on the durable until the source answers again.

Start order on a fresh broker: reload NATS with the new ACL, start Edge (it provisions the durables), then Knowledge.

## Release artifact

The unit starts `/usr/local/bin/ratatoskr-knowledge-service`, so each commit on `main` publishes that binary for linux/arm64. The `Dockerfile` builds it on Debian 12 (glibc 2.36) in the stage `builder`, and the stage `export` holds only the binary. The CI job `artifact` builds the image natively on an arm64 runner, checks that `export` holds exactly `ratatoskr-knowledge-service` for aarch64, and starts the runtime image once against a pgvector database: `check-config`, `/ready`, the schema, SIGTERM and exit 0. The job `release-artifact` then calls `po4yka/ratatoskr-workspace/.github/workflows/reusable-release-artifact.yml`, pinned to a full commit SHA, which publishes `ghcr.io/po4yka/ratatoskr-knowledge:sha-<commit>` with `SHA256SUMS`, an SPDX SBOM and a build-provenance attestation.
