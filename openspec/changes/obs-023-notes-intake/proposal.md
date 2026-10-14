## Why

Changeset OBS-023 connects Obsidian vaults to Ratatoskr. `ratatoskr-notes` publishes
`evt.notes.file.changed.v1`, `evt.notes.vault.connected.v1` and `evt.notes.vault.disconnected.v1`,
and Platform provisions two Knowledge durables for them: `ratatoskr_knowledge_notes_files` and
`ratatoskr_knowledge_notes_vaults`. Knowledge has no consumer on either durable, so a note is never
analysed or searchable, and a removed file or vault keeps its derived data.

The cross-repository behaviour is the workspace store's change `add-notes-vault-connections`, spec
`notes-analysis-intake`, design D9 and D12, workspace tasks 10.1 to 10.6. This change cites it and
records the choices local to Knowledge.

## What Changes

- The ingest worker gets two more families, `notes_files` and `notes_vaults`. It verifies both
  Edge-provisioned durables and never creates one. Readiness needs all six families.
- A `created`, `updated` or `renamed` file change of a Markdown file is the analysis request. One
  run exists for each `(file_id, revision)`. A file of another media type is acknowledged with no
  run and no error.
- A `deleted` file change removes the analyses, the search entries and the embeddings of the file.
  `evt.notes.vault.disconnected.v1` removes them for every file of the connection. A file event with
  a lower connection sequence than the applied one, or of a removed connection, creates nothing.
- Knowledge reads note bytes read-only from the notes blob root, `RATATOSKR__BLOBS__NOTES_ROOT`. A
  missing blob, or a blob whose digest or length differs from the event, is negatively acknowledged
  with the transient delay.
- A new analysis family, `note_analysis_v1`, with the prompt `note_prompt_v1`.
- `deploy/nats/identity.conf` gets the grants of the two durables, byte-equal to Platform's
  KNOWLEDGE stanza. A new unit, `deploy/systemd/ratatoskr-knowledge.service`, has
  `SupplementaryGroups=ratatoskr ratatoskr-notes-blobs` and the notes blob root in `ReadOnlyPaths`.

## Capabilities

### New Capabilities

- `notes-analysis-intake`: the Knowledge side of the workspace spec of the same name.

## Impact

`crates/knowledge` (configuration, schema, the note analysis family, the notes inbox),
`services/knowledge` (the ingest families and the notes handlers), `schema.sql`, `deploy/`, the
tests and `README.md`. The workspace harness tests that read the Knowledge durables and the
Knowledge ACL stanza must list the two new durables. No contract version, no migration and no shim.
