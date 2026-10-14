## 0. OpenSpec change

- [x] 0.1 Write this change and run `openspec validate --all --strict`. Documentation cannot start from a failing test.

## 1. Intake (workspace tasks 10.1 and 10.2)

- [x] 1.1 RED: `services/knowledge/tests/notes_intake.rs::one_run_per_file_revision` delivers one `created` event three times and asserts exactly one analysis run for its `file_id` and revision; `non_markdown_file_creates_no_run` asserts no run and an acknowledged message for a PNG. Confirm that both fail because Knowledge has no consumer on `ratatoskr_knowledge_notes_files`.
- [x] 1.2 GREEN: implement the intake on `ratatoskr_knowledge_notes_files`, with the inbox key `(file_id, revision)`, the `note_analysis_v1` family, and the grants of both notes durables in `deploy/nats/identity.conf`. Verify both tests of 1.1.

## 2. Removal (workspace tasks 10.3 and 10.4)

- [x] 2.1 RED: `services/knowledge/tests/notes_intake.rs::disconnect_removes_all_derived_data` asserts that no search result and no embedding of 30 analysed files remains after `evt.notes.vault.disconnected.v1`; `older_event_after_delete_creates_nothing` asserts no run for an `updated` event with a lower sequence after a `deleted` event. Confirm that both fail because removal does not exist.
- [x] 2.2 GREEN: implement removal on `deleted` and on `ratatoskr_knowledge_notes_vaults`, with the guards `knowledge.notes_files` and `knowledge.notes_disconnections`. Verify 2.1.

## 3. The notes blob root (workspace tasks 10.5 and 10.6)

- [x] 3.1 RED: `services/knowledge/tests/notes_intake.rs::missing_blob_is_transient` loads the configuration with `RATATOSKR__BLOBS__NOTES_ROOT` and asserts a negative acknowledgement with the transient delay and no run when the blob is missing; `services/knowledge/tests/deployment_profile.rs::the_unit_reads_the_notes_blob_root_through_its_group` asserts the key in the example and the group in the unit. Confirm that both fail because the notes blob root is not configured.
- [ ] 3.2 GREEN: read the notes blob root read-only with the configuration key `RATATOSKR__BLOBS__NOTES_ROOT`, and add the Knowledge unit with `SupplementaryGroups=ratatoskr ratatoskr-notes-blobs`. Verify 3.1 and the deployment-profile test with the new key.
