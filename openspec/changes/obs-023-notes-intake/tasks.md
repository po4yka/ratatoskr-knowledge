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
- [x] 3.2 GREEN: read the notes blob root read-only with the configuration key `RATATOSKR__BLOBS__NOTES_ROOT`, and add the Knowledge unit with `SupplementaryGroups=ratatoskr ratatoskr-notes-blobs`. Verify 3.1 and the deployment-profile test with the new key.

## 4. A removal that overlaps an analysis (workspace tasks 10.7 and 10.8)

- [ ] 4.1 RED: `services/knowledge/tests/notes_intake.rs::disconnect_during_analysis_leaves_nothing` holds a disconnection transaction open (a statement trigger sleeps after its guard delete) while a `created` change of a new file is analysed, and asserts that no run and no search entry of the file remain after both commit; `redelivery_after_a_missed_removal_removes_the_file` analyses a file, then records the disconnection and drops the guard row as a removal that missed the analysis would leave them, redelivers the change, and asserts that no run remains. Confirm that both fail because the removal and the analysis are not serialized and a stale change removes nothing.
- [ ] 4.2 GREEN: take `pg_advisory_xact_lock_shared` on the hash of `(tenant, connection)` in the claim and in the check after the analysis, and the exclusive lock at the start of the disconnection; when the claim finds the connection removed, remove the derived data of the file (idempotent). Verify 4.1.

## 5. Only the current revision is searchable (workspace tasks 10.9 and 10.10)

- [ ] 5.1 RED: `services/knowledge/tests/notes_intake.rs::updated_note_replaces_its_search_entry` asserts that the text of the first revision is not found after an `updated` change; `rename_updates_the_title` asserts that a `renamed` change with the same revision changes the search title; `rename_to_non_markdown_removes_the_entry` asserts that a rename to `image/png` leaves no search entry. Confirm that all three fail because the projection of an earlier revision and the earlier title stay.
- [ ] 5.2 GREEN: when a note revision is persisted, retire the search projections of every other revision of the same `(tenant, ratatoskr-notes, file_id)`; set the search title from the latest change; remove the derived data of a file whose current change is not Markdown. Verify 5.1.

## 6. Bounded, encoded note context and a strict root (workspace tasks 10.11 and 10.12)

- [ ] 6.1 RED: `crates/knowledge/tests/note_analysis.rs` holds table tests of the reader and the validator (`TooLarge`, a wrong digest, a wrong length, non-UTF-8 bytes, a note above `NOTE_CONTEXT_CHARACTERS`, an excerpt that is not in the note, `text/markdown; charset=utf-8`), `note_instructions_cannot_replace_fixed_policy` (the note text appears in the context only as one JSON string and never in the policy, the task or the schema), and `the_context_cap_is_recorded_with_the_run`; `crates/knowledge/tests/config.rs::the_notes_blob_root_must_be_absolute` refuses an empty and a relative `RATATOSKR__BLOBS__NOTES_ROOT`. Confirm that the cap, the encoding, the recorded cap and the path tests fail; the other rows describe behaviour that exists and pass.
- [ ] 6.2 GREEN: refuse a note above `NOTE_CONTEXT_CHARACTERS` as permanent (`note_too_long`), encode the note text as a JSON string in the `note_context_v1` context, record the cap in the context builder version of the run, and refuse an empty or relative notes root. Verify 6.1.

## 7. Tenant deletion covers the notes guards (workspace tasks 10.13 and 10.14)

- [ ] 7.1 RED: `crates/knowledge/tests/deletion.rs::tenant_deletion_removes_the_notes_guards` seeds `knowledge.notes_files` and `knowledge.notes_disconnections` rows for two tenants, deletes one tenant, and asserts that its rows are gone, the other tenant's rows stay, and the receipt and `knowledge.deletion_records` count them. Confirm that it fails because the rows stay.
- [ ] 7.2 GREEN: delete both tables in the tenant scope, count them in `DeletionCounts` and in the new columns `notes_files_deleted` and `notes_disconnections_deleted`, and include them in the tenant survivor check. Verify 7.1.

## 8. Release artifact

- [ ] 8.1 Add the `release-artifact` job that calls the workspace's `reusable-release-artifact.yml` at a full commit SHA for the `ratatoskr-knowledge-service` binary on linux/arm64, because the repository now ships a unit; and correct `deploy/README.md` on a missing group and a missing notes root. No test: CI configuration and documentation. Verify with `zizmor` and the gate.
