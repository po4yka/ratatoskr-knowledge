## ADDED Requirements

### Requirement: A note file change is itself the analysis request

Knowledge SHALL create analysis and search entries from `evt.notes.file.changed.v1` events of the
`ratatoskr-notes` producer, keyed by `file_id` plus revision, as the workspace spec
`notes-analysis-intake` defines. Knowledge SHALL analyse only files of the media type
`text/markdown`, and SHALL acknowledge other media types with no run and no error.

#### Scenario: One revision creates one run

- **WHEN** Knowledge receives a `created` event and then two redeliveries of it
- **THEN** exactly one analysis run exists for that `file_id` and revision, verified by `services/knowledge/tests/notes_intake.rs::one_run_per_file_revision`

#### Scenario: An image is changed

- **WHEN** Knowledge receives an event for `Attachments/photo.png`
- **THEN** no analysis run is created, the message is acknowledged, and no error is logged, verified by `services/knowledge/tests/notes_intake.rs::non_markdown_file_creates_no_run`

### Requirement: Deletion and disconnection remove derived data

On a `deleted` event, Knowledge SHALL remove the analyses, embeddings and search entries of that
file. On `evt.notes.vault.disconnected.v1`, Knowledge SHALL remove them for every file of that
connection. A later redelivery of an older event SHALL NOT recreate derived data for a removed file.

#### Scenario: A connection is removed

- **WHEN** a connection with 30 analysed files is removed
- **THEN** no search query of the owner returns any of those files, and no embedding of them remains, verified by `services/knowledge/tests/notes_intake.rs::disconnect_removes_all_derived_data`

#### Scenario: An old event arrives after a delete

- **WHEN** an `updated` event with a lower connection sequence is redelivered after the `deleted` event of the same file
- **THEN** Knowledge creates no run and no search entry for it, verified by `services/knowledge/tests/notes_intake.rs::older_event_after_delete_creates_nothing`

### Requirement: Knowledge reads note bytes read-only from the owner's root

Knowledge SHALL read note bytes only from the notes blob root that `RATATOSKR__BLOBS__NOTES_ROOT`
names, read-only, through the group `ratatoskr-notes-blobs` that its unit adds. It SHALL treat a
missing blob and a digest or length mismatch as a transient failure.

#### Scenario: The bytes are missing

- **WHEN** the blob that an event names is not present in the notes blob root
- **THEN** Knowledge negatively acknowledges the message with the transient delay and creates no run, verified by `services/knowledge/tests/notes_intake.rs::missing_blob_is_transient`

#### Scenario: The unit can read the root

- **WHEN** the shipped operator example and the shipped unit are read
- **THEN** the example sets `RATATOSKR__BLOBS__NOTES_ROOT=/mnt/nvme/ratatoskr/blobs/ratatoskr-notes`, and the unit has the group `ratatoskr-notes-blobs` in `SupplementaryGroups` and the root in `ReadOnlyPaths`, verified by `services/knowledge/tests/deployment_profile.rs::the_unit_reads_the_notes_blob_root_through_its_group`
