//! Note file changes against a real `JetStream` and database (changeset OBS-023, workspace change
//! `add-notes-vault-connections`, spec `notes-analysis-intake`).
//!
//! The two notes durables are provisioned here as Platform provisions them. The note bytes are
//! written into a disposable notes blob root in the layout of `ratatoskr-notes`.

mod ingest_support;
#[path = "log_capture/mod.rs"]
mod log_capture;
mod notes_support;

use std::sync::Arc;

use ratatoskr_knowledge::{
    ChunkPolicy, Config, EMBEDDING_STORAGE_DIMENSIONS, Indexer, IndexerLimits,
    NOTE_CONTEXT_CHARACTERS, ScriptedEmbeddingProvider, ScriptedEmbeddingSuccess, SearchQuery,
    search_page,
};
use ratatoskr_knowledge_service::Metrics;

use crate::ingest_support::{TestResult, eventually};
use crate::log_capture::capture;
use crate::notes_support::{
    BROKER, CONNECTION, Change, EXCERPT, FILE, FILES_DURABLE, FILES_SUBJECT, NOTE_TEXT, OWNER,
    Setup, VAULTS_DURABLE, analyses, envelope, event_id,
};

#[tokio::test]
async fn one_run_per_file_revision() -> TestResult {
    let _serial = BROKER.lock().await;
    let notes = Setup::start().await?;
    let revision = notes.store_note(NOTE_TEXT.as_bytes())?;
    let provider = analyses(1);
    let worker = notes.spawn(Arc::clone(&provider));
    let created = Change {
        file_id: FILE,
        sequence: 1,
        kind: "created",
        path: "Projects/Garden.md",
        revision: &revision,
        media_type: "text/markdown",
        length: NOTE_TEXT.len(),
    };
    let event = event_id(1);
    // The same event three times, under three broker message ids: one delivery and two
    // redeliveries of it, as Knowledge sees them.
    for copy in 0..3 {
        notes
            .publish(
                FILES_SUBJECT,
                &format!("{event}-{copy}"),
                created.envelope(&event),
            )
            .await?;
    }

    assert_eq!(
        notes.settle(FILES_DURABLE, 3).await?,
        (0, 0, 3),
        "the notes files durable delivered every copy and holds none open"
    );
    assert_eq!(
        notes.runs_for(FILE, &revision).await?,
        1,
        "one analysis run for the file revision"
    );
    assert_eq!(provider.call_count()?, 1, "the note is analysed once");
    notes.finish(worker).await
}

#[tokio::test]
async fn non_markdown_file_creates_no_run() -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let notes = Setup::start().await?;
    let image = b"\x89PNG\r\n\x1a\n not really an image";
    let revision = notes.store_note(image)?;
    let worker = notes.spawn(analyses(0));
    let created = Change {
        file_id: FILE,
        sequence: 1,
        kind: "created",
        path: "Attachments/photo.png",
        revision: &revision,
        media_type: "image/png",
        length: image.len(),
    };
    notes
        .publish(FILES_SUBJECT, &event_id(2), created.envelope(&event_id(2)))
        .await?;

    assert_eq!(
        notes.settle(FILES_DURABLE, 1).await?,
        (0, 0, 1),
        "the image change is delivered once and acknowledged"
    );
    assert_eq!(
        notes
            .count("select count(*) from knowledge.analysis_runs")
            .await?,
        0,
        "no analysis run for an image"
    );
    assert_eq!(
        logs.lines_containing("ingest delivery"),
        0,
        "the image change is neither rejected nor redelivered: {}",
        logs.text()
    );
    notes.finish(worker).await
}

/// The file id of the `index`th test file.
fn file_id(index: u32) -> String {
    format!("018f0000-0000-7000-8000-00000001{index:04x}")
}

/// Embeds every persisted analysis, as the background indexer does.
async fn index_embeddings(setup: &Setup) -> TestResult {
    let indexer = Indexer::new(
        &setup.database.database,
        ScriptedEmbeddingProvider::new(
            u16::try_from(EMBEDDING_STORAGE_DIMENSIONS)?,
            (0..64).map(|_| Ok(ScriptedEmbeddingSuccess { input_tokens: 12 })),
        ),
        ChunkPolicy::new(1_600, 200)?,
        IndexerLimits {
            batch_sources: 8,
            max_input_characters: 120_000,
            max_failure_attempts: 5,
        },
    );
    for _pass in 0..10 {
        if indexer.process_pending().await?.indexed == 0 {
            break;
        }
    }
    Ok(())
}

/// The owner's search results that are notes, browsing every document.
async fn note_search_results(setup: &Setup) -> Result<usize, Box<dyn std::error::Error>> {
    let page = search_page(
        setup.database.database.pool(),
        &SearchQuery::new(OWNER, "", 100, 0)?,
    )
    .await?;
    let lexical = search_page(
        setup.database.database.pool(),
        &SearchQuery::new(OWNER, "garden", 100, 0)?,
    )
    .await?;
    Ok(page
        .results
        .iter()
        .chain(&lexical.results)
        .filter(|result| result.owner_context == "ratatoskr-notes")
        .count())
}

#[tokio::test]
async fn disconnect_removes_all_derived_data() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    let worker = setup.spawn(analyses(30));
    for index in 1..=30 {
        let text = format!("# Note {index}\n\n{EXCERPT}\n");
        let revision = setup.store_note(text.as_bytes())?;
        let file = file_id(index);
        let path = format!("Daily/Note {index}.md");
        let created = Change {
            file_id: &file,
            sequence: u64::from(index),
            kind: "created",
            path: &path,
            revision: &revision,
            media_type: "text/markdown",
            length: text.len(),
        };
        setup
            .publish(
                FILES_SUBJECT,
                &event_id(100 + index),
                created.envelope(&event_id(100 + index)),
            )
            .await?;
    }
    assert_eq!(setup.settle(FILES_DURABLE, 30).await?, (0, 0, 30));
    index_embeddings(&setup).await?;
    let embeddings = "select count(*) from knowledge.embedding_chunks
                      where owner_context = 'ratatoskr-notes'";
    assert_eq!(
        note_search_results(&setup).await?,
        60,
        "30 notes are browsed and found"
    );
    assert!(
        setup.count(embeddings).await? >= 30,
        "every note is embedded"
    );

    setup
        .publish(
            "evt.notes.vault.disconnected.v1",
            &event_id(200),
            envelope(
                &event_id(200),
                "notes.vault.disconnected.v1",
                &format!("notes_connection:{CONNECTION}"),
                &serde_json::json!({
                    "connection_id": CONNECTION,
                    "channel": "plugin",
                    "vault_display_name": "Personal",
                }),
            ),
        )
        .await?;
    assert_eq!(setup.settle(VAULTS_DURABLE, 1).await?, (0, 0, 1));
    assert_eq!(
        note_search_results(&setup).await?,
        0,
        "no search result of the removed connection remains"
    );
    assert_eq!(
        setup.count(embeddings).await?,
        0,
        "no embedding of the removed connection remains"
    );
    setup.finish(worker).await
}

#[tokio::test]
async fn older_event_after_delete_creates_nothing() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    let first = NOTE_TEXT;
    let second = format!("{NOTE_TEXT}\nA second line.\n");
    let first_revision = setup.store_note(first.as_bytes())?;
    let second_revision = setup.store_note(second.as_bytes())?;
    let worker = setup.spawn(analyses(2));
    let change = |sequence, kind, revision, length| Change {
        file_id: FILE,
        sequence,
        kind,
        path: "Projects/Garden.md",
        revision,
        media_type: "text/markdown",
        length,
    };
    let steps = [
        change(1, "created", &first_revision, first.len()),
        change(3, "deleted", &first_revision, first.len()),
        change(2, "updated", &second_revision, second.len()),
    ];
    for (delivered, step) in (1..).zip(&steps) {
        let event = event_id(300 + delivered);
        setup
            .publish(FILES_SUBJECT, &event, step.envelope(&event))
            .await?;
        assert_eq!(
            setup.settle(FILES_DURABLE, u64::from(delivered)).await?,
            (0, 0, u64::from(delivered))
        );
    }
    assert_eq!(
        setup.runs_for(FILE, &second_revision).await?,
        0,
        "no run for the older update"
    );
    assert_eq!(
        setup
            .count(
                "select count(*) from knowledge.search_documents
                 where owner_context = 'ratatoskr-notes'"
            )
            .await?,
        0,
        "no search entry for the deleted file"
    );
    setup.finish(worker).await
}

/// The configuration key of the notes blob root.
const NOTES_ROOT_KEY: &str = "RATATOSKR__BLOBS__NOTES_ROOT";

/// The value of the label-free redelivery counter on the operator plane.
fn redeliveries(metrics: &Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix("knowledge_ingest_redeliveries_total "))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or_default()
}

#[tokio::test]
async fn missing_blob_is_transient() -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let setup = Setup::start().await?;
    let root = setup.notes_root.path().display().to_string();
    let config = Config::from_environment([
        ("RATATOSKR__INGEST__ENABLED", "true"),
        ("RATATOSKR__INGEST__BUS_ENDPOINT", setup.url.as_str()),
        ("RATATOSKR__INGEST__PROVIDER_MODE", "fixture"),
        (
            "RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET",
            "readme-secret",
        ),
        ("RATATOSKR__LIMITS__PROVIDER_TIMEOUT_MS", "5000"),
        (NOTES_ROOT_KEY, root.as_str()),
    ]);
    assert!(
        config.is_ok(),
        "the notes blob root is configured through {NOTES_ROOT_KEY}: {:?}",
        config.as_ref().err()
    );
    let worker = setup.spawn_with(config?, analyses(1));
    // A revision that ratatoskr-notes has not written to its root.
    let revision = "2".repeat(64);
    let created = Change {
        file_id: FILE,
        sequence: 1,
        kind: "created",
        path: "Projects/Garden.md",
        revision: &revision,
        media_type: "text/markdown",
        length: NOTE_TEXT.len(),
    };
    setup
        .publish(
            FILES_SUBJECT,
            &event_id(400),
            created.envelope(&event_id(400)),
        )
        .await?;

    let _timed_out = eventually("the change is redelivered", || async {
        Ok(setup.durable(FILES_DURABLE).await?.2 >= 3)
    })
    .await;
    let delivered = setup.durable(FILES_DURABLE).await?.2;
    assert!(
        delivered >= 3,
        "the change is negatively acknowledged and redelivered: {delivered} deliveries"
    );
    assert!(
        redeliveries(&setup.metrics) >= 2,
        "each redelivery waits the transient delay of the schedule"
    );
    assert_eq!(
        setup.runs_for(FILE, &revision).await?,
        0,
        "no run without the bytes"
    );
    assert!(
        logs.lines_containing("notes_blob") >= 1,
        "the redelivery is logged with the class notes_blob: {}",
        logs.text()
    );
    assert_eq!(
        logs.lines_containing("ingest delivery rejected"),
        0,
        "a missing blob is never terminal"
    );
    setup.finish(worker).await
}

/// Keeps every disconnection transaction open for four seconds after it deleted the guard rows of
/// its files, so a file change can be analysed while the removal has not committed.
const SLOW_DISCONNECTION: &str = "
    create function knowledge.slow_disconnection() returns trigger language plpgsql as $$
    begin perform pg_sleep(4); return null; end $$;
    create trigger slow_disconnection after delete on knowledge.notes_files
        for each statement execute function knowledge.slow_disconnection();";

/// The search entries of notes, of every owner.
const NOTE_SEARCH_DOCUMENTS: &str =
    "select count(*) from knowledge.search_documents where owner_context = 'ratatoskr-notes'";

fn disconnected(event: &str) -> Vec<u8> {
    envelope(
        event,
        "notes.vault.disconnected.v1",
        &format!("notes_connection:{CONNECTION}"),
        &serde_json::json!({
            "connection_id": CONNECTION,
            "channel": "plugin",
            "vault_display_name": "Personal",
        }),
    )
}

#[tokio::test]
async fn disconnect_during_analysis_leaves_nothing() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    sqlx::raw_sql(SLOW_DISCONNECTION)
        .execute(setup.database.database.pool())
        .await?;
    let revision = setup.store_note(NOTE_TEXT.as_bytes())?;
    let worker = setup.spawn(analyses(1));
    setup
        .publish(
            "evt.notes.vault.disconnected.v1",
            &event_id(500),
            disconnected(&event_id(500)),
        )
        .await?;
    // The disconnection is delivered and sleeps inside its transaction.
    eventually("the disconnection is in flight", || async {
        Ok(setup.durable(VAULTS_DURABLE).await? == (1, 0, 1))
    })
    .await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let created = Change {
        file_id: FILE,
        sequence: 1,
        kind: "created",
        path: "Projects/Garden.md",
        revision: &revision,
        media_type: "text/markdown",
        length: NOTE_TEXT.len(),
    };
    setup
        .publish(
            FILES_SUBJECT,
            &event_id(501),
            created.envelope(&event_id(501)),
        )
        .await?;

    assert_eq!(setup.settle(FILES_DURABLE, 1).await?, (0, 0, 1));
    assert_eq!(setup.settle(VAULTS_DURABLE, 1).await?, (0, 0, 1));
    assert_eq!(
        setup.runs_for(FILE, &revision).await?,
        0,
        "no run of a file whose connection was removed during its analysis"
    );
    assert_eq!(
        setup.count(NOTE_SEARCH_DOCUMENTS).await?,
        0,
        "no search entry of the removed connection"
    );
    setup.finish(worker).await
}

#[tokio::test]
async fn redelivery_after_a_missed_removal_removes_the_file() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    let revision = setup.store_note(NOTE_TEXT.as_bytes())?;
    let worker = setup.spawn(analyses(1));
    let created = Change {
        file_id: FILE,
        sequence: 1,
        kind: "created",
        path: "Projects/Garden.md",
        revision: &revision,
        media_type: "text/markdown",
        length: NOTE_TEXT.len(),
    };
    let event = event_id(510);
    setup
        .publish(FILES_SUBJECT, &event, created.envelope(&event))
        .await?;
    assert_eq!(setup.settle(FILES_DURABLE, 1).await?, (0, 0, 1));
    assert_eq!(
        setup.runs_for(FILE, &revision).await?,
        1,
        "the note is analysed"
    );
    // What a removal that committed between the claim and the persisted analysis leaves, when the
    // worker then dies before its check: the connection is removed, the guard row is gone and the
    // analysis stays.
    sqlx::raw_sql(&format!(
        "insert into knowledge.notes_disconnections (tenant_ref, connection_id)
         values ('{OWNER}', '{CONNECTION}');
         delete from knowledge.notes_files;"
    ))
    .execute(setup.database.database.pool())
    .await?;
    setup
        .publish(
            FILES_SUBJECT,
            &format!("{event}-again"),
            created.envelope(&event),
        )
        .await?;

    assert_eq!(setup.settle(FILES_DURABLE, 2).await?, (0, 0, 2));
    assert_eq!(
        setup.runs_for(FILE, &revision).await?,
        0,
        "the redelivery removes the analysis of the removed connection"
    );
    assert_eq!(setup.count(NOTE_SEARCH_DOCUMENTS).await?, 0);
    setup.finish(worker).await
}

/// The first revision of the garden note: it also names alpacas.
const FIRST_TEXT: &str =
    "# Garden\n\nAlpaca beds come first.\n\nThe garden plan starts in spring.\n";

/// The owner's search results that are notes, for one query.
async fn note_hits(setup: &Setup, query: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let page = search_page(
        setup.database.database.pool(),
        &SearchQuery::new(OWNER, query, 100, 0)?,
    )
    .await?;
    Ok(page
        .results
        .iter()
        .filter(|result| result.owner_context == "ratatoskr-notes")
        .count())
}

/// The search titles of notes.
async fn note_titles(setup: &Setup) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "select title from knowledge.search_documents where owner_context = 'ratatoskr-notes'
         order by title",
    )
    .fetch_all(setup.database.database.pool())
    .await
}

/// A change of the garden note.
fn garden<'a>(sequence: u64, kind: &'a str, path: &'a str, revision: &'a str) -> Change<'a> {
    Change {
        file_id: FILE,
        sequence,
        kind,
        path,
        revision,
        media_type: "text/markdown",
        length: NOTE_TEXT.len(),
    }
}

#[tokio::test]
async fn updated_note_replaces_its_search_entry() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    let first = setup.store_note(FIRST_TEXT.as_bytes())?;
    let second = setup.store_note(NOTE_TEXT.as_bytes())?;
    let worker = setup.spawn(analyses(2));
    let created = Change {
        length: FIRST_TEXT.len(),
        ..garden(1, "created", "Projects/Garden.md", &first)
    };
    setup
        .publish(
            FILES_SUBJECT,
            &event_id(600),
            created.envelope(&event_id(600)),
        )
        .await?;
    assert_eq!(setup.settle(FILES_DURABLE, 1).await?, (0, 0, 1));
    assert_eq!(
        note_hits(&setup, "alpaca").await?,
        1,
        "the first revision is found"
    );

    let updated = garden(2, "updated", "Projects/Garden.md", &second);
    setup
        .publish(
            FILES_SUBJECT,
            &event_id(601),
            updated.envelope(&event_id(601)),
        )
        .await?;
    assert_eq!(setup.settle(FILES_DURABLE, 2).await?, (0, 0, 2));
    assert_eq!(
        setup.runs_for(FILE, &second).await?,
        1,
        "the update is analysed"
    );
    assert_eq!(
        note_hits(&setup, "alpaca").await?,
        0,
        "the text of the first revision is not found"
    );
    assert_eq!(
        setup.count(NOTE_SEARCH_DOCUMENTS).await?,
        1,
        "one search entry for the file"
    );
    setup.finish(worker).await
}

#[tokio::test]
async fn rename_updates_the_title() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    let revision = setup.store_note(NOTE_TEXT.as_bytes())?;
    let worker = setup.spawn(analyses(1));
    let created = garden(1, "created", "Projects/Garden.md", &revision);
    setup
        .publish(
            FILES_SUBJECT,
            &event_id(610),
            created.envelope(&event_id(610)),
        )
        .await?;
    assert_eq!(setup.settle(FILES_DURABLE, 1).await?, (0, 0, 1));
    let renamed = garden(2, "renamed", "Projects/Orchard.md", &revision);
    setup
        .publish(
            FILES_SUBJECT,
            &event_id(611),
            renamed.renamed_from(&event_id(611), "Projects/Garden.md"),
        )
        .await?;
    assert_eq!(setup.settle(FILES_DURABLE, 2).await?, (0, 0, 2));
    assert_eq!(
        note_titles(&setup).await?,
        ["Orchard"],
        "the search title follows the rename"
    );
    setup.finish(worker).await
}

#[tokio::test]
async fn rename_to_non_markdown_removes_the_entry() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    let revision = setup.store_note(NOTE_TEXT.as_bytes())?;
    let worker = setup.spawn(analyses(1));
    let created = garden(1, "created", "Projects/Garden.md", &revision);
    setup
        .publish(
            FILES_SUBJECT,
            &event_id(620),
            created.envelope(&event_id(620)),
        )
        .await?;
    assert_eq!(setup.settle(FILES_DURABLE, 1).await?, (0, 0, 1));
    let renamed = Change {
        media_type: "image/png",
        ..garden(2, "renamed", "Projects/Garden.png", &revision)
    };
    setup
        .publish(
            FILES_SUBJECT,
            &event_id(621),
            renamed.renamed_from(&event_id(621), "Projects/Garden.md"),
        )
        .await?;
    assert_eq!(setup.settle(FILES_DURABLE, 2).await?, (0, 0, 2));
    assert_eq!(
        setup.count(NOTE_SEARCH_DOCUMENTS).await?,
        0,
        "a file that is no longer Markdown has no search entry"
    );
    setup.finish(worker).await
}

/// The guard sequence of one file.
async fn guard_sequence(setup: &Setup, file: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "select connection_sequence from knowledge.notes_files where file_id = $1::uuid",
    )
    .bind(file)
    .fetch_one(setup.database.database.pool())
    .await
}

#[tokio::test]
async fn rejected_update_removes_the_old_search_entry() -> TestResult {
    let _serial = BROKER.lock().await;
    let setup = Setup::start().await?;
    let worker = setup.spawn(analyses(3));
    let embeddings = "select count(*) from knowledge.embedding_chunks
                      where owner_context = 'ratatoskr-notes'";
    let large = setup.store_note(b"# Large\n")?;
    let not_utf8: &[u8] = b"# Bytes\n\xff\xfe\n";
    let encoding = setup.store_note(not_utf8)?;
    let long_text = "a".repeat(NOTE_CONTEXT_CHARACTERS + 1);
    let long = setup.store_note(long_text.as_bytes())?;
    // One file per class: the word its first revision names, and the rejected update.
    let classes = [
        ("alpaca", &large, 16_777_217),
        ("badger", &encoding, not_utf8.len()),
        ("heron", &long, long_text.len()),
    ];
    for (index, (word, _, _)) in (1..).zip(&classes) {
        let text = format!("# Note {index}\n\n{word} beds come first.\n\n{EXCERPT}\n");
        let revision = setup.store_note(text.as_bytes())?;
        let file = file_id(700 + index);
        let path = format!("Daily/Note {index}.md");
        let created = Change {
            length: text.len(),
            file_id: &file,
            ..garden(1, "created", &path, &revision)
        };
        setup
            .publish(
                FILES_SUBJECT,
                &event_id(700 + index),
                created.envelope(&event_id(700 + index)),
            )
            .await?;
    }
    assert_eq!(setup.settle(FILES_DURABLE, 3).await?, (0, 0, 3));
    index_embeddings(&setup).await?;
    for (word, _, _) in &classes {
        assert_eq!(note_hits(&setup, word).await?, 1, "{word} is found");
    }
    assert!(
        setup.count(embeddings).await? >= 3,
        "every note is embedded"
    );

    for (index, (_, revision, length)) in (1..).zip(&classes) {
        let file = file_id(700 + index);
        let path = format!("Daily/Note {index}.md");
        let updated = Change {
            length: *length,
            file_id: &file,
            ..garden(2, "updated", &path, revision)
        };
        setup
            .publish(
                FILES_SUBJECT,
                &event_id(710 + index),
                updated.envelope(&event_id(710 + index)),
            )
            .await?;
    }
    assert_eq!(
        setup.settle(FILES_DURABLE, 6).await?,
        (0, 0, 6),
        "each rejected update is delivered once and acknowledged"
    );
    for (index, (word, _, _)) in (1..).zip(&classes) {
        assert_eq!(
            guard_sequence(&setup, &file_id(700 + index)).await?,
            2,
            "the guard of the {word} file holds the rejected update"
        );
        assert_eq!(
            note_hits(&setup, word).await?,
            0,
            "the text of the earlier {word} revision is not found"
        );
    }
    assert_eq!(
        setup.count(NOTE_SEARCH_DOCUMENTS).await?,
        0,
        "no search entry of a rejected file remains"
    );
    assert_eq!(
        setup.count(embeddings).await?,
        0,
        "no embedding of a rejected file remains"
    );
    setup.finish(worker).await
}
