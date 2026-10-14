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
    ScriptedEmbeddingProvider, ScriptedEmbeddingSuccess, SearchQuery, search_page,
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
