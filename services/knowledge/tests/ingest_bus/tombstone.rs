//! Archive tombstones and the other archive facts against a real `JetStream` and database (XR-021
//! CONTRACTS.md R3-08).
//!
//! A deletion tombstone is never dead-lettered and is never left half finished, and a delivery
//! that can never succeed is terminated at its first delivery instead of its fifth. The transient
//! failures are forced from the database side (a trigger that refuses the inbox receipt) and from
//! the filesystem side (a blob directory the worker may not write).

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ratatoskr_knowledge::ScriptedProvider;
use ratatoskr_knowledge_service::{IngestFamily, TERMINAL_DELIVERY};

use crate::fixtures::{
    ARCHIVE_ANALYSIS, CONTRACT_CONVERSATION, CONTRACT_TOMBSTONE, CONVERSATION_ADDED, EVENT_A,
    EVENT_B, EVENT_C, OTHER_OWNER, OWNER, envelope, response,
};
use crate::harness::{BROKER, Harness};
use crate::ingest_support::{TestResult, eventually};
use crate::log_capture::capture;

const ADDED_SUBJECT: &str = "evt.ai_archive.conversation.added.v1";
const TOMBSTONE_SUBJECT: &str = "evt.ai_archive.subject.tombstoned.v1";
const TOMBSTONED: &str = "ai_archive.subject.tombstoned.v1";

/// Refuses every inbox receipt until [`allow_inbox_receipts`], so `accept` fails with a storage
/// error and rolls back whatever it began.
const REFUSE_INBOX_RECEIPTS: &str = "
    create function knowledge.refuse_inbox_receipt() returns trigger language plpgsql as $$
    begin raise exception 'receipts are refused for this test'; end $$;
    create trigger refuse_inbox_receipt before insert on knowledge.source_analysis_inbox
        for each row execute function knowledge.refuse_inbox_receipt();";

fn scripted() -> Arc<ScriptedProvider> {
    Arc::new(ScriptedProvider::new([Ok(response(ARCHIVE_ANALYSIS))]))
}

/// The contract fixture with its owner replaced.
fn as_owner(fixture: &str, owner: &str) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::from_str(&fixture.replace(OWNER, owner))
}

async fn publish_added(harness: &Harness, event_id: &str, owner: &str) -> TestResult {
    harness
        .publish(
            ADDED_SUBJECT,
            event_id,
            envelope(
                event_id,
                CONVERSATION_ADDED,
                "ratatoskr-chatgpt",
                Some(owner),
                &as_owner(CONTRACT_CONVERSATION, owner)?,
            ),
        )
        .await
}

async fn publish_tombstone(harness: &Harness, event_id: &str, owner: &str) -> TestResult {
    harness
        .publish(
            TOMBSTONE_SUBJECT,
            event_id,
            envelope(
                event_id,
                TOMBSTONED,
                "ratatoskr-chatgpt",
                Some(owner),
                &as_owner(CONTRACT_TOMBSTONE, owner)?,
            ),
        )
        .await
}

async fn refuse_inbox_receipts(harness: &Harness) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(REFUSE_INBOX_RECEIPTS)
        .execute(harness.database.database.pool())
        .await?;
    Ok(())
}

async fn allow_inbox_receipts(harness: &Harness) -> Result<(), sqlx::Error> {
    sqlx::raw_sql("drop trigger refuse_inbox_receipt on knowledge.source_analysis_inbox")
        .execute(harness.database.database.pool())
        .await?;
    Ok(())
}

/// The consumer sequence of the archive durable: every delivery, redeliveries included.
async fn archive_deliveries(harness: &Harness) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(harness.durable(IngestFamily::AiArchive).await?.2)
}

/// Waits until the archive durable holds nothing open and has nothing left to deliver.
async fn archive_is_quiet(harness: &Harness) -> TestResult {
    eventually("the archive durable is quiet", || async {
        let (ack_pending, pending, _) = harness.durable(IngestFamily::AiArchive).await?;
        Ok(ack_pending == 0 && pending == 0)
    })
    .await
}

async fn failure_rows(harness: &Harness) -> Result<i64, sqlx::Error> {
    harness
        .count("select count(*) from knowledge.ingest_failures")
        .await
}

/// The stored blob files under the worker's blob root.
fn blob_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for prefix in std::fs::read_dir(root.join("sha256"))
        .into_iter()
        .flatten()
        .flatten()
    {
        for file in std::fs::read_dir(prefix.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            files.push(file.path());
        }
    }
    files
}

fn blob_directories(root: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(root.join("sha256"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .collect()
}

fn set_mode(directories: &[PathBuf], mode: u32) -> std::io::Result<()> {
    for directory in directories {
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(mode))?;
    }
    Ok(())
}

/// A tombstone is a privacy deletion: the delivery budget that protects the worker from a poison
/// message must never end it. The database refuses the receipt for eight deliveries, three more
/// than the budget of any other fact, and the ninth applies the tombstone.
#[tokio::test]
async fn a_tombstone_survives_more_than_five_transient_failures() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    publish_added(&harness, EVENT_A, OWNER).await?;
    eventually("the conversation is searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    archive_is_quiet(&harness).await?;
    refuse_inbox_receipts(&harness).await?;

    publish_tombstone(&harness, EVENT_B, OWNER).await?;
    let first_tombstone_delivery = 2;
    let target = first_tombstone_delivery + 7;
    eventually("eight tombstone deliveries or a dead letter", || async {
        Ok(archive_deliveries(&harness).await? >= target || failure_rows(&harness).await? > 0)
    })
    .await?;
    assert_eq!(
        failure_rows(&harness).await?,
        0,
        "a tombstone is never dead-lettered after {TERMINAL_DELIVERY} deliveries"
    );
    assert_eq!(
        harness.search_documents().await?,
        1,
        "nothing was applied while the receipt was refused"
    );

    allow_inbox_receipts(&harness).await?;
    eventually("the ninth delivery applies the tombstone", || async {
        Ok(harness.search_documents().await? == 0)
    })
    .await?;
    archive_is_quiet(&harness).await?;
    assert_eq!(failure_rows(&harness).await?, 0);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// The first delivery commits the row deletion and the erasure queue and then cannot remove the
/// blob files; the redelivery is a duplicate and must still finish the erasure.
#[tokio::test]
async fn a_redelivered_tombstone_finishes_the_blob_erasure() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    publish_added(&harness, EVENT_A, OWNER).await?;
    eventually("the conversation is searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    archive_is_quiet(&harness).await?;
    let stored = blob_files(harness.blob_root());
    assert!(
        !stored.is_empty(),
        "the analysis stored blobs, so there is something to erase"
    );
    let directories = blob_directories(harness.blob_root());
    set_mode(&directories, 0o555)?;

    publish_tombstone(&harness, EVENT_B, OWNER).await?;
    let outcome = async {
        eventually("the rows are deleted and the erasure is queued", || async {
            Ok(harness.search_documents().await? == 0
                && harness
                    .count("select count(*) from knowledge.blob_erasure_queue")
                    .await?
                    > 0)
        })
        .await?;
        eventually("the tombstone was delivered twice", || async {
            Ok(archive_deliveries(&harness).await? >= 3)
        })
        .await?;
        assert!(
            blob_files(harness.blob_root()).len() >= stored.len(),
            "the blobs cannot be removed yet, so the delivery is still open"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    set_mode(&directories, 0o755)?;
    outcome?;

    eventually(
        "a redelivery drains the queue and erases the files",
        || async {
            Ok(blob_files(harness.blob_root()).is_empty()
                && harness
                    .count("select count(*) from knowledge.blob_erasure_queue")
                    .await?
                    == 0)
        },
    )
    .await?;
    archive_is_quiet(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A tombstone for a subject another tenant owns can never succeed: it is terminated at its
/// first delivery, leaves no failure row and does not touch the other tenant's rows.
#[tokio::test]
async fn a_tombstone_for_another_tenants_subject_is_rejected_at_once() -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    publish_added(&harness, EVENT_A, OTHER_OWNER).await?;
    eventually("the other tenant's conversation is searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    archive_is_quiet(&harness).await?;
    let rows_before = harness
        .count("select count(*) from knowledge.source_refs")
        .await?;

    publish_tombstone(&harness, EVENT_B, OWNER).await?;
    eventually("the tombstone was delivered", || async {
        Ok(archive_deliveries(&harness).await? == 2)
    })
    .await?;
    archive_is_quiet(&harness).await?;
    // A redelivery would arrive after the 2 s nak delay; none may.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(archive_deliveries(&harness).await?, 2, "no redelivery");
    assert_eq!(failure_rows(&harness).await?, 0);
    assert_eq!(harness.search_documents().await?, 1);
    assert_eq!(
        harness
            .count("select count(*) from knowledge.source_refs")
            .await?,
        rows_before
    );
    assert_eq!(
        harness
            .count("select count(*) from knowledge.ai_archive_tombstones")
            .await?,
        0
    );
    assert_eq!(logs.lines_containing("foreign_subject"), 1);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

async fn rejected_at_once(
    harness: &Harness,
    logs: &crate::log_capture::Captured,
    class: &str,
) -> TestResult {
    eventually("the delivery was made", || async {
        Ok(archive_deliveries(harness).await? == 1)
    })
    .await?;
    archive_is_quiet(harness).await?;
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(archive_deliveries(harness).await?, 1, "no redelivery");
    assert_eq!(failure_rows(harness).await?, 0);
    assert_eq!(
        logs.lines_containing(class),
        1,
        "the class {class} is logged once"
    );
    Ok(())
}

#[tokio::test]
async fn an_undecodable_archive_fact_is_rejected_at_once() -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    harness
        .publish(TOMBSTONE_SUBJECT, EVENT_A, b"not an envelope".to_vec())
        .await?;
    rejected_at_once(&harness, &logs, "undecodable").await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn an_unsupported_archive_type_is_rejected_at_once() -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    harness
        .publish(
            "evt.ai_archive.workspace.added.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                "ai_archive.workspace.added.v1",
                "ratatoskr-chatgpt",
                Some(OWNER),
                &serde_json::json!({ "owner": OWNER }),
            ),
        )
        .await?;
    rejected_at_once(&harness, &logs, "unsupported").await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// The budget still protects the worker from every other archive fact: a conversation that fails
/// transiently gives up on the fifth delivery and leaves a failure row.
#[tokio::test]
async fn a_non_tombstone_archive_fact_keeps_its_delivery_budget() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let worker = harness.spawn(scripted());
    refuse_inbox_receipts(&harness).await?;
    publish_added(&harness, EVENT_C, OWNER).await?;
    eventually("the delivery gave up", || async {
        Ok(failure_rows(&harness).await? == 1)
    })
    .await?;
    archive_is_quiet(&harness).await?;
    assert_eq!(
        archive_deliveries(&harness).await?,
        u64::try_from(TERMINAL_DELIVERY)?,
        "the fifth delivery is the last"
    );
    assert_eq!(
        harness
            .count(
                "select count(*) from knowledge.ingest_failures
                 where family = 'ai_archive' and delivered_count = 5"
            )
            .await?,
        1
    );
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}
