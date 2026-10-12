//! The order of deliveries of one source: `(observed_at, event id)` (XR-021 CONTRACTS.md R5-08
//! rule 2).
//!
//! A survivor re-announcement after a privacy deletion carries the original `imported_at` of the
//! retained archive, the same instant as the fact Knowledge already holds, so only the event id
//! (a UUIDv7, time ordered) can say which of the two is newer.

use std::time::Duration;

use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, FamilyPipeline, ProviderResponse, ProviderUsage, ScriptedProvider, SourceInbox,
    SourceInboxAdmission,
};
use ratatoskr_social_contracts::SocialSourceSnapshot;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const SOCIAL: &str = r#"{
  "social_source_id":"018f0000-0000-7000-8000-000000000201",
  "platform":"x", "external_post_id":"1234567890123456789",
  "owner":"user:018f0000-0000-7000-8000-000000000005",
  "captured_at":"2026-08-17T10:00:00Z", "text":"A useful post.",
  "content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
  "acquisition":"official_api", "saved_authority":"authoritative_platform_state",
  "completeness":"complete", "upstream_availability":"available"
}"#;

const EARLIER: &str = "018f0000-0000-7000-8000-000000000a01";
const LATER: &str = "018f0000-0000-7000-8000-000000000a02";
const CAPTURED: &str = "social.source.captured.v1";

fn snapshot(
    captured_at: &str,
    digest_digit: char,
) -> Result<SocialSourceSnapshot, Box<dyn std::error::Error>> {
    snapshot_with_text(captured_at, digest_digit, "A useful post.")
}

fn snapshot_with_text(
    captured_at: &str,
    digest_digit: char,
    text: &str,
) -> Result<SocialSourceSnapshot, Box<dyn std::error::Error>> {
    let mut value: serde_json::Value = serde_json::from_str(SOCIAL)?;
    value["text"] = serde_json::json!(text);
    value["captured_at"] = serde_json::json!(captured_at);
    value["content_digest"]["hex"] = serde_json::json!(digest_digit.to_string().repeat(64));
    Ok(serde_json::from_value(value)?)
}

fn analysis() -> ProviderResponse {
    ProviderResponse {
        bytes: br#"{"summary":"Useful post.","topics":["rust"],"evidence_excerpt":"useful post","confidence":"grounded"}"#
            .to_vec(),
        request_id: Some("head-order".to_owned()),
        usage: ProviderUsage {
            input_tokens: 5,
            output_tokens: 5,
        },
    }
}

fn id(text: &str) -> Result<uuid::Uuid, uuid::Error> {
    uuid::Uuid::parse_str(text)
}

async fn head(database: &TestDatabase) -> Result<(String, uuid::Uuid), sqlx::Error> {
    sqlx::query_as("select content_digest_hex, inbox_event_id from knowledge.source_analysis_heads")
        .fetch_one(database.database.pool())
        .await
}

#[tokio::test]
async fn an_equal_time_delivery_with_a_later_event_id_replaces_the_head_and_is_analysed()
-> TestResult {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let inbox = SourceInbox::new(&database.database);
    let provider = ScriptedProvider::new([Ok(analysis()), Ok(analysis())]);
    let pipeline = FamilyPipeline::new(
        &database.database,
        &provider,
        &blobs,
        Duration::from_secs(1),
    );

    let first = snapshot("2026-08-17T10:00:00Z", 'a')?;
    assert_eq!(
        inbox.accept_social(id(EARLIER)?, CAPTURED, &first).await?,
        SourceInboxAdmission::AcceptedCurrent
    );
    pipeline.execute_social_event(&inbox, id(EARLIER)?).await?;

    let second = snapshot("2026-08-17T10:00:00Z", 'b')?;
    assert_eq!(
        inbox.accept_social(id(LATER)?, CAPTURED, &second).await?,
        SourceInboxAdmission::AcceptedCurrent,
        "an equal instant with a later event id is the newer delivery"
    );
    assert!(inbox.is_current_head(id(LATER)?).await?);
    assert!(!inbox.is_current_head(id(EARLIER)?).await?);
    assert_eq!(head(&database).await?, ("b".repeat(64), id(LATER)?));
    pipeline.execute_social_event(&inbox, id(LATER)?).await?;

    assert_eq!(
        provider.call_count()?,
        2,
        "the changed digest is analysed again"
    );
    let revisions: i64 = sqlx::query_scalar("select count(*) from knowledge.source_refs")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(revisions, 2);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn an_equal_time_delivery_with_an_earlier_event_id_delivered_second_stays_historical()
-> TestResult {
    let database = TestDatabase::create().await?;
    let inbox = SourceInbox::new(&database.database);

    assert_eq!(
        inbox
            .accept_social(
                id(LATER)?,
                CAPTURED,
                &snapshot("2026-08-17T10:00:00Z", 'b')?
            )
            .await?,
        SourceInboxAdmission::AcceptedCurrent
    );
    assert_eq!(
        inbox
            .accept_social(
                id(EARLIER)?,
                CAPTURED,
                &snapshot("2026-08-17T10:00:00Z", 'a')?
            )
            .await?,
        SourceInboxAdmission::AcceptedHistorical
    );

    assert!(inbox.is_current_head(id(LATER)?).await?);
    assert_eq!(head(&database).await?, ("b".repeat(64), id(LATER)?));
    database.cleanup().await?;
    Ok(())
}

/// An unchanged digest moves the head (the delivery is the newer one) but not the analysis: the
/// analysis revision is keyed by the digest, so the provider is not called again.
#[tokio::test]
async fn an_equal_time_delivery_with_an_equal_digest_keeps_the_analysis() -> TestResult {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let inbox = SourceInbox::new(&database.database);
    let provider = ScriptedProvider::new([Ok(analysis())]);
    let pipeline = FamilyPipeline::new(
        &database.database,
        &provider,
        &blobs,
        Duration::from_secs(1),
    );

    let same = snapshot("2026-08-17T10:00:00Z", 'a')?;
    inbox.accept_social(id(EARLIER)?, CAPTURED, &same).await?;
    pipeline.execute_social_event(&inbox, id(EARLIER)?).await?;
    assert_eq!(
        inbox.accept_social(id(LATER)?, CAPTURED, &same).await?,
        SourceInboxAdmission::AcceptedCurrent
    );
    pipeline.execute_social_event(&inbox, id(LATER)?).await?;

    assert_eq!(
        provider.call_count()?,
        1,
        "an unchanged digest is not analysed again"
    );
    let runs: i64 = sqlx::query_scalar("select count(*) from knowledge.analysis_runs")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(runs, 1);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_strictly_older_delivery_is_historical_whatever_its_event_id() -> TestResult {
    let database = TestDatabase::create().await?;
    let inbox = SourceInbox::new(&database.database);

    assert_eq!(
        inbox
            .accept_social(
                id(EARLIER)?,
                CAPTURED,
                &snapshot("2026-08-17T10:00:00Z", 'a')?
            )
            .await?,
        SourceInboxAdmission::AcceptedCurrent
    );
    assert_eq!(
        inbox
            .accept_social(
                id(LATER)?,
                CAPTURED,
                &snapshot("2026-08-17T09:00:00Z", 'b')?
            )
            .await?,
        SourceInboxAdmission::AcceptedHistorical,
        "a later event id does not make an older instant newer"
    );
    assert_eq!(head(&database).await?, ("a".repeat(64), id(EARLIER)?));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_repeated_event_is_a_duplicate_and_leaves_the_head() -> TestResult {
    let database = TestDatabase::create().await?;
    let inbox = SourceInbox::new(&database.database);
    let delivery = snapshot("2026-08-17T10:00:00Z", 'a')?;

    inbox
        .accept_social(id(EARLIER)?, CAPTURED, &delivery)
        .await?;
    inbox
        .accept_social(
            id(LATER)?,
            CAPTURED,
            &snapshot("2026-08-17T10:00:00Z", 'b')?,
        )
        .await?;
    assert_eq!(
        inbox
            .accept_social(id(EARLIER)?, CAPTURED, &delivery)
            .await?,
        SourceInboxAdmission::Duplicate
    );
    assert_eq!(
        inbox
            .accept_social(
                id(LATER)?,
                CAPTURED,
                &snapshot("2026-08-17T10:00:00Z", 'b')?
            )
            .await?,
        SourceInboxAdmission::Duplicate
    );
    assert_eq!(head(&database).await?, ("b".repeat(64), id(LATER)?));
    database.cleanup().await?;
    Ok(())
}

async fn searchable(
    database: &TestDatabase,
    term: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let page = ratatoskr_knowledge::search_page(
        database.database.pool(),
        &ratatoskr_knowledge::SearchQuery::new(
            "user:018f0000-0000-7000-8000-000000000005".to_owned(),
            term,
            10,
            0,
        )?,
    )
    .await?;
    Ok(page.results.len())
}

/// The text of the revision a newer head supersedes stops being served, and a rebuild of the
/// search documents cannot bring it back: a source is searchable as its head (XR-021 CONTRACTS.md
/// R5-08 rule 2, the survivor sequence of a privacy deletion).
#[tokio::test]
async fn the_head_revision_is_the_only_one_that_search_serves() -> TestResult {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let inbox = SourceInbox::new(&database.database);
    let provider = ScriptedProvider::new([Ok(analysis()), Ok(analysis())]);
    let pipeline = FamilyPipeline::new(
        &database.database,
        &provider,
        &blobs,
        Duration::from_secs(1),
    );

    inbox
        .accept_social(
            id(EARLIER)?,
            CAPTURED,
            &snapshot_with_text("2026-08-17T10:00:00Z", 'a', "A useful post about zanzibar.")?,
        )
        .await?;
    pipeline.execute_social_event(&inbox, id(EARLIER)?).await?;
    assert_eq!(searchable(&database, "zanzibar").await?, 1);

    inbox
        .accept_social(
            id(LATER)?,
            CAPTURED,
            &snapshot_with_text("2026-08-17T10:00:00Z", 'b', "A useful post about quokkas.")?,
        )
        .await?;
    pipeline.execute_social_event(&inbox, id(LATER)?).await?;

    assert_eq!(
        searchable(&database, "zanzibar").await?,
        0,
        "the superseded text"
    );
    assert_eq!(searchable(&database, "quokkas").await?, 1, "the head text");
    let rows: (i64, i64, i64) = sqlx::query_as(
        "select (select count(*) from knowledge.search_documents),
                (select count(*) from knowledge.search_projection_inputs),
                (select count(*) from knowledge.source_refs)",
    )
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(
        rows,
        (1, 1, 2),
        "one document and one input for the head, both revisions kept as history"
    );
    database.cleanup().await?;
    Ok(())
}

/// An analysis that was in flight when the head moved finishes afterwards: its revision does not
/// become searchable.
#[tokio::test]
async fn a_revision_persisted_after_the_head_moved_is_not_searchable() -> TestResult {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let inbox = SourceInbox::new(&database.database);
    let provider = ScriptedProvider::new([Ok(analysis()), Ok(analysis())]);
    let pipeline = FamilyPipeline::new(
        &database.database,
        &provider,
        &blobs,
        Duration::from_secs(1),
    );

    inbox
        .accept_social(
            id(EARLIER)?,
            CAPTURED,
            &snapshot_with_text("2026-08-17T10:00:00Z", 'a', "A useful post about zanzibar.")?,
        )
        .await?;
    inbox
        .accept_social(
            id(LATER)?,
            CAPTURED,
            &snapshot_with_text("2026-08-17T10:00:00Z", 'b', "A useful post about quokkas.")?,
        )
        .await?;
    pipeline.execute_social_event(&inbox, id(LATER)?).await?;
    pipeline.execute_social_event(&inbox, id(EARLIER)?).await?;

    assert_eq!(searchable(&database, "zanzibar").await?, 0);
    assert_eq!(searchable(&database, "quokkas").await?, 1);
    database.cleanup().await?;
    Ok(())
}
