//! A conversation as large as a producer may publish, through a real broker that carries the
//! deployed 8 MiB `max_payload` and the pinned Knowledge identity (XR-021 CONTRACTS.md R5-03).
//!
//! The service builds its blob store with `BlobStore::for_config`, whose ceiling is
//! `limits.blob_bytes`; this test goes through the same function, so a regression of the old
//! `min(blob_bytes, raw_response_bytes)` ceiling (1 MiB by default) fails here too.

use std::sync::Arc;
use std::time::Duration;

use ratatoskr_knowledge::BlobStore;
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge_service::{IngestFamily, Lifecycle, spawn_ingest_workers_with_provider};
use tokio::sync::watch;

use crate::authorized::Authorized;
use crate::fixtures::{
    ARCHIVE_ANALYSIS, CONVERSATION_ADDED, EVENT_C, OWNER, envelope, large_conversation_payload,
    routed,
};
use crate::ingest_support::{TestResult, eventually};

/// Publishes one conversation fact with the `ChatGPT` identity and waits until it is searchable.
async fn publish_and_search(messages: usize, part_bytes: usize) -> TestResult {
    let fabric = Authorized::boot().await?;
    let database = TestDatabase::create().await?;
    let blobs_root = TemporaryBlobRoot::create().await?;
    let mut config = fabric.config(std::net::SocketAddr::from(([127, 0, 0, 1], 9)));
    config.channel_recap.enabled = false;
    config.storage.blob_root = blobs_root.path().to_path_buf();
    assert_eq!(
        config.limits.raw_response_bytes, 1_048_576,
        "the default provider response cap is smaller than the conversation"
    );
    let blobs = BlobStore::for_config(&config);
    let lifecycle = Lifecycle::starting_with_ingest();
    let (drain, receiver) = watch::channel(false);
    let worker = spawn_ingest_workers_with_provider(
        config,
        database.database.clone(),
        blobs,
        lifecycle,
        receiver,
        routed(&[("archive_prompt_v1", ARCHIVE_ANALYSIS)]),
        Arc::new(ratatoskr_knowledge_service::Metrics::new()),
    );

    let payload = large_conversation_payload(messages, part_bytes)?;
    let bytes = envelope(
        EVENT_C,
        CONVERSATION_ADDED,
        "ratatoskr-chatgpt",
        Some(OWNER),
        &payload,
    );
    assert!(
        bytes.len() >= messages * part_bytes,
        "the fact is {} bytes",
        bytes.len()
    );
    fabric
        .publish("evt.ai_archive.conversation.added.v1", bytes)
        .await?;

    eventually("the large conversation is searchable", || async {
        let documents: i64 = sqlx::query_scalar("select count(*) from knowledge.search_documents")
            .fetch_one(database.database.pool())
            .await?;
        Ok(documents == 1)
    })
    .await?;
    eventually("the archive durable acknowledged it", || async {
        Ok(fabric
            .durable(
                ratatoskr_knowledge_service::EVENTS_STREAM,
                IngestFamily::AiArchive.durable(),
            )
            .await?
            == (0, 0, 1))
    })
    .await?;
    let _ignored = drain.send(true);
    tokio::time::timeout(Duration::from_secs(10), worker).await??;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_two_mebibyte_conversation_fact_becomes_searchable() -> TestResult {
    publish_and_search(36, 60_000).await
}

#[tokio::test]
async fn a_conversation_fact_of_7_9_million_bytes_becomes_searchable() -> TestResult {
    publish_and_search(132, 59_800).await
}
