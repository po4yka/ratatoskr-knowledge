//! General terminal-outbox recovery tests.

use std::time::Duration;

use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{TerminalOutbox, TerminalOutboxError};
use uuid::Uuid;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one fault trace keeps all family terminal commits and publish uncertainty together"
)]
async fn terminal_state_and_fact_survive_publish_uncertainty()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let outbox = TerminalOutbox::new(&database.database);

    let request_id = Uuid::now_v7();
    let (social, social_event) = seed_work(&database, "social", 'a', None).await?;
    let (archive, archive_event) = seed_work(&database, "ai_archive", 'b', None).await?;
    let (repository, repository_event) =
        seed_work(&database, "repository", 'c', Some(request_id)).await?;
    sqlx::query(
        "insert into knowledge.repository_analysis_requests (
             request_id, tenant_ref, repository_id, github_repository_numeric_id,
             source_revision, repository_attributes, requested_contract,
             idempotency_digest_hex
         ) values ($1, $2, $3, 42, '{}', '{}', 'repository_analysis', $4)",
    )
    .bind(request_id)
    .bind("user:018f0000-0000-7000-8000-000000000005")
    .bind(Uuid::now_v7())
    .bind("d".repeat(64))
    .execute(database.database.pool())
    .await?;

    let social_id = Uuid::now_v7();
    outbox
        .settle(
            social,
            "worker",
            true,
            "knowledge.analysis.completed.v1",
            "evt.knowledge.analysis.completed.v1",
            &terminal(
                social_id,
                social_event,
                "knowledge.analysis.completed.v1",
                serde_json::json!({}),
            ),
        )
        .await?;
    let archive_id = Uuid::now_v7();
    outbox
        .settle(
            archive,
            "worker",
            true,
            "knowledge.ai_archive_analysis.completed.v1",
            "evt.knowledge.ai_archive_analysis.completed.v1",
            &terminal(
                archive_id,
                archive_event,
                "knowledge.ai_archive_analysis.completed.v1",
                serde_json::json!({}),
            ),
        )
        .await?;
    let repository_id = Uuid::now_v7();
    let repository_outbox = outbox
        .settle(
            repository,
            "worker",
            true,
            "knowledge.repository_analysis.completed.v1",
            "evt.knowledge.repository_analysis.completed.v1",
            &terminal(
                repository_id,
                repository_event,
                "knowledge.repository_analysis.completed.v1",
                serde_json::json!({
                    "request_id": request_id,
                    "analysis_result_ref": "knowledge-result:018f0000-0000-7000-8000-000000000901"
                }),
            ),
        )
        .await?;

    let repository_state: String = sqlx::query_scalar(
        "select state from knowledge.repository_analysis_requests where request_id = $1",
    )
    .bind(request_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(repository_state, "completed");

    let mut identities = Vec::new();
    while let Some(entry) = outbox.next_pending().await? {
        identities.push(entry.message_id);
        if entry.outbox_id == repository_outbox {
            outbox.retry_after(entry.outbox_id, Duration::ZERO).await?;
            let retried = outbox
                .next_pending()
                .await?
                .ok_or("publish uncertainty lost the idle outbox row")?;
            assert_eq!(retried.message_id, repository_id);
            outbox.mark_published(retried.outbox_id).await?;
        } else {
            outbox.mark_published(entry.outbox_id).await?;
        }
    }
    identities.sort_unstable();
    let mut expected = vec![social_id, archive_id, repository_id];
    expected.sort_unstable();
    assert_eq!(identities, expected);

    let duplicate = outbox
        .settle(
            repository,
            "worker",
            true,
            "knowledge.repository_analysis.completed.v1",
            "evt.knowledge.repository_analysis.completed.v1",
            &terminal(
                repository_id,
                repository_event,
                "knowledge.repository_analysis.completed.v1",
                serde_json::json!({"request_id": request_id}),
            ),
        )
        .await;
    assert!(matches!(duplicate, Err(TerminalOutboxError::Transition)));

    let states: Vec<String> = sqlx::query_scalar(
        "select state from knowledge.analysis_work where work_id = any($1) order by state",
    )
    .bind(vec![social, archive, repository])
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(states, ["completed", "completed", "completed"]);
    database.cleanup().await?;
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the JSON fixture builder intentionally takes ownership of its synthetic payload"
)]
fn terminal(
    event_id: Uuid,
    causation_event: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": "2026-08-30T12:00:00Z",
        "producer": "ratatoskr-knowledge",
        "aggregate_id": "document:018f0000-0000-7000-8000-000000000021",
        "correlation_id": format!("event:{causation_event}"),
        "causation_id": format!("event:{causation_event}"),
        "tenant_id": "user:018f0000-0000-7000-8000-000000000005",
        "schema_version": 1,
        "payload": payload
    })
}

async fn seed_work(
    database: &TestDatabase,
    family: &str,
    digest: char,
    request_id: Option<Uuid>,
) -> Result<(Uuid, Uuid), Box<dyn std::error::Error>> {
    let event_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.primary_event_receipts (
             event_id, subject, envelope_digest_hex, producer, tenant_ref, aggregate_id, family
         ) values ($1, 'evt.content.document.extracted.v1', $2, 'ratatoskr-extractor',
             'user:018f0000-0000-7000-8000-000000000005', $3, $4)",
    )
    .bind(event_id)
    .bind(digest.to_string().repeat(64))
    .bind(format!("document:{event_id}"))
    .bind(family)
    .execute(database.database.pool())
    .await?;
    let work_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_work (
             work_id, event_id, family, tenant_ref, source_key, parent_source_key, source_revision,
             input_envelope,
             lease_owner, lease_expires_at
         ) values ($1, $2, $3,
             'user:018f0000-0000-7000-8000-000000000005',
             $4, $4, $5, $6, 'worker', now() + interval '1 minute')",
    )
    .bind(work_id)
    .bind(event_id)
    .bind(family)
    .bind(event_id.to_string())
    .bind(digest.to_string().repeat(64))
    .bind(serde_json::json!({
        "event_id": event_id,
        "event_type": "content.document.extracted.v1",
        "occurred_at": "2026-08-30T11:00:00Z",
        "producer": "ratatoskr-extractor",
        "aggregate_id": "document:018f0000-0000-7000-8000-000000000021",
        "correlation_id": format!("event:{event_id}"),
        "tenant_id": "user:018f0000-0000-7000-8000-000000000005",
        "schema_version": 1,
        "payload": request_id.map_or_else(
            || serde_json::json!({}),
            |request_id| serde_json::json!({"request_id": request_id})
        )
    }))
    .execute(database.database.pool())
    .await?;
    Ok((work_id, event_id))
}
