//! Durable primary-work recovery regression tests.

use std::time::Duration;

use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{AnalysisWorkState, TerminalOutbox, WorkQueue};
use uuid::Uuid;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one recovery trace proves the complete lease, uncertainty, retry, and outbox sequence"
)]
async fn admitted_work_reclaims_every_state_without_duplicate_effects()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let tables: Vec<String> = sqlx::query_scalar(
        "select table_name from information_schema.tables
         where table_schema = 'knowledge' and table_name in (
             'primary_event_receipts', 'analysis_work', 'knowledge_outbox'
         ) order by table_name",
    )
    .fetch_all(database.database.pool())
    .await?;

    assert_eq!(
        tables,
        [
            "analysis_work",
            "knowledge_outbox",
            "primary_event_receipts"
        ],
        "primary admission currently has no recoverable work/outbox model"
    );

    let event_id = Uuid::now_v7();
    let work_id = seed_work(&database, event_id).await?;
    let queue = WorkQueue::new(&database.database);

    let admitted = queue
        .claim("worker-a", Duration::from_secs(30))
        .await?
        .ok_or("admitted work was not claimable")?;
    assert_eq!(admitted.work_id, work_id);
    assert_eq!(admitted.state, AnalysisWorkState::Admitted);
    queue
        .transition(
            work_id,
            "worker-a",
            AnalysisWorkState::Admitted,
            AnalysisWorkState::Preparing,
        )
        .await?;

    let preparing = queue
        .claim("worker-b", Duration::from_secs(30))
        .await?
        .ok_or("released preparation was not reclaimable")?;
    assert_eq!(preparing.state, AnalysisWorkState::Preparing);
    queue
        .transition(
            work_id,
            "worker-b",
            AnalysisWorkState::Preparing,
            AnalysisWorkState::ProviderPending,
        )
        .await?;

    let provider = queue
        .claim("worker-c", Duration::from_secs(30))
        .await?
        .ok_or("provider-pending work was not claimable")?;
    assert_eq!(provider.state, AnalysisWorkState::ProviderPending);
    queue.mark_provider_unknown(work_id, "worker-c").await?;
    assert!(
        queue
            .claim("worker-d", Duration::from_secs(30))
            .await?
            .is_none(),
        "an uncertain billable call was retried blindly"
    );

    queue
        .requeue_provider_unknown(work_id, "scripted-idempotent-request")
        .await?;
    let replay: (String, Option<String>, bool) = sqlx::query_as(
        "select state, provider_replay_key, provider_replay_authorized
         from knowledge.analysis_runs
         where run_id = (select analysis_run_id from knowledge.analysis_work where work_id = $1)",
    )
    .bind(work_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(replay.0, "model_requested");
    assert_eq!(replay.1.as_deref(), Some("scripted-idempotent-request"));
    assert!(replay.2);
    let requeued = queue
        .claim("worker-e", Duration::from_secs(30))
        .await?
        .ok_or("explicit idempotent requeue was not claimable")?;
    assert_eq!(requeued.state, AnalysisWorkState::ProviderPending);
    queue
        .retry_after(work_id, "worker-e", Duration::ZERO)
        .await?;
    let retry = queue
        .claim("worker-f", Duration::from_secs(30))
        .await?
        .ok_or("bounded retry did not become eligible")?;
    assert_eq!(retry.state, AnalysisWorkState::RetryWait);
    assert_eq!(retry.attempt_count, 1);

    let message_id = Uuid::now_v7();
    let terminal = serde_json::json!({
        "event_id": message_id,
        "event_type": "knowledge.analysis.completed.v1",
        "occurred_at": "2026-08-30T12:00:00Z",
        "producer": "ratatoskr-knowledge",
        "aggregate_id": "document:018f0000-0000-7000-8000-000000000021",
        "correlation_id": format!("event:{event_id}"),
        "causation_id": format!("event:{event_id}"),
        "tenant_id": "user:018f0000-0000-7000-8000-000000000005",
        "schema_version": 1,
        "payload": {}
    });
    let outbox = TerminalOutbox::new(&database.database);
    let outbox_id = outbox
        .settle(
            work_id,
            "worker-f",
            true,
            "knowledge.analysis.completed.v1",
            "evt.knowledge.analysis.completed.v1",
            &terminal,
        )
        .await?;
    let pending = outbox
        .next_pending()
        .await?
        .ok_or("idle startup did not discover an unsent terminal fact")?;
    assert_eq!(pending.outbox_id, outbox_id);
    assert_eq!(pending.message_id, message_id);
    outbox.retry_after(outbox_id, Duration::ZERO).await?;
    assert_eq!(
        outbox
            .next_pending()
            .await?
            .ok_or("publish uncertainty lost the row")?
            .message_id,
        message_id
    );
    outbox.mark_published(outbox_id).await?;
    assert!(outbox.next_pending().await?.is_none());

    let duplicate_terminal = outbox
        .settle(
            work_id,
            "worker-f",
            true,
            "knowledge.analysis.completed.v1",
            "evt.knowledge.analysis.completed.v1",
            &terminal,
        )
        .await;
    assert!(matches!(
        duplicate_terminal,
        Err(ratatoskr_knowledge::TerminalOutboxError::Transition)
    ));

    let expired_event = Uuid::now_v7();
    let expired_work = seed_work(&database, expired_event).await?;
    sqlx::query(
        "update knowledge.analysis_work set lease_owner = 'dead-worker',
             lease_expires_at = now() - interval '1 second' where work_id = $1",
    )
    .bind(expired_work)
    .execute(database.database.pool())
    .await?;
    let reclaimed = queue
        .claim("replacement-worker", Duration::from_secs(30))
        .await?
        .ok_or("expired lease was not reclaimed")?;
    assert_eq!(reclaimed.work_id, expired_work);
    assert!(!matches!(
        queue
            .transition(
                expired_work,
                "dead-worker",
                AnalysisWorkState::Admitted,
                AnalysisWorkState::Preparing,
            )
            .await,
        Ok(())
    ));

    database.cleanup().await?;
    Ok(())
}

async fn seed_work(
    database: &TestDatabase,
    event_id: Uuid,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    sqlx::query(
        "insert into knowledge.primary_event_receipts (
             event_id, subject, envelope_digest_hex, producer, tenant_ref, aggregate_id, family
         ) values ($1, 'evt.social.source.captured.v1', $2, 'ratatoskr-x',
             'user:018f0000-0000-7000-8000-000000000005',
             'social_source:018f0000-0000-7000-8000-000000000021', 'social')",
    )
    .bind(event_id)
    .bind("a".repeat(64))
    .execute(database.database.pool())
    .await?;
    let work_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_work (
             work_id, event_id, family, tenant_ref, source_key, parent_source_key, source_revision,
             input_envelope
         ) values ($1, $2, 'social',
             'user:018f0000-0000-7000-8000-000000000005',
             '018f0000-0000-7000-8000-000000000021',
             '018f0000-0000-7000-8000-000000000021', $3, $4)",
    )
    .bind(work_id)
    .bind(event_id)
    .bind(event_id.simple().to_string().repeat(2))
    .bind(serde_json::json!({
        "event_id": event_id,
        "event_type": "social.source.captured.v1",
        "occurred_at": "2026-08-30T11:00:00Z",
        "producer": "ratatoskr-x",
        "aggregate_id": "document:018f0000-0000-7000-8000-000000000021",
        "correlation_id": format!("event:{event_id}"),
        "tenant_id": "user:018f0000-0000-7000-8000-000000000005",
        "schema_version": 1,
        "payload": {}
    }))
    .execute(database.database.pool())
    .await?;
    let source_ref: Uuid = sqlx::query_scalar(
        "insert into knowledge.source_refs (
             source_ref_id, tenant_ref, owner_context, source_document_id,
             content_digest_algorithm, content_digest_hex, source_blob
         ) values ($1, 'user:018f0000-0000-7000-8000-000000000005',
             'ratatoskr-knowledge', '018f0000-0000-7000-8000-000000000021',
             'sha256', $2, '{}'::jsonb)
         on conflict (tenant_ref, owner_context, ai_archive_id, source_document_id,
                      content_digest_algorithm, content_digest_hex)
         do update set source_blob = excluded.source_blob
         returning source_ref_id",
    )
    .bind(Uuid::now_v7())
    .bind("b".repeat(64))
    .fetch_one(database.database.pool())
    .await?;
    let run_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         ) values ($1, $2, 'social_analysis_v1', $3,
             'social_context_v1', 'family_default_v1', 'provider_outcome_unknown')",
    )
    .bind(run_id)
    .bind(source_ref)
    .bind(format!("social_prompt_{event_id}"))
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.analysis_attempts (
             run_id, ordinal, reason, provider, model_policy, model, outcome
         ) values ($1, 1, 'initial', 'scripted', 'family_default_v1',
             'scripted/model', 'transient_failure')",
    )
    .bind(run_id)
    .execute(database.database.pool())
    .await?;
    Ok(work_id)
}
