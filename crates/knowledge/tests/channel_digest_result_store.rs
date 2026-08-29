//! Integrity-checked channel-digest result projection tests.

use ratatoskr_event_envelope::CommandEnvelope;
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    ChannelRecapInbox, ChannelRecapInboxAdmission, ChannelRecapResultReadError,
    ChannelRecapRunStore,
};
use sha2::{Digest as _, Sha256};

#[tokio::test]
async fn completed_recap_result_reads_are_scoped_and_integrity_checked()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let envelope: CommandEnvelope = serde_json::from_value(command_json())?;
    assert_eq!(
        ChannelRecapInbox::new(&database.database)
            .accept(&envelope)
            .await?,
        ChannelRecapInboxAdmission::Accepted
    );
    let run_id: uuid::Uuid = sqlx::query_scalar(
        "select recap_run_id from knowledge.channel_recap_runs where inbox_command_id = $1",
    )
    .bind(envelope.command_id.0)
    .fetch_one(database.database.pool())
    .await?;
    let analysis_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000209")?;
    let recap = recap_json();
    let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&recap)?));
    let coverage = recap.get("coverage").ok_or("recap coverage is missing")?;
    sqlx::query(
        "insert into knowledge.channel_recap_results
             (result_id, recap_run_id, result, result_digest_hex, coverage)
         values ($1, $2, $3, $4, $5)",
    )
    .bind(analysis_id)
    .bind(run_id)
    .bind(&recap)
    .bind(&digest)
    .bind(coverage)
    .execute(database.database.pool())
    .await?;
    let store = ChannelRecapRunStore::new(&database.database);

    assert!(matches!(
        store.read_completed_result(analysis_id).await,
        Err(ChannelRecapResultReadError::NotFound)
    ));
    sqlx::query(
        "update knowledge.channel_recap_runs set state = 'completed' where recap_run_id = $1",
    )
    .bind(run_id)
    .execute(database.database.pool())
    .await?;

    let projection = store.read_completed_result(analysis_id).await?;
    assert_eq!(projection.analysis_id, analysis_id);
    assert_eq!(projection.result_digest_hex, digest);
    assert_eq!(projection.recap.headline, "Grounded fixture recap");
    let exposed = serde_json::to_string(&projection)?;
    for forbidden in ["raw_response", "\"prompt\":", "complete grounding source"] {
        assert!(!exposed.contains(forbidden));
    }
    assert!(matches!(
        store.read_completed_result(uuid::Uuid::now_v7()).await,
        Err(ChannelRecapResultReadError::NotFound)
    ));

    sqlx::query(
        "update knowledge.channel_recap_results set result_digest_hex = $2 where result_id = $1",
    )
    .bind(analysis_id)
    .bind("1111111111111111111111111111111111111111111111111111111111111111")
    .execute(database.database.pool())
    .await?;
    assert!(matches!(
        store.read_completed_result(analysis_id).await,
        Err(ChannelRecapResultReadError::Integrity)
    ));

    let mut malformed = recap;
    malformed["headline"] = serde_json::json!("");
    let malformed_digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&malformed)?));
    sqlx::query(
        "update knowledge.channel_recap_results
         set result = $2, result_digest_hex = $3 where result_id = $1",
    )
    .bind(analysis_id)
    .bind(malformed)
    .bind(malformed_digest)
    .execute(database.database.pool())
    .await?;
    assert!(matches!(
        store.read_completed_result(analysis_id).await,
        Err(ChannelRecapResultReadError::Integrity)
    ));

    database.cleanup().await?;
    Ok(())
}

fn command_json() -> serde_json::Value {
    serde_json::json!({
        "command_id": "018f0000-0000-7000-8000-000000000205",
        "command_type": "knowledge.channel_digest_recap.requested.v1",
        "issued_at": "2026-08-21T10:00:01Z",
        "producer": "ratatoskr-channel-digests",
        "aggregate_id": "channel-digest-run:018f0000-0000-7000-8000-000000000203",
        "correlation_id": "operation:018f0000-0000-7000-8000-000000000201",
        "tenant_id": "user:018f0000-0000-7000-8000-000000000202",
        "schema_version": 1,
        "payload": {
            "operation_id": "018f0000-0000-7000-8000-000000000201",
            "owner": "user:018f0000-0000-7000-8000-000000000202",
            "digest_run_id": "018f0000-0000-7000-8000-000000000203",
            "window": {"start_at": "2026-08-20T10:00:00Z", "end_at": "2026-08-21T10:00:00Z"},
            "output_language": "ru",
            "source_count": 1,
            "channel_count": 1,
            "manifest_ref": "channel-digest-manifest:018f0000-0000-7000-8000-000000000204",
            "manifest_digest": {"algorithm": "sha256", "hex": "0000000000000000000000000000000000000000000000000000000000000000"},
            "analysis_family": "channel_digest_recap",
            "analysis_contract": "channel_digest_recap.v1"
        }
    })
}

fn recap_json() -> serde_json::Value {
    serde_json::json!({
        "contract_version": "channel_digest_recap.v1",
        "prompt_version": "channel_digest_recap_prompt.v1",
        "context_version": "channel_digest_recap_context.v1",
        "output_language": "ru",
        "manifest_digest": {"algorithm": "sha256", "hex": "0000000000000000000000000000000000000000000000000000000000000000"},
        "headline": "Grounded fixture recap",
        "overview": "The supplied fixture discusses one bounded event.",
        "topics": [{
            "label": "Fixture topic",
            "summary": "A summary grounded only in one opaque revision.",
            "citations": ["channel-post-revision:018f0000-0000-7000-8000-000000000210"]
        }],
        "notable_items": [],
        "coverage": {"selected_count": 1, "included_count": 1, "omitted_count": 0, "channel_count": 1},
        "warnings": []
    })
}
