//! Typed channel-digest recap command admission.

use ratatoskr_channel_digest_contracts::{
    KnowledgeChannelDigestRecapCompleted, KnowledgeChannelDigestRecapFailed,
    KnowledgeChannelDigestRecapRequested,
};
use ratatoskr_event_envelope::{CommandEnvelope, CommandPayload};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    ChannelRecapAdmissionError, ChannelRecapInbox, ChannelRecapInboxAdmission,
    ChannelRecapRunError, ChannelRecapRunState, ChannelRecapRunStore, admit_channel_digest_recap,
};

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");
const CONTRACT_REVISION: &str = "a9289e27bcf53f496b5917889580965d97ff2358";

mod channel_recap_support;

use channel_recap_support::{command_json, request_json};

#[test]
fn channel_digest_recap_command_is_typed_and_content_free() {
    let request: KnowledgeChannelDigestRecapRequested =
        serde_json::from_value(request_json()).expect("published fixture is valid");
    assert_eq!(
        KnowledgeChannelDigestRecapRequested::COMMAND_TYPE,
        "knowledge.channel_digest_recap.requested.v1"
    );
    request
        .validate_for_publish()
        .expect("published fixture is producer-valid");
    let envelope: CommandEnvelope =
        serde_json::from_value(command_json()).expect("valid recap envelope");
    assert_eq!(
        admit_channel_digest_recap(&envelope).expect("typed command is admitted"),
        request
    );

    for (field, replacement) in [
        ("owner", serde_json::json!("user:foreign")),
        ("digest_run_id", serde_json::json!("not-a-uuid")),
        ("manifest_ref", serde_json::json!("https://source.invalid")),
        ("source_count", serde_json::json!(0)),
        ("channel_count", serde_json::json!(13)),
    ] {
        let mut malformed = request_json();
        malformed
            .as_object_mut()
            .expect("fixture is an object")
            .insert(field.to_owned(), replacement);
        assert!(
            serde_json::from_value::<KnowledgeChannelDigestRecapRequested>(malformed).is_err(),
            "malformed {field} must be refused"
        );
    }

    let mut malformed = command_json();
    malformed
        .get_mut("payload")
        .and_then(serde_json::Value::as_object_mut)
        .expect("payload is an object")
        .insert("owner".to_owned(), serde_json::json!("user:foreign"));
    let malformed: CommandEnvelope =
        serde_json::from_value(malformed).expect("envelope remains structurally valid");
    let error = admit_channel_digest_recap(&malformed).expect_err("payload must be rejected");
    assert_eq!(error, ChannelRecapAdmissionError::InvalidPayload);
    assert_eq!(error.to_string(), "the recap command payload is invalid");

    let mut foreign_owner = command_json();
    foreign_owner
        .as_object_mut()
        .expect("envelope is an object")
        .insert(
            "tenant_id".to_owned(),
            serde_json::json!("user:018f0000-0000-7000-8000-000000000299"),
        );
    let foreign_owner: CommandEnvelope =
        serde_json::from_value(foreign_owner).expect("foreign owner envelope is valid");
    let error = admit_channel_digest_recap(&foreign_owner).expect_err("owner must match");
    assert_eq!(error, ChannelRecapAdmissionError::OwnerMismatch);
    assert_eq!(error.to_string(), "the recap command owner is invalid");

    assert!(
        WORKSPACE_MANIFEST.contains(&format!(
            "ratatoskr-channel-digest-contracts = {{ git = \
             \"https://github.com/po4yka/ratatoskr-contracts.git\", rev = \"{CONTRACT_REVISION}\" }}"
        )),
        "the production contract pin and recap handler are absent"
    );
}

#[tokio::test]
async fn channel_recap_inbox_is_idempotent_and_owner_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let inbox = ChannelRecapInbox::new(&database.database);
    let first: CommandEnvelope = serde_json::from_value(command_json())?;
    assert_eq!(
        inbox.accept(&first).await?,
        ChannelRecapInboxAdmission::Accepted
    );
    assert_eq!(
        inbox.accept(&first).await?,
        ChannelRecapInboxAdmission::Duplicate
    );
    let mut semantic_duplicate = command_json();
    semantic_duplicate
        .as_object_mut()
        .ok_or("command fixture is not an object")?
        .insert(
            "command_id".to_owned(),
            serde_json::json!("018f0000-0000-7000-8000-000000000206"),
        );
    let semantic_duplicate: CommandEnvelope = serde_json::from_value(semantic_duplicate)?;
    assert_eq!(
        inbox.accept(&semantic_duplicate).await?,
        ChannelRecapInboxAdmission::Duplicate
    );

    let work_items: i64 = sqlx::query_scalar("select count(*) from knowledge.channel_recap_inbox")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(work_items, 1);
    let owner: String = sqlx::query_scalar(
        "select owner_ref from knowledge.channel_recap_inbox where digest_run_id = $1",
    )
    .bind(uuid::Uuid::parse_str(
        "018f0000-0000-7000-8000-000000000203",
    )?)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(owner, "user:018f0000-0000-7000-8000-000000000202");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one lifecycle scenario asserts every legal and terminal state edge"
)]
async fn channel_recap_terminal_state_cannot_regress() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let envelope: CommandEnvelope = serde_json::from_value(command_json())?;
    let inbox = ChannelRecapInbox::new(&database.database);
    assert_eq!(
        inbox.accept(&envelope).await?,
        ChannelRecapInboxAdmission::Accepted
    );

    let (run_id, state): (uuid::Uuid, String) = sqlx::query_as(
        "select recap_run_id, state from knowledge.channel_recap_runs
         where inbox_command_id = $1",
    )
    .bind(envelope.command_id.0)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "received");
    assert!(!run_id.is_nil());

    let runs = ChannelRecapRunStore::new(&database.database);
    for (expected, next) in [
        (
            ChannelRecapRunState::Received,
            ChannelRecapRunState::ManifestRetry,
        ),
        (
            ChannelRecapRunState::ManifestRetry,
            ChannelRecapRunState::ManifestVerified,
        ),
        (
            ChannelRecapRunState::ManifestVerified,
            ChannelRecapRunState::ContextPrepared,
        ),
        (
            ChannelRecapRunState::ContextPrepared,
            ChannelRecapRunState::ModelRequested,
        ),
        (
            ChannelRecapRunState::ModelRequested,
            ChannelRecapRunState::ResponseReceived,
        ),
        (
            ChannelRecapRunState::ResponseReceived,
            ChannelRecapRunState::SchemaValidated,
        ),
        (
            ChannelRecapRunState::SchemaValidated,
            ChannelRecapRunState::Persisted,
        ),
    ] {
        runs.transition(run_id, expected, next).await?;
    }
    let result_id = uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000207")?;
    sqlx::query(
        "insert into knowledge.channel_recap_results
             (result_id, recap_run_id, result, result_digest_hex, coverage)
         values ($1, $2, '{}'::jsonb, $3, $4)",
    )
    .bind(result_id)
    .bind(run_id)
    .bind("1111111111111111111111111111111111111111111111111111111111111111")
    .bind(serde_json::json!({
        "selected_count": 12,
        "included_count": 12,
        "omitted_count": 0,
        "channel_count": 3
    }))
    .execute(database.database.pool())
    .await?;
    let completed: KnowledgeChannelDigestRecapCompleted =
        serde_json::from_value(serde_json::json!({
            "owner": "user:018f0000-0000-7000-8000-000000000202",
            "operation_id": "018f0000-0000-7000-8000-000000000201",
            "digest_run_id": "018f0000-0000-7000-8000-000000000203",
            "manifest_digest": {
                "algorithm": "sha256",
                "hex": "0000000000000000000000000000000000000000000000000000000000000000"
            },
            "analysis_ref": "analysis:018f0000-0000-7000-8000-000000000208",
            "digest_result_id": "018f0000-0000-7000-8000-000000000207",
            "result_ref": "channel-digest-result:018f0000-0000-7000-8000-000000000207",
            "result_digest": {
                "algorithm": "sha256",
                "hex": "1111111111111111111111111111111111111111111111111111111111111111"
            },
            "completed_at": "2026-08-21T10:01:00Z",
            "coverage": {
                "selected_count": 12,
                "included_count": 12,
                "omitted_count": 0,
                "channel_count": 3
            }
        }))?;
    runs.settle_completed(run_id, ChannelRecapRunState::Persisted, &completed)
        .await?;
    assert!(matches!(
        runs.transition(
            run_id,
            ChannelRecapRunState::SchemaValidated,
            ChannelRecapRunState::Persisted
        )
        .await,
        Err(ChannelRecapRunError::StateConflict)
    ));
    assert!(matches!(
        runs.settle_completed(run_id, ChannelRecapRunState::Persisted, &completed)
            .await,
        Err(ChannelRecapRunError::StateConflict)
    ));

    let restarted = ChannelRecapInbox::new(&database.database);
    assert_eq!(
        restarted.accept(&envelope).await?,
        ChannelRecapInboxAdmission::Duplicate
    );
    let run_count: i64 = sqlx::query_scalar("select count(*) from knowledge.channel_recap_runs")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(run_count, 1);
    let outbox_count: i64 =
        sqlx::query_scalar("select count(*) from knowledge.channel_recap_outbox")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(outbox_count, 1);

    let failed: KnowledgeChannelDigestRecapFailed = serde_json::from_value(serde_json::json!({
        "owner": "user:018f0000-0000-7000-8000-000000000202",
        "operation_id": "018f0000-0000-7000-8000-000000000201",
        "digest_run_id": "018f0000-0000-7000-8000-000000000203",
        "manifest_digest": {
            "algorithm": "sha256",
            "hex": "0000000000000000000000000000000000000000000000000000000000000000"
        },
        "failure_code": "provider_timeout",
        "failed_at": "2026-08-21T10:01:00Z"
    }))?;
    assert!(matches!(
        runs.settle_failed(run_id, ChannelRecapRunState::Persisted, &failed)
            .await,
        Err(ChannelRecapRunError::StateConflict)
    ));
    database.cleanup().await?;
    Ok(())
}
