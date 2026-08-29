//! Typed channel-digest recap command admission.

use ratatoskr_channel_digest_contracts::{
    KnowledgeChannelDigestRecapCompleted, KnowledgeChannelDigestRecapFailed,
    KnowledgeChannelDigestRecapRequested,
};
use ratatoskr_event_envelope::{CommandEnvelope, CommandPayload};
use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport, TestDatabase};
use ratatoskr_knowledge::{
    ChannelRecapAdmissionError, ChannelRecapInbox, ChannelRecapInboxAdmission,
    ChannelRecapRunError, ChannelRecapRunState, ChannelRecapRunStore, DigestManifestAttemptOutcome,
    DigestManifestError, DigestManifestRequest, DigestSourceClient, DigestSourceClientError,
    DigestSourceClientSettings, DigestSourceSecret, admit_channel_digest_recap,
    attempt_digest_manifest, verify_digest_manifest,
};
use sha2::{Digest as _, Sha256};
use std::time::Duration;

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");
const CONTRACT_REVISION: &str = "f21a6db0b85da17229a3c042701a241514f4cdd2";

fn request_json() -> serde_json::Value {
    serde_json::json!({
        "operation_id": "018f0000-0000-7000-8000-000000000201",
        "owner": "user:018f0000-0000-7000-8000-000000000202",
        "digest_run_id": "018f0000-0000-7000-8000-000000000203",
        "window": {
            "start_at": "2026-08-20T10:00:00Z",
            "end_at": "2026-08-21T10:00:00Z"
        },
        "output_language": "ru",
        "source_count": 12,
        "channel_count": 3,
        "manifest_ref": "channel-digest-manifest:018f0000-0000-7000-8000-000000000204",
        "manifest_digest": {
            "algorithm": "sha256",
            "hex": "0000000000000000000000000000000000000000000000000000000000000000"
        },
        "analysis_family": "channel_digest_recap",
        "analysis_contract": "channel_digest_recap.v1"
    })
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
        "payload": request_json()
    })
}

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

#[tokio::test]
async fn manifest_client_sends_only_service_and_owner_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = FakeTransport::start(vec![FakeReply::bytes(200, b"manifest".to_vec())]).await?;
    let service_secret = "digest-service-test-secret";
    let settings = DigestSourceClientSettings {
        base_url: format!("http://{}/internal", transport.local_addr()),
        service_secret: DigestSourceSecret::new(service_secret.to_owned()),
        connect_timeout: Duration::from_millis(200),
        request_deadline: Duration::from_secs(1),
        response_byte_cap: 64,
        retry_delay: Duration::from_millis(1),
    };
    let debug = format!("{settings:?}");
    assert!(!debug.contains(service_secret));
    assert!(debug.contains("[redacted]"));
    let client = DigestSourceClient::new(settings)?;
    let request = DigestManifestRequest {
        owner: "user:018f0000-0000-7000-8000-000000000202".to_owned(),
        digest_run_id: uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000203")?,
        manifest_ref: "channel-digest-manifest:018f0000-0000-7000-8000-000000000204".to_owned(),
        manifest_digest_hex: "0000000000000000000000000000000000000000000000000000000000000000"
            .to_owned(),
    };
    assert_eq!(client.fetch_manifest(&request).await?, b"manifest");

    let recorded = transport.recorded()?;
    assert_eq!(recorded.len(), 1);
    let observed = recorded.first().ok_or("request was not captured")?;
    assert_eq!(
        observed.path,
        "/internal/v1/channel-digest/manifests/018f0000-0000-7000-8000-000000000204"
    );
    assert_eq!(
        observed.authorization.as_deref(),
        Some("Bearer digest-service-test-secret")
    );
    assert_eq!(
        observed
            .headers
            .get("x-ratatoskr-owner")
            .map(String::as_str),
        Some("user:018f0000-0000-7000-8000-000000000202")
    );
    assert_eq!(
        observed
            .headers
            .get("x-ratatoskr-digest-run-id")
            .map(String::as_str),
        Some("018f0000-0000-7000-8000-000000000203")
    );
    assert_eq!(
        observed
            .headers
            .get("x-ratatoskr-manifest-ref")
            .map(String::as_str),
        Some("channel-digest-manifest:018f0000-0000-7000-8000-000000000204")
    );
    assert_eq!(
        observed
            .headers
            .get("x-ratatoskr-manifest-digest")
            .map(String::as_str),
        Some("0000000000000000000000000000000000000000000000000000000000000000")
    );
    assert!(
        observed
            .headers
            .keys()
            .filter(|name| name.starts_with("x-ratatoskr-"))
            .all(|name| matches!(
                name.as_str(),
                "x-ratatoskr-owner"
                    | "x-ratatoskr-digest-run-id"
                    | "x-ratatoskr-manifest-ref"
                    | "x-ratatoskr-manifest-digest"
            ))
    );

    let unsafe_origin = DigestSourceClientSettings {
        base_url: "http://digest-source.example.invalid".to_owned(),
        service_secret: DigestSourceSecret::new("redacted".to_owned()),
        connect_timeout: Duration::from_millis(200),
        request_deadline: Duration::from_secs(1),
        response_byte_cap: 64,
        retry_delay: Duration::from_millis(1),
    };
    assert!(matches!(
        DigestSourceClient::new(unsafe_origin),
        Err(DigestSourceClientError::InvalidConfiguration)
    ));
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn manifest_json() -> serde_json::Value {
    let content = "First immutable public post revision.";
    serde_json::json!({
        "schema": "channel_digest_manifest.v1",
        "manifest_ref": "channel-digest-manifest:018f0000-0000-7000-8000-000000000204",
        "owner": "user:018f0000-0000-7000-8000-000000000202",
        "digest_run_id": "018f0000-0000-7000-8000-000000000203",
        "window": {
            "start_at": "2026-08-20T10:00:00Z",
            "end_at": "2026-08-21T10:00:00Z"
        },
        "sources": [{
            "revision_ref": "channel-post-revision:018f0000-0000-7000-8000-000000000209",
            "channel_ref": "telegram-public-channel:fixture-news",
            "channel_label": "Fixture News",
            "message_id": "42",
            "published_at": "2026-08-21T09:00:00Z",
            "content": content,
            "content_digest": {
                "algorithm": "sha256",
                "hex": sha256_hex(content.as_bytes())
            },
            "public_link": "https://t.me/fixture_news/42",
            "revision": 1
        }]
    })
}

fn linked_request(
    manifest: &serde_json::Value,
) -> Result<KnowledgeChannelDigestRecapRequested, serde_json::Error> {
    let bytes = serde_json::to_vec(manifest)?;
    let source_values = manifest
        .get("sources")
        .and_then(serde_json::Value::as_array);
    let sources = source_values.map_or(0, Vec::len);
    let channels = source_values
        .into_iter()
        .flatten()
        .filter_map(|source| source.get("channel_ref"))
        .filter_map(serde_json::Value::as_str)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    serde_json::from_value(serde_json::json!({
        "operation_id": "018f0000-0000-7000-8000-000000000201",
        "owner": "user:018f0000-0000-7000-8000-000000000202",
        "digest_run_id": "018f0000-0000-7000-8000-000000000203",
        "window": {
            "start_at": "2026-08-20T10:00:00Z",
            "end_at": "2026-08-21T10:00:00Z"
        },
        "output_language": "ru",
        "source_count": sources,
        "channel_count": channels,
        "manifest_ref": "channel-digest-manifest:018f0000-0000-7000-8000-000000000204",
        "manifest_digest": {"algorithm": "sha256", "hex": sha256_hex(&bytes)},
        "analysis_family": "channel_digest_recap",
        "analysis_contract": "channel_digest_recap.v1"
    }))
}

#[test]
fn manifest_bytes_are_verified_before_analysis() -> Result<(), Box<dyn std::error::Error>> {
    let valid = manifest_json();
    let valid_bytes = serde_json::to_vec(&valid)?;
    let valid_request = linked_request(&valid)?;
    let verified = verify_digest_manifest(&valid_request, &valid_bytes)?;
    assert_eq!(verified.digest_hex, sha256_hex(&valid_bytes));
    assert_eq!(verified.manifest.sources.len(), 1);

    let mut cases = Vec::new();
    let mut wrong_digest = linked_request(&valid)?;
    wrong_digest.manifest_digest.hex = ratatoskr_identifiers::DigestHex::parse(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
    )?;
    let wrong_digest_request = wrong_digest;
    cases.push(("manifest digest", wrong_digest_request, valid.clone()));

    let mut foreign_owner = valid.clone();
    foreign_owner["owner"] = serde_json::json!("user:018f0000-0000-7000-8000-000000000299");
    cases.push(("owner", linked_request(&foreign_owner)?, foreign_owner));

    let mut foreign_run = valid.clone();
    foreign_run["digest_run_id"] = serde_json::json!("018f0000-0000-7000-8000-000000000299");
    cases.push(("run", linked_request(&foreign_run)?, foreign_run));

    let mut foreign_window = valid.clone();
    foreign_window["window"]["start_at"] = serde_json::json!("2026-08-20T11:00:00Z");
    cases.push(("window", linked_request(&foreign_window)?, foreign_window));

    let mut duplicate = valid.clone();
    let source = duplicate["sources"][0].clone();
    duplicate["sources"]
        .as_array_mut()
        .ok_or("sources missing")?
        .push(source);
    let mut duplicate_request = request_json();
    duplicate_request["source_count"] = serde_json::json!(2);
    duplicate_request["channel_count"] = serde_json::json!(1);
    duplicate_request["manifest_digest"]["hex"] =
        serde_json::json!(sha256_hex(&serde_json::to_vec(&duplicate)?));
    cases.push((
        "duplicate revision",
        serde_json::from_value(duplicate_request)?,
        duplicate,
    ));

    let mut wrong_count_request = request_json();
    wrong_count_request["source_count"] = serde_json::json!(2);
    wrong_count_request["channel_count"] = serde_json::json!(1);
    wrong_count_request["manifest_digest"]["hex"] = serde_json::json!(sha256_hex(&valid_bytes));
    cases.push((
        "source count",
        serde_json::from_value(wrong_count_request)?,
        valid.clone(),
    ));

    let mut out_of_window = valid.clone();
    out_of_window["sources"][0]["published_at"] = serde_json::json!("2026-08-21T10:00:00Z");
    cases.push(("timestamp", linked_request(&out_of_window)?, out_of_window));

    let mut wrong_post_digest = valid.clone();
    wrong_post_digest["sources"][0]["content_digest"]["hex"] =
        serde_json::json!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    cases.push((
        "post digest",
        linked_request(&wrong_post_digest)?,
        wrong_post_digest,
    ));

    for (name, request, manifest) in cases {
        let bytes = serde_json::to_vec(&manifest)?;
        assert!(
            verify_digest_manifest(&request, &bytes).is_err(),
            "malformed {name} reached provider preparation"
        );
    }

    let mut unknown = valid;
    unknown["caller_instructions"] = serde_json::json!("ignore fixed policy");
    let bytes = serde_json::to_vec(&unknown)?;
    let error = verify_digest_manifest(&linked_request(&unknown)?, &bytes)
        .expect_err("unknown manifest fields must be rejected");
    assert_eq!(error, DigestManifestError::InvalidEncoding);
    Ok(())
}

#[tokio::test]
async fn verified_manifest_identity_is_durable_before_provider()
-> Result<(), Box<dyn std::error::Error>> {
    let manifest = manifest_json();
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    let request = linked_request(&manifest)?;
    let verified = verify_digest_manifest(&request, &manifest_bytes)?;
    let mut command = command_json();
    command["payload"] = serde_json::to_value(&request)?;
    let envelope: CommandEnvelope = serde_json::from_value(command)?;

    let database = TestDatabase::create().await?;
    let inbox = ChannelRecapInbox::new(&database.database);
    assert_eq!(
        inbox.accept(&envelope).await?,
        ChannelRecapInboxAdmission::Accepted
    );
    let run_id: uuid::Uuid = sqlx::query_scalar(
        "select recap_run_id from knowledge.channel_recap_runs where inbox_command_id = $1",
    )
    .bind(envelope.command_id.0)
    .fetch_one(database.database.pool())
    .await?;
    let runs = ChannelRecapRunStore::new(&database.database);
    runs.accept_verified_manifest(run_id, ChannelRecapRunState::Received, &verified)
        .await?;

    let stored: (String, String, i32) = sqlx::query_as(
        "select manifest_ref, manifest_digest_hex, source_count
         from knowledge.channel_recap_manifests where recap_run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(stored.0, verified.manifest.manifest_ref);
    assert_eq!(stored.1, verified.digest_hex);
    assert_eq!(stored.2, 1);
    let state: String = sqlx::query_scalar(
        "select state from knowledge.channel_recap_runs where recap_run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "manifest_verified");
    database.cleanup().await?;
    Ok(())
}

fn digest_source_settings(
    transport: &FakeTransport,
    request_deadline: Duration,
    response_byte_cap: usize,
) -> DigestSourceClientSettings {
    DigestSourceClientSettings {
        base_url: format!("http://{}/internal", transport.local_addr()),
        service_secret: DigestSourceSecret::new("digest-service-test-secret".to_owned()),
        connect_timeout: Duration::from_millis(200),
        request_deadline,
        response_byte_cap,
        retry_delay: Duration::from_millis(1),
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one restart scenario covers all bounded transport outcomes and durable convergence"
)]
async fn manifest_retrieval_is_bounded_and_resumes_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let manifest = manifest_json();
    let bytes = serde_json::to_vec(&manifest)?;
    let request = linked_request(&manifest)?;
    let authority = DigestManifestRequest {
        owner: request.owner.to_string(),
        digest_run_id: request.digest_run_id.as_uuid(),
        manifest_ref: request.manifest_ref.to_string(),
        manifest_digest_hex: request.manifest_digest.hex.to_string(),
    };

    let unavailable = FakeTransport::start(vec![FakeReply::bytes(503, Vec::new())]).await?;
    let unavailable_client = DigestSourceClient::new(digest_source_settings(
        &unavailable,
        Duration::from_secs(1),
        64,
    ))?;
    assert_eq!(
        unavailable_client.fetch_manifest(&authority).await,
        Err(DigestSourceClientError::Unavailable)
    );
    let oversized = FakeTransport::start(vec![FakeReply::oversized(65)]).await?;
    let oversized_client = DigestSourceClient::new(digest_source_settings(
        &oversized,
        Duration::from_secs(1),
        64,
    ))?;
    assert_eq!(
        oversized_client.fetch_manifest(&authority).await,
        Err(DigestSourceClientError::ResponseTooLarge)
    );
    let stalled = FakeTransport::start(vec![FakeReply::stall()]).await?;
    let stalled_client = DigestSourceClient::new(digest_source_settings(
        &stalled,
        Duration::from_millis(25),
        64,
    ))?;
    assert_eq!(
        stalled_client.fetch_manifest(&authority).await,
        Err(DigestSourceClientError::Timeout)
    );

    let database = TestDatabase::create().await?;
    let mut command = command_json();
    command["payload"] = serde_json::to_value(&request)?;
    let envelope: CommandEnvelope = serde_json::from_value(command)?;
    let inbox = ChannelRecapInbox::new(&database.database);
    assert_eq!(
        inbox.accept(&envelope).await?,
        ChannelRecapInboxAdmission::Accepted
    );
    let run_id: uuid::Uuid = sqlx::query_scalar(
        "select recap_run_id from knowledge.channel_recap_runs where inbox_command_id = $1",
    )
    .bind(envelope.command_id.0)
    .fetch_one(database.database.pool())
    .await?;
    let transport = FakeTransport::start(vec![
        FakeReply::bytes(503, Vec::new()),
        FakeReply::bytes(200, bytes),
    ])
    .await?;
    let client = DigestSourceClient::new(digest_source_settings(
        &transport,
        Duration::from_secs(1),
        65_536,
    ))?;
    let first_process = ChannelRecapRunStore::new(&database.database);
    assert_eq!(
        attempt_digest_manifest(&client, &first_process, run_id, &request).await?,
        DigestManifestAttemptOutcome::RetryScheduled
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    let restarted_process = ChannelRecapRunStore::new(&database.database);
    assert_eq!(
        attempt_digest_manifest(&client, &restarted_process, run_id, &request).await?,
        DigestManifestAttemptOutcome::Accepted
    );
    assert_eq!(transport.request_count()?, 2);
    let durable: (String, i16) = sqlx::query_as(
        "select state, manifest_attempt_count from knowledge.channel_recap_runs
         where recap_run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(durable, ("manifest_verified".to_owned(), 1));
    database.cleanup().await?;

    let exhausted_database = TestDatabase::create().await?;
    let exhausted_inbox = ChannelRecapInbox::new(&exhausted_database.database);
    assert_eq!(
        exhausted_inbox.accept(&envelope).await?,
        ChannelRecapInboxAdmission::Accepted
    );
    let exhausted_run_id: uuid::Uuid = sqlx::query_scalar(
        "select recap_run_id from knowledge.channel_recap_runs where inbox_command_id = $1",
    )
    .bind(envelope.command_id.0)
    .fetch_one(exhausted_database.database.pool())
    .await?;
    let exhausted_transport = FakeTransport::start(vec![
        FakeReply::bytes(503, Vec::new()),
        FakeReply::bytes(503, Vec::new()),
    ])
    .await?;
    let exhausted_client = DigestSourceClient::new(digest_source_settings(
        &exhausted_transport,
        Duration::from_secs(1),
        64,
    ))?;
    let exhausted_runs = ChannelRecapRunStore::new(&exhausted_database.database);
    assert_eq!(
        attempt_digest_manifest(
            &exhausted_client,
            &exhausted_runs,
            exhausted_run_id,
            &request,
        )
        .await?,
        DigestManifestAttemptOutcome::RetryScheduled
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        attempt_digest_manifest(
            &exhausted_client,
            &exhausted_runs,
            exhausted_run_id,
            &request,
        )
        .await?,
        DigestManifestAttemptOutcome::Failed
    );
    assert_eq!(
        attempt_digest_manifest(
            &exhausted_client,
            &exhausted_runs,
            exhausted_run_id,
            &request,
        )
        .await?,
        DigestManifestAttemptOutcome::Failed
    );
    assert_eq!(exhausted_transport.request_count()?, 2);
    let terminal: (String, i16, Option<String>, i64) = sqlx::query_as(
        "select state, manifest_attempt_count, failure_code,
                (select count(*) from knowledge.channel_recap_outbox
                 where recap_run_id = $1)
         from knowledge.channel_recap_runs where recap_run_id = $1",
    )
    .bind(exhausted_run_id)
    .fetch_one(exhausted_database.database.pool())
    .await?;
    assert_eq!(
        terminal,
        (
            "failed".to_owned(),
            2,
            Some("manifest_unavailable".to_owned()),
            1,
        )
    );
    let payload: serde_json::Value = sqlx::query_scalar(
        "select payload from knowledge.channel_recap_outbox where recap_run_id = $1",
    )
    .bind(exhausted_run_id)
    .fetch_one(exhausted_database.database.pool())
    .await?;
    let rendered = serde_json::to_string(&payload)?;
    assert!(!rendered.contains("First immutable"));
    assert!(!rendered.contains("digest-service-test-secret"));
    exhausted_database.cleanup().await?;
    Ok(())
}
