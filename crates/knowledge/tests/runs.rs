//! Durable analysis identity and state tests.

use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    AnalysisIdentity, AttemptInput, AttemptOutcome, AttemptReason, RunState, SourceReference,
};

#[tokio::test]
async fn changed_source_digest_creates_an_immutable_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let tenant = TenantRef::of_user(UserId::new_v7());
    let document_id = DocumentId::new_v7();

    let first = database
        .database
        .register_source(&source(tenant, document_id, 'a')?)
        .await?;
    let second = database
        .database
        .register_source(&source(tenant, document_id, 'b')?)
        .await?;

    assert_ne!(first.id, second.id);
    let count: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.source_refs where source_document_id = $1",
    )
    .bind(document_id.to_string())
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(count, 2);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn complete_analysis_identity_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let source = database
        .database
        .register_source(&source(
            TenantRef::of_user(UserId::new_v7()),
            DocumentId::new_v7(),
            'c',
        )?)
        .await?;
    let identity = AnalysisIdentity {
        source_revision_id: source.id,
        contract_version: "article_v1".to_owned(),
        prompt_version: "article_prompt_v1".to_owned(),
        context_builder_version: "document_context_v1".to_owned(),
        model_policy: "fake_default_v1".to_owned(),
    };

    let first = database.database.create_run(&identity).await?;
    let second = database.database.create_run(&identity).await?;
    assert_eq!(first.id, second.id);

    let count: i64 = sqlx::query_scalar("select count(*) from knowledge.analysis_runs")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(count, 1);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn terminal_state_cannot_regress() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let source = database
        .database
        .register_source(&source(
            TenantRef::of_user(UserId::new_v7()),
            DocumentId::new_v7(),
            'd',
        )?)
        .await?;
    let legal = [
        (RunState::Queued, RunState::ContextPrepared),
        (RunState::ContextPrepared, RunState::ModelRequested),
        (RunState::ModelRequested, RunState::ResponseReceived),
        (RunState::ResponseReceived, RunState::SchemaValidated),
        (RunState::ResponseReceived, RunState::Repaired),
        (RunState::Repaired, RunState::ModelRequested),
        (RunState::SchemaValidated, RunState::Persisted),
        (RunState::Persisted, RunState::Completed),
        (RunState::Queued, RunState::Failed),
        (RunState::ContextPrepared, RunState::Failed),
        (RunState::ModelRequested, RunState::Failed),
        (RunState::ResponseReceived, RunState::Failed),
        (RunState::Repaired, RunState::Failed),
        (RunState::SchemaValidated, RunState::Failed),
    ];
    let illegal = [
        (RunState::Queued, RunState::Completed),
        (RunState::ContextPrepared, RunState::Persisted),
        (RunState::Completed, RunState::Queued),
        (RunState::Failed, RunState::ModelRequested),
    ];

    for (ordinal, &(from, to)) in legal.iter().chain(&illegal).enumerate() {
        let identity = identity(source.id, format!("policy_{ordinal}"));
        let run = database.database.create_run(&identity).await?;
        sqlx::query("update knowledge.analysis_runs set state = $2 where run_id = $1")
            .bind(run.id)
            .bind(from.as_str())
            .execute(database.database.pool())
            .await?;
        let changed = database.database.transition_run(run.id, from, to).await?;
        assert_eq!(changed, ordinal < legal.len(), "{from:?} -> {to:?}");
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn attempt_ordinals_and_reasons_are_durable() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let source = database
        .database
        .register_source(&source(
            TenantRef::of_user(UserId::new_v7()),
            DocumentId::new_v7(),
            'e',
        )?)
        .await?;
    let run = database
        .database
        .create_run(&identity(source.id, "attempt_policy".to_owned()))
        .await?;

    let first = database
        .database
        .record_attempt(run.id, &attempt(AttemptReason::Initial, "request-1"))
        .await?;
    let repair = database
        .database
        .record_attempt(run.id, &attempt(AttemptReason::Repair, "request-2"))
        .await?;
    assert_eq!((first.ordinal, repair.ordinal), (1, 2));

    let rows: Vec<(i16, String, String, String)> = sqlx::query_as(
        "select ordinal, reason, provider, provider_request_id
         from knowledge.analysis_attempts where run_id = $1 order by ordinal",
    )
    .bind(run.id)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(
        rows,
        [
            (
                1,
                "initial".to_owned(),
                "fake".to_owned(),
                "request-1".to_owned()
            ),
            (
                2,
                "repair".to_owned(),
                "fake".to_owned(),
                "request-2".to_owned()
            )
        ]
    );
    database.cleanup().await?;
    Ok(())
}

fn source(
    tenant: TenantRef,
    document_id: DocumentId,
    digit: char,
) -> Result<SourceReference, ratatoskr_identifiers::IdentifierError> {
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    };
    Ok(SourceReference {
        tenant,
        owner_context: "ratatoskr-extractor".to_owned(),
        ai_archive_id: String::new(),
        document_id,
        content_digest: digest.clone(),
        source_blob: BlobRef {
            owner_service: BlobOwner::parse("ratatoskr-extractor")?,
            digest,
            media_type: MediaType::parse("application/json")?,
            length_bytes: 128,
        },
    })
}

fn identity(source_revision_id: uuid::Uuid, model_policy: String) -> AnalysisIdentity {
    AnalysisIdentity {
        source_revision_id,
        contract_version: "article_v1".to_owned(),
        prompt_version: "article_prompt_v1".to_owned(),
        context_builder_version: "document_context_v1".to_owned(),
        model_policy,
    }
}

fn attempt(reason: AttemptReason, request_id: &str) -> AttemptInput {
    AttemptInput {
        reason,
        provider: "fake".to_owned(),
        model: "fake_default_v1".to_owned(),
        model_policy: "attempt_policy".to_owned(),
        provider_request_id: Some(request_id.to_owned()),
        outcome: AttemptOutcome::Requested,
    }
}
