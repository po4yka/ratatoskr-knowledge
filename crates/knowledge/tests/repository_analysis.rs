//! Repository-analysis intake and terminal-linkage integration tests.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "assertions and synthetic fixture construction in a test binary"
)]

use ratatoskr_github_contracts::{
    AnalysisFailureCode, ReadmeAbsenceReason, ReadmeRevision, RepositoryAnalysisAttributes,
    RepositoryAnalysisContract, RepositoryAnalysisRequested, RepositoryAnalysisRevision,
    RepositoryFullName,
};
use ratatoskr_identifiers::{
    ContentDigest, DigestAlgorithm, DigestHex, EntityRef, Extensions, RepositoryAnalysisRequestId,
    RepositoryId, TenantRef, UserId,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    RepositoryAnalysisAdmission, RepositoryAnalysisConsumer, RepositoryAnalysisError,
};

/// A redelivered immutable request creates one durable pending record.
#[tokio::test]
async fn repository_request_redelivery_creates_one_pending_record()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let consumer = RepositoryAnalysisConsumer::new(&database.database);
    let request = request('a');

    assert_eq!(
        consumer.accept(&request).await?,
        RepositoryAnalysisAdmission::Accepted
    );
    assert_eq!(
        consumer.accept(&request).await?,
        RepositoryAnalysisAdmission::Duplicate
    );
    let count: i64 =
        sqlx::query_scalar("select count(*) from knowledge.repository_analysis_requests")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(count, 1);

    database.cleanup().await?;
    Ok(())
}

/// A reused idempotency digest may only acknowledge the same immutable request.
#[tokio::test]
async fn conflicting_request_with_the_same_idempotency_digest_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let consumer = RepositoryAnalysisConsumer::new(&database.database);
    let request = request('d');
    consumer.accept(&request).await?;
    let mut conflicting = request.clone();
    conflicting.request_id =
        RepositoryAnalysisRequestId::parse("018f0000-0000-7000-8000-000000000705")?;
    conflicting.repository_attributes.repository_full_name =
        RepositoryFullName::parse("different/repository")?;

    let outcome = consumer.accept(&conflicting).await;
    assert!(matches!(
        outcome,
        Err(RepositoryAnalysisError::IdempotencyConflict)
    ));
    let count: i64 =
        sqlx::query_scalar("select count(*) from knowledge.repository_analysis_requests")
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(count, 1);

    database.cleanup().await?;
    Ok(())
}

/// Only an exact pending revision yields one completion fact; a mismatched revision cannot consume it.
#[tokio::test]
async fn completion_only_publishes_for_the_matching_pending_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let consumer = RepositoryAnalysisConsumer::new(&database.database);
    let request = request('a');
    consumer.accept(&request).await?;
    let mut mismatched = request.clone();
    mismatched.source_revision.attributes_digest = digest('b')?;
    let result = EntityRef::parse("analysis:018f0000-0000-7000-8000-000000000703")?;

    assert!(
        consumer
            .complete(&mismatched, result.clone())
            .await?
            .is_none()
    );
    let completion = consumer
        .complete(&request, result)
        .await?
        .expect("the matching request completes");
    assert_eq!(completion.request_id, request.request_id);
    assert_eq!(completion.source_revision, request.source_revision);
    assert!(
        consumer
            .complete(
                &request,
                EntityRef::parse("analysis:018f0000-0000-7000-8000-000000000704")?
            )
            .await?
            .is_none()
    );

    database.cleanup().await?;
    Ok(())
}

/// A final failure is linked to its immutable pending request and is emitted once.
#[tokio::test]
async fn final_failure_only_publishes_once_without_a_result_reference()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let consumer = RepositoryAnalysisConsumer::new(&database.database);
    let request = request('c');
    consumer.accept(&request).await?;

    let failure = consumer
        .fail(&request, AnalysisFailureCode::DependencyUnavailable, true)
        .await?
        .expect("the pending request fails once");
    assert_eq!(failure.request_id, request.request_id);
    assert_eq!(failure.source_revision, request.source_revision);
    assert_eq!(
        failure.failure_code,
        AnalysisFailureCode::DependencyUnavailable
    );
    assert!(failure.retryable);
    assert!(
        consumer
            .fail(&request, AnalysisFailureCode::DependencyUnavailable, true,)
            .await?
            .is_none()
    );
    let terminal: (String, Option<String>) = sqlx::query_as(
        "select state, analysis_result_ref
         from knowledge.repository_analysis_requests where request_id = $1",
    )
    .bind(uuid::Uuid::parse_str(&request.request_id.to_string())?)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(terminal, ("failed".to_owned(), None));

    database.cleanup().await?;
    Ok(())
}

fn request(digit: char) -> RepositoryAnalysisRequested {
    RepositoryAnalysisRequested {
        owner: TenantRef::of_user(UserId::new_v7()),
        repository_id: RepositoryId::parse("018f0000-0000-7000-8000-000000000701")
            .expect("repository identity"),
        github_repository_numeric_id: 42,
        request_id: RepositoryAnalysisRequestId::parse("018f0000-0000-7000-8000-000000000702")
            .expect("request identity"),
        source_revision: RepositoryAnalysisRevision {
            attributes_digest: digest(digit).expect("digest"),
            readme: ReadmeRevision::Absent {
                reason: ReadmeAbsenceReason::NotFound,
            },
        },
        repository_attributes: RepositoryAnalysisAttributes {
            repository_full_name: RepositoryFullName::parse("owner/repository").expect("alias"),
            description: None,
            primary_language: None,
        },
        requested_contract: RepositoryAnalysisContract::RepositoryAnalysis,
        idempotency_key: digest(digit).expect("idempotency digest"),
        extensions: Extensions::new(),
    }
}

fn digest(digit: char) -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    let hex = digit.to_string().repeat(64);
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&hex)?,
    })
}
