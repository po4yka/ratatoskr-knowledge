//! The repository-request lane of the ingest worker through a real `JetStream` and a loopback
//! README source: success, a transient source failure that becomes terminal on the fifth
//! delivery, and a permanent integrity failure (XR-021 CONTRACTS.md S09).

use std::time::Duration;

use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_github_contracts::{
    AnalysisFailureCode, RepositoryAnalysisCompleted, RepositoryAnalysisFailed,
};
use ratatoskr_knowledge_service::IngestFamily;

use crate::fixtures::{
    EVENT_A, HttpStub, OWNER, README, REPOSITORY_ANALYSIS_PRESENT, REPOSITORY_REQUESTED, envelope,
    readme_hex, repository_request, routed,
};
use crate::harness::{BROKER, Harness};
use crate::ingest_support::{TestResult, eventually};

const REQUESTED_SUBJECT: &str = "evt.knowledge.repository_analysis.requested.v1";
const COMPLETED_SUBJECT: &str = "evt.knowledge.repository_analysis.completed.v1";
const FAILED_SUBJECT: &str = "evt.knowledge.repository_analysis.failed.v1";

fn readme_path() -> String {
    format!("/internal/v1/readme-blobs/{}", readme_hex(README))
}

/// Publishes the request for `README` and returns the worker configuration that reads it from
/// `stub`.
async fn publish_request(
    harness: &Harness,
    stub: &HttpStub,
) -> Result<ratatoskr_knowledge::Config, Box<dyn std::error::Error>> {
    let mut config = harness.config();
    config.ingest.github_readme_base_url = format!("http://{}/", stub.address());
    harness
        .publish(
            REQUESTED_SUBJECT,
            EVENT_A,
            envelope(
                EVENT_A,
                REPOSITORY_REQUESTED,
                "ratatoskr-github",
                Some(OWNER),
                &serde_json::to_value(repository_request(OWNER, Some(README))?)?,
            ),
        )
        .await?;
    Ok(config)
}

async fn request_state(harness: &Harness) -> Result<String, sqlx::Error> {
    sqlx::query_scalar("select state from knowledge.repository_analysis_requests")
        .fetch_one(harness.database.database.pool())
        .await
}

async fn only_fact(
    harness: &Harness,
    subject: &str,
) -> Result<EventEnvelope, Box<dyn std::error::Error>> {
    let mut facts = harness.facts_on(subject).await?;
    if facts.len() != 1 {
        return Err(format!("expected one fact on {subject}, found {}", facts.len()).into());
    }
    Ok(facts.remove(0).0)
}

/// The README is read over the authenticated route, the request settles, the repository becomes
/// searchable and the completed fact reaches the stream exactly once.
#[tokio::test]
async fn readme_request_settles_and_publishes_one_completed_fact() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let stub = HttpStub::start(vec![(readme_path(), 200, README.to_vec())]).await?;
    let config = publish_request(&harness, &stub).await?;
    let worker = harness.spawn_with(
        config,
        routed(&[("repository_prompt_v1", REPOSITORY_ANALYSIS_PRESENT)]),
    );

    eventually("the completed fact is published", || async {
        Ok(harness.facts_on(COMPLETED_SUBJECT).await?.len() == 1)
    })
    .await?;
    harness.settled(IngestFamily::Repository, 1).await?;
    let completed = only_fact(&harness, COMPLETED_SUBJECT).await?;
    let payload = completed.payload_as::<RepositoryAnalysisCompleted>()?;
    assert_eq!(payload.owner.to_string(), OWNER);
    assert_eq!(request_state(&harness).await?, "completed");
    assert_eq!(harness.search_documents().await?, 1);
    assert!(harness.facts_on(FAILED_SUBJECT).await?.is_empty());
    assert_eq!(stub.hits(&readme_path()), 1);
    let first = stub
        .requests()
        .into_iter()
        .next()
        .ok_or("no README request")?;
    assert_eq!(first.authorization.as_deref(), Some("Bearer readme-secret"));
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A README source that keeps failing is retried with a delay; the fifth delivery settles the
/// request as failed and retryable, and nothing is left unacknowledged.
#[tokio::test]
async fn unavailable_readme_is_naked_until_the_fifth_delivery_then_fails_retryably() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let stub = HttpStub::start(vec![(readme_path(), 503, Vec::new())]).await?;
    let config = publish_request(&harness, &stub).await?;
    let worker = harness.spawn_with(
        config,
        routed(&[("repository_prompt_v1", REPOSITORY_ANALYSIS_PRESENT)]),
    );

    eventually("the terminal failure is published", || async {
        Ok(harness.facts_on(FAILED_SUBJECT).await?.len() == 1)
    })
    .await?;
    let failed = only_fact(&harness, FAILED_SUBJECT).await?;
    let payload = failed.payload_as::<RepositoryAnalysisFailed>()?;
    assert_eq!(payload.failure_code, AnalysisFailureCode::SourceUnavailable);
    assert!(payload.retryable);
    harness.settled(IngestFamily::Repository, 5).await?;
    assert_eq!(stub.hits(&readme_path()), 5, "one README read per delivery");
    assert_eq!(request_state(&harness).await?, "failed");
    assert!(harness.facts_on(COMPLETED_SUBJECT).await?.is_empty());
    assert_eq!(harness.search_documents().await?, 0);
    assert_eq!(
        harness.rows_in(&["ingest_failures"]).await?,
        0,
        "the request settled through its own terminal fact"
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    harness.settled(IngestFamily::Repository, 5).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// Bytes that are not the referenced README can never succeed, so the first delivery settles the
/// request as failed and not retryable.
#[tokio::test]
async fn a_readme_that_fails_its_digest_fails_permanently_on_the_first_delivery() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let mut tampered = README.to_vec();
    tampered.reverse();
    let stub = HttpStub::start(vec![(readme_path(), 200, tampered)]).await?;
    let config = publish_request(&harness, &stub).await?;
    let worker = harness.spawn_with(
        config,
        routed(&[("repository_prompt_v1", REPOSITORY_ANALYSIS_PRESENT)]),
    );

    eventually("the terminal failure is published", || async {
        Ok(harness.facts_on(FAILED_SUBJECT).await?.len() == 1)
    })
    .await?;
    let failed = only_fact(&harness, FAILED_SUBJECT).await?;
    let payload = failed.payload_as::<RepositoryAnalysisFailed>()?;
    assert_eq!(payload.failure_code, AnalysisFailureCode::ContractInvalid);
    assert!(!payload.retryable);
    harness.settled(IngestFamily::Repository, 1).await?;
    assert_eq!(stub.hits(&readme_path()), 1);
    assert_eq!(request_state(&harness).await?, "failed");
    assert_eq!(harness.search_documents().await?, 0);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}
