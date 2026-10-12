//! The repository-request lane of the ingest worker through a real `JetStream` and a loopback
//! README source: success, a transient source failure that becomes terminal on the fifth
//! delivery, a permanent integrity failure, and transient database failures in the intake and
//! in the settlement (XR-021 CONTRACTS.md S02 rule 7 and S09).

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
    let harness = Harness::start_fast().await?;
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

/// Makes the first `failures` writes of `event` against the request table fail inside the
/// database, then lets later ones through.
///
/// `event` is `insert`, or `update` with `when_state` naming the state the update writes. The
/// sequence counts every attempt, including the failed ones, because a sequence is not rolled back.
async fn fail_first_writes(
    harness: &Harness,
    event: &str,
    when_state: &str,
    failures: i64,
) -> Result<(), sqlx::Error> {
    let pool = harness.database.database.pool();
    for statement in [
        "create sequence knowledge.test_write_attempts".to_owned(),
        "create function knowledge.test_fail_first() returns trigger language plpgsql as $$
         begin
           if nextval('knowledge.test_write_attempts') <= tg_argv[0]::bigint then
             raise exception 'injected transient write failure';
           end if;
           return new;
         end $$"
            .to_owned(),
        format!(
            "create trigger test_fail_first before {event} on knowledge.repository_analysis_requests
             for each row when ({when_state}) execute function knowledge.test_fail_first({failures})"
        ),
    ] {
        sqlx::query(&statement).execute(pool).await?;
    }
    Ok(())
}

async fn write_attempts(harness: &Harness) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("select last_value from knowledge.test_write_attempts")
        .fetch_one(harness.database.database.pool())
        .await
}

/// A database failure while accepting the request is transient: the first delivery is naked, not
/// acknowledged and not settled, so the second delivery completes the request.
#[tokio::test]
async fn a_transient_intake_failure_is_retried_and_then_completes() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    fail_first_writes(&harness, "insert", "true", 1).await?;
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
    harness.settled(IngestFamily::Repository, 2).await?;
    assert_eq!(request_state(&harness).await?, "completed");
    assert!(
        write_attempts(&harness).await? >= 2,
        "the first insert failed and the second delivery inserted"
    );
    assert!(
        harness.facts_on(FAILED_SUBJECT).await?.is_empty(),
        "a transient database failure must not produce a terminal failure"
    );
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A database failure while settling a request that was analysed successfully is transient: the
/// request is not settled as failed, and the redelivery completes it.
#[tokio::test]
async fn a_transient_settlement_failure_does_not_fail_an_analysed_request() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    fail_first_writes(&harness, "update", "new.state = 'completed'", 1).await?;
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
    harness.settled(IngestFamily::Repository, 2).await?;
    assert_eq!(request_state(&harness).await?, "completed");
    assert!(
        harness.facts_on(FAILED_SUBJECT).await?.is_empty(),
        "an analysed request must not be settled as a permanent contract failure"
    );
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// When the intake keeps failing until the delivery budget runs out, the last delivery still
/// leaves a terminal failed fact: the handler records the request before it settles it, so a
/// request that was never stored is not acknowledged without a fact.
#[tokio::test]
async fn an_intake_that_never_succeeded_still_ends_in_one_retryable_failed_fact() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    fail_first_writes(&harness, "insert", "true", 5).await?;
    let stub = HttpStub::start(vec![(readme_path(), 200, README.to_vec())]).await?;
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
    assert_eq!(
        payload.failure_code,
        AnalysisFailureCode::DependencyUnavailable
    );
    assert!(payload.retryable);
    harness.settled(IngestFamily::Repository, 5).await?;
    assert_eq!(request_state(&harness).await?, "failed");
    assert!(harness.facts_on(COMPLETED_SUBJECT).await?.is_empty());
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A database failure while settling the request on the terminal delivery must not end the
/// delivery without a fact: the settlement is redelivered past the delivery budget until the
/// failed fact is committed.
#[tokio::test]
async fn a_settlement_failure_on_the_fifth_delivery_is_redelivered_until_the_failed_fact()
-> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    fail_first_writes(&harness, "update", "new.state = 'failed'", 1).await?;
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
    harness.settled(IngestFamily::Repository, 6).await?;
    assert_eq!(request_state(&harness).await?, "failed");
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}
