//! The repository family waits out a provider that only an operator or the spend window can fix,
//! and keeps its five-delivery budget for plain transient faults (XR-021 CONTRACTS.md R6-01).
//!
//! The provider refuses the call for eight deliveries, past the budget of five, and then answers.
//! The request must stay pending the whole time: no `failed` fact is ever published, the run's
//! calls are not spent, the redeliveries are counted and logged without content, and one completed
//! fact ends the wait.

use std::sync::Arc;
use std::time::Duration;

use ratatoskr_github_contracts::{AnalysisFailureCode, RepositoryAnalysisFailed};
use ratatoskr_knowledge::{LlmProvider, ProviderError, ScriptedProvider};
use ratatoskr_knowledge_service::IngestFamily;

use crate::fixtures::{HttpStub, README, REPOSITORY_ANALYSIS_PRESENT, response, routed};
use crate::harness::{BROKER, Harness};
use crate::ingest_support::{TestResult, eventually};
use crate::log_capture::{Captured, capture};
use crate::redelivery::{
    DELIVERIES_PAST_THE_OLD_BUDGET, assert_counted, quiet, redelivered_until, rejecting_provider,
};
use crate::repository::{only_fact, publish_request, readme_path, request_state};

const COMPLETED_SUBJECT: &str = "evt.knowledge.repository_analysis.completed.v1";
const FAILED_SUBJECT: &str = "evt.knowledge.repository_analysis.failed.v1";

/// Polls until the completed fact is on the stream, failing the moment a failed fact appears.
async fn completes_without_a_failed_fact(harness: &Harness) -> TestResult {
    eventually("the completed fact is published", || async {
        if !harness.facts_on(FAILED_SUBJECT).await?.is_empty() {
            return Err("a failed fact was published while the request had to wait".into());
        }
        Ok(harness.facts_on(COMPLETED_SUBJECT).await?.len() == 1)
    })
    .await
}

/// Runs one repository request against `provider` until it completes, then checks everything the
/// wait must leave behind except the provider's own request count. `class` is the class every
/// redelivery from the second on is logged under.
async fn waits_out<P: LlmProvider + 'static>(
    provider: Arc<P>,
    class: &str,
) -> Result<(Harness, Captured), Box<dyn std::error::Error>> {
    let (logs, _guard) = capture();
    let harness = Harness::start_fast().await?;
    let stub = HttpStub::start(vec![(readme_path(), 200, README.to_vec())]).await?;
    let config = publish_request(&harness, &stub).await?;
    let worker = harness.spawn_with(config, provider);

    completes_without_a_failed_fact(&harness).await?;
    redelivered_until(
        &harness,
        IngestFamily::Repository,
        DELIVERIES_PAST_THE_OLD_BUDGET + 1,
    )
    .await?;
    quiet(&harness, IngestFamily::Repository).await?;
    assert!(harness.facts_on(FAILED_SUBJECT).await?.is_empty());
    assert_eq!(harness.facts_on(COMPLETED_SUBJECT).await?.len(), 1);
    assert_eq!(request_state(&harness).await?, "completed");
    assert_eq!(harness.search_documents().await?, 1);
    assert_eq!(
        harness
            .rows_in(&["analysis_runs", "analysis_outputs"])
            .await?,
        2
    );
    assert_eq!(
        spent_attempts(&harness).await?,
        1,
        "only the accepted answer is a spent call"
    );
    assert_counted(&harness).await?;
    assert_logged_without_content(&logs, class);
    worker.stop().await?;
    Ok((harness, logs))
}

/// The attempts of every run that count against the per-run budget: all but transient failures.
async fn spent_attempts(harness: &Harness) -> Result<i64, sqlx::Error> {
    harness
        .count(
            "select count(*) from knowledge.analysis_attempts where outcome <> 'transient_failure'",
        )
        .await
}

fn assert_logged_without_content(logs: &Captured, class: &str) {
    let warnings: Vec<String> = logs
        .text()
        .lines()
        .filter(|line| line.contains("will be redelivered"))
        .map(str::to_owned)
        .collect();
    for delivered in 2..=DELIVERIES_PAST_THE_OLD_BUDGET {
        assert!(
            warnings.iter().any(|line| {
                line.contains("family=\"repository\"")
                    && line.contains(&format!("class=\"{class}\""))
                    && line.contains(&format!("delivered={delivered}"))
            }),
            "delivery {delivered} is logged with its family and class {class}: {warnings:?}"
        );
    }
    assert!(
        !warnings.iter().any(|line| line.contains("delivered=1")),
        "the first failure is not a redelivery"
    );
    assert_eq!(logs.lines_containing("sk-or-v1-LEAKME"), 0, "no credential");
    assert_eq!(logs.lines_containing("A Rust library"), 0, "no content");
    assert_eq!(logs.lines_containing("Quokkas"), 0, "no README");
}

/// A real adapter that is refused with 401 on eight deliveries: the credential is an operator
/// matter, so the request waits and completes once it works.
#[tokio::test]
async fn a_rejected_credential_does_not_lose_a_repository_request() -> TestResult {
    let _serial = BROKER.lock().await;
    let (transport, provider) = rejecting_provider(
        401,
        usize::try_from(DELIVERIES_PAST_THE_OLD_BUDGET)?,
        REPOSITORY_ANALYSIS_PRESENT,
    )
    .await?;
    let (harness, _logs) = waits_out(provider, "provider_operator_action").await?;
    assert_eq!(
        transport.request_count()?,
        9,
        "eight rejections, one answer"
    );
    harness.database.cleanup().await?;
    Ok(())
}

/// The adapter's classification is not what makes the wait: a provider that reports an operator
/// failure on eight deliveries is waited out the same way.
#[tokio::test]
async fn an_operator_action_failure_does_not_lose_a_repository_request() -> TestResult {
    let _serial = BROKER.lock().await;
    let mut script: Vec<_> = (0..DELIVERIES_PAST_THE_OLD_BUDGET)
        .map(|_| Err(ProviderError::OperatorAction))
        .collect();
    script.push(Ok(response(REPOSITORY_ANALYSIS_PRESENT)));
    let (harness, _logs) = waits_out(
        Arc::new(ScriptedProvider::new(script)),
        "provider_operator_action",
    )
    .await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A spend ceiling that refuses the call on eight deliveries and then lifts: the request is
/// redelivered, never settled, and completes after the rollover.
#[tokio::test]
async fn a_spend_budget_refusal_does_not_lose_a_repository_request() -> TestResult {
    let _serial = BROKER.lock().await;
    let mut script: Vec<_> = (0..DELIVERIES_PAST_THE_OLD_BUDGET)
        .map(|_| Err(ProviderError::BudgetExhausted))
        .collect();
    script.push(Ok(response(REPOSITORY_ANALYSIS_PRESENT)));
    let (harness, _logs) = waits_out(
        Arc::new(ScriptedProvider::new(script)),
        "repository_analysis",
    )
    .await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// The budget that stays: a README source that fails four deliveries and then recovers completes
/// on the fifth, with no failed fact.
#[tokio::test]
async fn a_readme_source_that_recovers_within_the_budget_completes() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let script = vec![
        (503, Vec::new()),
        (503, Vec::new()),
        (503, Vec::new()),
        (503, Vec::new()),
    ]
    .into_iter()
    .chain([(200, README.to_vec())])
    .collect();
    let stub = HttpStub::start_scripted(vec![(readme_path(), script)]).await?;
    let config = publish_request(&harness, &stub).await?;
    let worker = harness.spawn_with(
        config,
        routed(&[("repository_prompt_v1", REPOSITORY_ANALYSIS_PRESENT)]),
    );

    completes_without_a_failed_fact(&harness).await?;
    harness.settled(IngestFamily::Repository, 5).await?;
    assert_eq!(stub.hits(&readme_path()), 5, "one README read per delivery");
    assert_eq!(request_state(&harness).await?, "completed");
    assert!(harness.facts_on(FAILED_SUBJECT).await?.is_empty());
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// The budget that stays: a transient provider failure on five deliveries settles the retryable
/// `dependency_unavailable` fact exactly once, and the request is not redelivered after it.
#[tokio::test]
async fn a_transient_provider_failure_still_settles_one_failed_fact_at_the_fifth_delivery()
-> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let stub = HttpStub::start(vec![(readme_path(), 200, README.to_vec())]).await?;
    let config = publish_request(&harness, &stub).await?;
    let script: Vec<_> = (0..20).map(|_| Err(ProviderError::Transient)).collect();
    let worker = harness.spawn_with(config, Arc::new(ScriptedProvider::new(script)));

    eventually("the failed fact is published", || async {
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
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.settled(IngestFamily::Repository, 5).await?;
    assert_eq!(harness.facts_on(FAILED_SUBJECT).await?.len(), 1);
    assert!(harness.facts_on(COMPLETED_SUBJECT).await?.is_empty());
    assert_eq!(request_state(&harness).await?, "failed");
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}
