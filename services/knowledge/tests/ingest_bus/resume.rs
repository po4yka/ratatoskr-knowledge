//! A redelivery resumes an analysis run from its durable state in every family lane (XR-021
//! CONTRACTS.md S02 rule 7 and S09).
//!
//! Two ways a run is left unfinished are exercised: a provider that fails transiently on every
//! call of the first delivery (the run fails, the delivery is naked), and a worker killed while
//! the provider call is in flight (the run rests in `model_requested`). The next delivery must
//! finish the same run, so the family ends with one persisted output and one search document.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ratatoskr_github_contracts::RepositoryAnalysisCompleted;
use ratatoskr_knowledge::{
    Config, GenerationRequest, LlmProvider, ProviderError, ProviderFailure, ProviderIdentity,
    ProviderResponse, ScriptedProvider,
};
use ratatoskr_knowledge_service::IngestFamily;

use crate::fixtures::{
    ARCHIVE_ANALYSIS, ARTICLE_ANALYSIS, CAPTURED, CONTRACT_CONVERSATION, EVENT_A, HttpStub, OWNER,
    README, REPOSITORY_ANALYSIS_PRESENT, REPOSITORY_REQUESTED, SOCIAL_ANALYSIS, envelope,
    extracted_document, readme_hex, repository_request, response, social_payload,
};
use crate::harness::{BROKER, Harness};
use crate::ingest_support::{TestResult, eventually};

const COMPLETED_SUBJECT: &str = "evt.knowledge.repository_analysis.completed.v1";
const FAILED_SUBJECT: &str = "evt.knowledge.repository_analysis.failed.v1";

/// The ack wait that brings a delivery abandoned by a killed worker back within the test deadline.
const SHORT_ACK_WAIT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy)]
enum Lane {
    Documents,
    Social,
    Archive,
    Repository,
}

impl Lane {
    const fn family(self) -> IngestFamily {
        match self {
            Self::Documents => IngestFamily::Documents,
            Self::Social => IngestFamily::Social,
            Self::Archive => IngestFamily::AiArchive,
            Self::Repository => IngestFamily::Repository,
        }
    }

    /// The provider answer that is valid for this lane.
    const fn answer(self) -> &'static str {
        match self {
            Self::Documents => ARTICLE_ANALYSIS,
            Self::Social => SOCIAL_ANALYSIS,
            Self::Archive => ARCHIVE_ANALYSIS,
            Self::Repository => REPOSITORY_ANALYSIS_PRESENT,
        }
    }
}

/// What a lane needs kept alive while its events are processed.
struct Arranged {
    config: Config,
    _stub: Option<HttpStub>,
}

/// Publishes the lane's event and returns the worker configuration that can analyse it.
async fn arrange(harness: &Harness, lane: Lane) -> Result<Arranged, Box<dyn std::error::Error>> {
    let mut config = harness.config();
    let mut stub = None;
    let (subject, event_type, producer, payload) = match lane {
        Lane::Documents => (
            "evt.content.document.extracted.v1",
            "content.document.extracted.v1",
            "ratatoskr-extractor",
            extracted_document()?,
        ),
        Lane::Social => (
            "evt.social.source.captured.v1",
            CAPTURED,
            "ratatoskr-x",
            social_payload("2026-08-17T10:00:00Z")?,
        ),
        Lane::Archive => (
            "evt.ai_archive.conversation.added.v1",
            "ai_archive.conversation.added.v1",
            "ratatoskr-chatgpt",
            serde_json::from_str(CONTRACT_CONVERSATION)?,
        ),
        Lane::Repository => {
            let served = HttpStub::start(vec![(
                format!("/internal/v1/readme-blobs/{}", readme_hex(README)),
                200,
                README.to_vec(),
            )])
            .await?;
            config.ingest.github_readme_base_url = format!("http://{}/", served.address());
            stub = Some(served);
            (
                "evt.knowledge.repository_analysis.requested.v1",
                REPOSITORY_REQUESTED,
                "ratatoskr-github",
                serde_json::to_value(repository_request(OWNER, Some(README))?)?,
            )
        }
    };
    harness
        .publish(
            subject,
            EVENT_A,
            envelope(EVENT_A, event_type, producer, Some(OWNER), &payload),
        )
        .await?;
    Ok(Arranged {
        config,
        _stub: stub,
    })
}

/// One run, one accepted output and one search document; a repository request also settled as
/// completed with exactly one completed fact and no failed fact.
async fn assert_converged(harness: &Harness, lane: Lane) -> TestResult {
    eventually("the analysis becomes searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    harness.settled(lane.family(), 2).await?;
    assert_eq!(harness.rows_in(&["analysis_runs"]).await?, 1);
    assert_eq!(
        harness
            .count("select count(*) from knowledge.analysis_runs where state = 'persisted'")
            .await?,
        1
    );
    assert_eq!(harness.rows_in(&["analysis_outputs"]).await?, 1);
    if matches!(lane, Lane::Repository) {
        eventually("the completed fact is published", || async {
            Ok(harness.facts_on(COMPLETED_SUBJECT).await?.len() == 1)
        })
        .await?;
        let completed = harness.facts_on(COMPLETED_SUBJECT).await?;
        let (fact, _) = completed.first().ok_or("no completed fact")?;
        assert_eq!(
            fact.payload_as::<RepositoryAnalysisCompleted>()?
                .owner
                .to_string(),
            OWNER
        );
        assert!(harness.facts_on(FAILED_SUBJECT).await?.is_empty());
        assert_eq!(
            harness
                .count(
                    "select count(*) from knowledge.repository_analysis_requests
                     where state = 'completed'"
                )
                .await?,
            1
        );
    }
    Ok(())
}

/// Answers every call, except that the first call never returns.
struct HangsOnce {
    answer: &'static str,
    hung: AtomicBool,
    entered: Arc<AtomicBool>,
}

impl LlmProvider for HangsOnce {
    fn identity(&self) -> ProviderIdentity {
        ProviderIdentity {
            provider: "scripted_fake".to_owned(),
            model: "fake_default_v1".to_owned(),
        }
    }

    fn generate_json(
        &self,
        _request: GenerationRequest,
    ) -> impl Future<Output = Result<ProviderResponse, ProviderFailure>> + Send {
        let hang = !self.hung.swap(true, Ordering::SeqCst);
        let entered = Arc::clone(&self.entered);
        let answer = self.answer;
        async move {
            if hang {
                entered.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            Ok(response(answer))
        }
    }
}

/// Every call of the first delivery fails transiently, so the run fails; the second delivery's
/// call answers, and the same run completes.
async fn transient_failure_then_redelivery(lane: Lane) -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start().await?;
    let provider = Arc::new(ScriptedProvider::new([
        Err(ProviderError::Transient),
        Err(ProviderError::Transient),
        Ok(response(lane.answer())),
    ]));
    let arranged = arrange(&harness, lane).await?;
    let worker = harness.spawn_with(arranged.config, Arc::clone(&provider));

    assert_converged(&harness, lane).await?;
    assert_eq!(provider.call_count()?, 3);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// The worker dies while the provider call is in flight, leaving the run in `model_requested`;
/// the redelivery to a new worker finishes the same run.
async fn killed_mid_call_then_redelivery(lane: Lane) -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_with_ack_wait(SHORT_ACK_WAIT).await?;
    let entered = Arc::new(AtomicBool::new(false));
    let arranged = arrange(&harness, lane).await?;
    let doomed = harness.spawn_with(
        arranged.config.clone(),
        Arc::new(HangsOnce {
            answer: lane.answer(),
            hung: AtomicBool::new(false),
            entered: Arc::clone(&entered),
        }),
    );
    eventually("the provider call is in flight", || async {
        Ok(entered.load(Ordering::SeqCst))
    })
    .await?;
    doomed.kill().await;
    let state: String = sqlx::query_scalar("select state from knowledge.analysis_runs")
        .fetch_one(harness.database.database.pool())
        .await?;
    assert_eq!(state, "model_requested", "the kill left the run mid-call");

    let provider = Arc::new(ScriptedProvider::new([Ok(response(lane.answer()))]));
    let survivor = harness.spawn_with(arranged.config, Arc::clone(&provider));
    assert_converged(&harness, lane).await?;
    assert_eq!(provider.call_count()?, 1);
    survivor.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn repository_request_completes_after_two_transient_provider_failures() -> TestResult {
    transient_failure_then_redelivery(Lane::Repository).await
}

#[tokio::test]
async fn social_capture_completes_after_two_transient_provider_failures() -> TestResult {
    transient_failure_then_redelivery(Lane::Social).await
}

#[tokio::test]
async fn archive_conversation_completes_after_two_transient_provider_failures() -> TestResult {
    transient_failure_then_redelivery(Lane::Archive).await
}

#[tokio::test]
async fn document_completes_after_two_transient_provider_failures() -> TestResult {
    transient_failure_then_redelivery(Lane::Documents).await
}

#[tokio::test]
async fn repository_request_resumes_a_run_killed_mid_call() -> TestResult {
    killed_mid_call_then_redelivery(Lane::Repository).await
}

#[tokio::test]
async fn social_capture_resumes_a_run_killed_mid_call() -> TestResult {
    killed_mid_call_then_redelivery(Lane::Social).await
}

#[tokio::test]
async fn archive_conversation_resumes_a_run_killed_mid_call() -> TestResult {
    killed_mid_call_then_redelivery(Lane::Archive).await
}

#[tokio::test]
async fn document_resumes_a_run_killed_mid_call() -> TestResult {
    killed_mid_call_then_redelivery(Lane::Documents).await
}
