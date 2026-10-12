//! A retryable ingest failure is redelivered until it succeeds, in every family that has no
//! settlement of its own (XR-021 CONTRACTS.md R4-05).
//!
//! A fault is forced from the database side (a trigger that refuses one write) or from the
//! provider side (a scripted provider that errors), kept for at least eight deliveries, and then
//! lifted. The fact must be applied once, no delivery may be terminated, and the operator plane
//! must have counted the redeliveries. The redelivery schedule is lowered through the spawn seam,
//! so the eight deliveries fit the polling deadline; the production schedule is pinned by a unit
//! test beside it.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt as _;
use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport};
use ratatoskr_knowledge::{
    OpenRouterProvider, OpenRouterSettings, ProviderError, ProviderSecret, RetryPolicy,
    ScriptedProvider,
};
use ratatoskr_knowledge_service::{IngestFamily, Lifecycle, admin_router};
use tower::ServiceExt as _;

use crate::fixtures::{
    ARCHIVE_ANALYSIS, ARTICLE_ANALYSIS, CAPTURED, CONTRACT_CONVERSATION, EVENT_A, EVENT_B,
    HttpStub, OWNER, README, REMOVED, REPOSITORY_ANALYSIS_PRESENT, REPOSITORY_REQUESTED,
    SOCIAL_ANALYSIS, envelope, extracted_document, readme_hex, removal_payload, repository_request,
    response, routed, social_payload,
};
use crate::harness::{BROKER, Harness};
use crate::ingest_support::{TestResult, eventually};
use crate::log_capture::capture;

/// The deliveries a fault must outlast: three more than the budget that used to end a delivery.
const DELIVERIES_PAST_THE_OLD_BUDGET: u64 = 8;

const COMPLETED_SUBJECT: &str = "evt.knowledge.analysis.completed.v1";
const FAILED_SUBJECT: &str = "evt.knowledge.repository_analysis.failed.v1";

/// Refuses every insert into `knowledge.<table>` until [`allow_writes`].
async fn refuse_writes(harness: &Harness, table: &str) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(&format!(
        "create function knowledge.refuse_{table}() returns trigger language plpgsql as $$
         begin raise exception '{table} is refused for this test'; end $$;
         create trigger refuse_{table} before insert on knowledge.{table}
             for each row execute function knowledge.refuse_{table}();"
    ))
    .execute(harness.database.database.pool())
    .await?;
    Ok(())
}

async fn allow_writes(harness: &Harness, table: &str) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(&format!("drop trigger refuse_{table} on knowledge.{table}"))
        .execute(harness.database.database.pool())
        .await?;
    Ok(())
}

/// Every delivery of `family`, redeliveries included.
async fn deliveries(
    harness: &Harness,
    family: IngestFamily,
) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(harness.durable(family).await?.2)
}

/// Waits until `family` has delivered at least `count` times: the message was redelivered that
/// often, so none of those deliveries was terminated.
async fn redelivered_until(harness: &Harness, family: IngestFamily, count: u64) -> TestResult {
    eventually("the fault outlasts the old delivery budget", || async {
        Ok(deliveries(harness, family).await? >= count)
    })
    .await
}

/// Waits until `family` holds no open delivery and has nothing left to deliver.
async fn quiet(harness: &Harness, family: IngestFamily) -> TestResult {
    eventually("the durable is quiet", || async {
        let (ack_pending, pending, _) = harness.durable(family).await?;
        Ok(ack_pending == 0 && pending == 0)
    })
    .await
}

/// The value of `knowledge_ingest_redeliveries_total` as the operator listener serves it.
async fn operator_redeliveries(harness: &Harness) -> Result<u64, Box<dyn std::error::Error>> {
    let operator = admin_router(Lifecycle::starting(), Arc::clone(&harness.metrics));
    let response = operator
        .oneshot(Request::builder().uri("/metrics").body(Body::empty())?)
        .await?;
    let body = response.into_body().collect().await?.to_bytes();
    let exposition = String::from_utf8(body.to_vec())?;
    let value = exposition
        .lines()
        .find_map(|line| line.strip_prefix("knowledge_ingest_redeliveries_total "))
        .ok_or("the redelivery counter is not exposed")?;
    Ok(value.parse()?)
}

async fn assert_counted(harness: &Harness) -> TestResult {
    let counted = operator_redeliveries(harness).await?;
    assert!(
        counted >= DELIVERIES_PAST_THE_OLD_BUDGET - 1,
        "the operator plane counted {counted} redeliveries"
    );
    Ok(())
}

async fn publish_capture(harness: &Harness) -> TestResult {
    harness
        .publish(
            "evt.social.source.captured.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                CAPTURED,
                "ratatoskr-x",
                Some(OWNER),
                &social_payload("2026-08-17T10:00:00Z")?,
            ),
        )
        .await
}

/// A document whose registration is refused for eight deliveries is registered and analysed once
/// when the database recovers.
#[tokio::test]
async fn a_document_survives_more_than_five_transient_failures() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let worker = harness.spawn(Arc::new(ScriptedProvider::new([Ok(response(
        ARTICLE_ANALYSIS,
    ))])));
    refuse_writes(&harness, "source_refs").await?;
    harness
        .publish(
            "evt.content.document.extracted.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                "content.document.extracted.v1",
                "ratatoskr-extractor",
                Some(OWNER),
                &extracted_document()?,
            ),
        )
        .await?;

    redelivered_until(
        &harness,
        IngestFamily::Documents,
        DELIVERIES_PAST_THE_OLD_BUDGET,
    )
    .await?;
    assert_eq!(harness.search_documents().await?, 0);
    allow_writes(&harness, "source_refs").await?;
    eventually("the document becomes searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    quiet(&harness, IngestFamily::Documents).await?;
    assert_eq!(
        harness
            .rows_in(&["analysis_runs", "analysis_outputs"])
            .await?,
        2
    );
    assert_counted(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A social capture whose inbox receipt is refused for eight deliveries is analysed once when the
/// database recovers.
#[tokio::test]
async fn a_social_capture_survives_more_than_five_transient_failures() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let worker = harness.spawn(routed(&[("social_prompt_v1", SOCIAL_ANALYSIS)]));
    refuse_writes(&harness, "source_analysis_inbox").await?;
    publish_capture(&harness).await?;

    redelivered_until(
        &harness,
        IngestFamily::Social,
        DELIVERIES_PAST_THE_OLD_BUDGET,
    )
    .await?;
    assert_eq!(harness.search_documents().await?, 0);
    allow_writes(&harness, "source_analysis_inbox").await?;
    eventually("the capture becomes searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    quiet(&harness, IngestFamily::Social).await?;
    assert_eq!(
        harness
            .rows_in(&["analysis_runs", "analysis_outputs"])
            .await?,
        2
    );
    eventually("one completion fact is published", || async {
        Ok(harness.facts_on(COMPLETED_SUBJECT).await?.len() == 1)
    })
    .await?;
    assert_counted(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// An X deletion whose tombstone is refused for eight deliveries is applied when the database
/// recovers, and the source is gone from search.
#[tokio::test]
async fn a_social_removal_survives_more_than_five_transient_failures() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let worker = harness.spawn(routed(&[("social_prompt_v1", SOCIAL_ANALYSIS)]));
    publish_capture(&harness).await?;
    eventually("the capture is searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    quiet(&harness, IngestFamily::Social).await?;
    refuse_writes(&harness, "social_tombstones").await?;

    harness
        .publish(
            "evt.social.source.removed.v1",
            EVENT_B,
            envelope(
                EVENT_B,
                REMOVED,
                "ratatoskr-x",
                Some(OWNER),
                &removal_payload("2026-08-18T10:00:00Z"),
            ),
        )
        .await?;
    let removal_deliveries = 1 + DELIVERIES_PAST_THE_OLD_BUDGET;
    redelivered_until(&harness, IngestFamily::Social, removal_deliveries).await?;
    assert_eq!(
        harness.search_documents().await?,
        1,
        "nothing was removed while the tombstone was refused"
    );
    allow_writes(&harness, "social_tombstones").await?;
    eventually("the removal deletes the search document", || async {
        Ok(harness.search_documents().await? == 0)
    })
    .await?;
    quiet(&harness, IngestFamily::Social).await?;
    assert_eq!(harness.rows_in(&["social_tombstones"]).await?, 1);
    assert_counted(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// An AI conversation whose inbox receipt is refused for eight deliveries is analysed once when
/// the database recovers.
#[tokio::test]
async fn an_ai_archive_conversation_survives_more_than_five_transient_failures() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let worker = harness.spawn(routed(&[("archive_prompt_v1", ARCHIVE_ANALYSIS)]));
    refuse_writes(&harness, "source_analysis_inbox").await?;
    let conversation: serde_json::Value = serde_json::from_str(CONTRACT_CONVERSATION)?;
    harness
        .publish(
            "evt.ai_archive.conversation.added.v1",
            EVENT_A,
            envelope(
                EVENT_A,
                "ai_archive.conversation.added.v1",
                "ratatoskr-chatgpt",
                Some(OWNER),
                &conversation,
            ),
        )
        .await?;

    redelivered_until(
        &harness,
        IngestFamily::AiArchive,
        DELIVERIES_PAST_THE_OLD_BUDGET,
    )
    .await?;
    assert_eq!(harness.search_documents().await?, 0);
    allow_writes(&harness, "source_analysis_inbox").await?;
    eventually("the conversation becomes searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    quiet(&harness, IngestFamily::AiArchive).await?;
    assert_eq!(
        harness
            .rows_in(&["analysis_runs", "analysis_outputs"])
            .await?,
        2
    );
    assert_counted(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A provider that errors for eight deliveries (two calls each) and then answers: the capture is
/// analysed once and its completion fact is queued once, with no termination in between. A
/// provider outage is not a spend, so the run's budget of answers is not used up by it
/// (XR-021 CONTRACTS.md R4-05 rule 1).
#[tokio::test]
async fn a_provider_outage_longer_than_five_deliveries_does_not_lose_a_capture() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let mut script: Vec<_> = (0..DELIVERIES_PAST_THE_OLD_BUDGET * 2)
        .map(|_| Err(ProviderError::Transient))
        .collect();
    script.push(Ok(response(SOCIAL_ANALYSIS)));
    let worker = harness.spawn(Arc::new(ScriptedProvider::new(script)));
    publish_capture(&harness).await?;

    redelivered_until(
        &harness,
        IngestFamily::Social,
        DELIVERIES_PAST_THE_OLD_BUDGET,
    )
    .await?;
    eventually(
        "the capture is analysed once the provider answers",
        || async { Ok(harness.search_documents().await? == 1) },
    )
    .await?;
    eventually("the completion fact is published once", || async {
        Ok(harness.facts_on(COMPLETED_SUBJECT).await?.len() == 1)
    })
    .await?;
    quiet(&harness, IngestFamily::Social).await?;
    assert_eq!(
        harness
            .rows_in(&["analysis_runs", "analysis_outputs"])
            .await?,
        2
    );
    assert_eq!(harness.rows_in(&["event_outbox"]).await?, 1);
    assert_counted(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A spend ceiling that refuses the call for eight deliveries and then lifts: the capture is
/// redelivered, never termed, and analysed once after the rollover. The ceiling is a daily and
/// monthly window, so a refusal is a wait and not a verdict on the capture (XR-021 CONTRACTS.md
/// R4-05 rule 1).
#[tokio::test]
async fn a_spend_budget_refusal_longer_than_five_deliveries_does_not_lose_a_capture() -> TestResult
{
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let mut script: Vec<_> = (0..DELIVERIES_PAST_THE_OLD_BUDGET)
        .map(|_| Err(ProviderError::BudgetExhausted))
        .collect();
    script.push(Ok(response(SOCIAL_ANALYSIS)));
    let worker = harness.spawn(Arc::new(ScriptedProvider::new(script)));
    publish_capture(&harness).await?;

    redelivered_until(
        &harness,
        IngestFamily::Social,
        DELIVERIES_PAST_THE_OLD_BUDGET,
    )
    .await?;
    eventually("the capture is analysed once the budget clears", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    eventually("the completion fact is published once", || async {
        Ok(harness.facts_on(COMPLETED_SUBJECT).await?.len() == 1)
    })
    .await?;
    quiet(&harness, IngestFamily::Social).await?;
    assert_eq!(
        harness
            .rows_in(&["analysis_runs", "analysis_outputs"])
            .await?,
        2
    );
    assert_eq!(harness.rows_in(&["event_outbox"]).await?, 1);
    assert_counted(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// Three deliveries of provider errors followed by four of a refused completion fact: the
/// capture is analysed once and its completion fact is queued once, after eight deliveries. The
/// two faults are of different kinds, so each recovery path is exercised in one message's life.
#[tokio::test]
async fn a_provider_outage_then_a_database_outage_does_not_lose_a_capture() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let mut script: Vec<_> = (0..6).map(|_| Err(ProviderError::Transient)).collect();
    script.push(Ok(response(SOCIAL_ANALYSIS)));
    let worker = harness.spawn(Arc::new(ScriptedProvider::new(script)));
    refuse_writes(&harness, "event_outbox").await?;
    publish_capture(&harness).await?;

    redelivered_until(
        &harness,
        IngestFamily::Social,
        DELIVERIES_PAST_THE_OLD_BUDGET,
    )
    .await?;
    assert_eq!(
        harness.search_documents().await?,
        1,
        "the provider recovered and the capture was analysed"
    );
    assert_eq!(harness.rows_in(&["event_outbox"]).await?, 0);
    allow_writes(&harness, "event_outbox").await?;
    eventually("the completion fact is published once", || async {
        Ok(harness.facts_on(COMPLETED_SUBJECT).await?.len() == 1)
    })
    .await?;
    quiet(&harness, IngestFamily::Social).await?;
    assert_eq!(
        harness
            .rows_in(&["analysis_runs", "analysis_outputs"])
            .await?,
        2
    );
    assert_eq!(harness.rows_in(&["event_outbox"]).await?, 1);
    assert_counted(&harness).await?;
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A message that is not an envelope can never succeed: it is terminated at its first delivery
/// and nothing is counted as a redelivery.
#[tokio::test]
async fn an_undecodable_message_is_still_termed_at_its_first_delivery() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let worker = harness.spawn(routed(&[]));
    harness
        .publish(
            "evt.content.document.extracted.v1",
            EVENT_A,
            b"not an envelope".to_vec(),
        )
        .await?;

    harness.settled(IngestFamily::Documents, 1).await?;
    // A redelivery would arrive within the lowered schedule's cap; none may.
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.settled(IngestFamily::Documents, 1).await?;
    assert_eq!(operator_redeliveries(&harness).await?, 0);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A provider that fails permanently leaves the run failed for good: a redelivery would report
/// the same failure for ever, so the first delivery is terminated with a closed class.
#[tokio::test]
async fn a_permanent_provider_failure_is_rejected_at_the_first_delivery() -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let harness = Harness::start_fast().await?;
    let worker = harness.spawn(Arc::new(ScriptedProvider::new([Err(
        ProviderError::Permanent,
    )])));
    publish_capture(&harness).await?;

    harness.settled(IngestFamily::Social, 1).await?;
    // A redelivery would arrive within the lowered schedule's cap; none may.
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.settled(IngestFamily::Social, 1).await?;
    assert_eq!(logs.lines_containing("unanalysable"), 1);
    assert_eq!(operator_redeliveries(&harness).await?, 0);
    assert_eq!(harness.search_documents().await?, 0);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// The repository family keeps its bounded budget: its terminal delivery settles the failed fact
/// the requester waits for, so the fifth delivery is the last.
#[tokio::test]
async fn the_repository_family_still_settles_a_failed_fact_at_the_fifth_delivery() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let stub = HttpStub::start(vec![(
        format!("/internal/v1/readme-blobs/{}", readme_hex(README)),
        503,
        Vec::new(),
    )])
    .await?;
    let mut config = harness.config();
    config.ingest.github_readme_base_url = format!("http://{}/", stub.address());
    harness
        .publish(
            "evt.knowledge.repository_analysis.requested.v1",
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
    let worker = harness.spawn_with(
        config,
        routed(&[("repository_prompt_v1", REPOSITORY_ANALYSIS_PRESENT)]),
    );

    eventually("the failed fact is published", || async {
        Ok(harness.facts_on(FAILED_SUBJECT).await?.len() == 1)
    })
    .await?;
    harness.settled(IngestFamily::Repository, 5).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.settled(IngestFamily::Repository, 5).await?;
    assert_eq!(harness.facts_on(FAILED_SUBJECT).await?.len(), 1);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// R4-05 rule 3: from the second delivery on, a redelivery logs the family, the class and the
/// delivery count, and never content.
#[tokio::test]
async fn a_redelivery_from_the_second_on_logs_family_class_and_count_without_content() -> TestResult
{
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let harness = Harness::start_fast().await?;
    let worker = harness.spawn(routed(&[("social_prompt_v1", SOCIAL_ANALYSIS)]));
    refuse_writes(&harness, "source_analysis_inbox").await?;
    publish_capture(&harness).await?;

    redelivered_until(&harness, IngestFamily::Social, 4).await?;
    allow_writes(&harness, "source_analysis_inbox").await?;
    eventually("the capture becomes searchable", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    quiet(&harness, IngestFamily::Social).await?;

    let redeliveries: Vec<String> = logs
        .text()
        .lines()
        .filter(|line| line.contains("will be redelivered"))
        .map(str::to_owned)
        .collect();
    assert!(
        redeliveries.len() >= 3,
        "a line per failed delivery from the second on: {redeliveries:?}"
    );
    for count in 2..=4 {
        assert!(
            redeliveries.iter().any(|line| {
                line.contains("family=\"social\"")
                    && line.contains("class=\"social_inbox\"")
                    && line.contains(&format!("delivered={count}"))
            }),
            "delivery {count} is logged with its family and class: {redeliveries:?}"
        );
    }
    assert!(
        !redeliveries.iter().any(|line| line.contains("delivered=1")),
        "the first failure is not a redelivery"
    );
    assert_eq!(logs.lines_containing("A useful post"), 0, "no content");
    assert_eq!(logs.lines_containing(OWNER), 0, "no tenant");
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// A real adapter over a loopback transport that answers `status` to the first `rejected`
/// requests and then a chat completion carrying `content`: the failure is classified by
/// `classify_error`, not scripted.
async fn rejecting_provider(
    status: u16,
    rejected: usize,
    content: &str,
) -> Result<(FakeTransport, Arc<OpenRouterProvider>), Box<dyn std::error::Error>> {
    let success = serde_json::json!({
        "id": "gen-r5-operator-action",
        "choices": [{"message": {"role": "assistant", "content": content}}],
        "usage": {"prompt_tokens": 20, "completion_tokens": 10},
    });
    let mut replies: Vec<FakeReply> = (0..rejected)
        .map(|_| FakeReply::bytes(status, br#"{"error":{"message":"refused"}}"#.to_vec()))
        .collect();
    replies.push(FakeReply::bytes(200, serde_json::to_vec(&success)?));
    let transport = FakeTransport::start(replies).await?;
    let provider = OpenRouterProvider::new(OpenRouterSettings {
        base_url: format!("http://{}/api/v1", transport.local_addr()),
        model: "openai/gpt-oss-20b".to_owned(),
        credential: ProviderSecret::new("sk-or-v1-LEAKME".to_owned()),
        max_output_tokens: 2_048,
        response_byte_cap: 65_536,
        call_deadline: Duration::from_secs(2),
        connect_timeout: Duration::from_secs(2),
        retry: RetryPolicy::new(1, 0, 0),
    })?;
    Ok((transport, Arc::new(provider)))
}

/// One fact of `family` whose provider answers 401 for the first eight deliveries: it is applied
/// once the credential works, none of its deliveries is terminated, and the counter and the log
/// say so without content (XR-021 CONTRACTS.md R5-08 rule 1).
async fn survives_a_rejected_credential(
    family: IngestFamily,
    subject: &str,
    event_type: &str,
    producer: &str,
    payload: &serde_json::Value,
    answer: &str,
) -> TestResult {
    let _serial = BROKER.lock().await;
    let (logs, _guard) = capture();
    let harness = Harness::start_fast().await?;
    let (transport, provider) = rejecting_provider(
        401,
        usize::try_from(DELIVERIES_PAST_THE_OLD_BUDGET)?,
        answer,
    )
    .await?;
    let worker = harness.spawn(provider);
    harness
        .publish(
            subject,
            EVENT_A,
            envelope(EVENT_A, event_type, producer, Some(OWNER), payload),
        )
        .await?;

    redelivered_until(&harness, family, DELIVERIES_PAST_THE_OLD_BUDGET).await?;
    eventually("the fact is analysed once the credential works", || async {
        Ok(harness.search_documents().await? == 1)
    })
    .await?;
    quiet(&harness, family).await?;
    assert_eq!(
        transport.request_count()?,
        9,
        "eight rejections, one answer"
    );
    assert_eq!(
        harness
            .rows_in(&["analysis_runs", "analysis_outputs"])
            .await?,
        2
    );
    assert_counted(&harness).await?;
    let warnings: Vec<String> = logs
        .text()
        .lines()
        .filter(|line| line.contains("will be redelivered"))
        .map(str::to_owned)
        .collect();
    for delivered in 2..=DELIVERIES_PAST_THE_OLD_BUDGET {
        assert!(
            warnings.iter().any(|line| {
                line.contains(&format!("family=\"{}\"", family.label()))
                    && line.contains("class=\"provider_operator_action\"")
                    && line.contains(&format!("delivered={delivered}"))
            }),
            "delivery {delivered} is logged with its family and class: {warnings:?}"
        );
    }
    assert!(
        !warnings.iter().any(|line| line.contains("delivered=1")),
        "the first failure is not a redelivery"
    );
    assert_eq!(logs.lines_containing("sk-or-v1-LEAKME"), 0, "no credential");
    assert_eq!(logs.lines_containing("A useful post"), 0, "no content");
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_rejected_credential_does_not_lose_a_document() -> TestResult {
    survives_a_rejected_credential(
        IngestFamily::Documents,
        "evt.content.document.extracted.v1",
        "content.document.extracted.v1",
        "ratatoskr-extractor",
        &extracted_document()?,
        ARTICLE_ANALYSIS,
    )
    .await
}

#[tokio::test]
async fn a_rejected_credential_does_not_lose_a_social_capture() -> TestResult {
    survives_a_rejected_credential(
        IngestFamily::Social,
        "evt.social.source.captured.v1",
        CAPTURED,
        "ratatoskr-x",
        &social_payload("2026-08-17T10:00:00Z")?,
        SOCIAL_ANALYSIS,
    )
    .await
}

#[tokio::test]
async fn a_rejected_credential_does_not_lose_an_ai_conversation() -> TestResult {
    survives_a_rejected_credential(
        IngestFamily::AiArchive,
        "evt.ai_archive.conversation.added.v1",
        "ai_archive.conversation.added.v1",
        "ratatoskr-chatgpt",
        &serde_json::from_str(CONTRACT_CONVERSATION)?,
        ARCHIVE_ANALYSIS,
    )
    .await
}

/// A malformed request is the provider's verdict on the delivery itself: 400 is terminated at
/// the first delivery, as before.
#[tokio::test]
async fn a_provider_400_is_still_rejected_at_the_first_delivery() -> TestResult {
    let _serial = BROKER.lock().await;
    let harness = Harness::start_fast().await?;
    let (transport, provider) = rejecting_provider(400, 4, SOCIAL_ANALYSIS).await?;
    let worker = harness.spawn(provider);
    publish_capture(&harness).await?;

    harness.settled(IngestFamily::Social, 1).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.settled(IngestFamily::Social, 1).await?;
    assert_eq!(transport.request_count()?, 1);
    assert_eq!(operator_redeliveries(&harness).await?, 0);
    assert_eq!(harness.search_documents().await?, 0);
    worker.stop().await?;
    harness.database.cleanup().await?;
    Ok(())
}
