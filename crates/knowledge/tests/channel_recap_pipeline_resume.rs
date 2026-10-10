//! Crash-resumable channel recap execution (XR-021 CONTRACTS.md S08): a redelivery after the
//! process died at any durable state converges, and a second executor never touches a run that
//! another one holds.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use ratatoskr_knowledge::test_support::recap::{
    SeededRecap, fixture_source, model_output, provider_response, seed_recap_run,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, ChannelDigestRecap, ChannelRecapContextPolicy, ChannelRecapOutputLanguage,
    ChannelRecapPipeline, ChannelRecapPipelineError, Database, GenerationRequest, LlmProvider,
    PreparedChannelRecapContext, ProviderError, ProviderFailure, ProviderIdentity,
    ProviderResponse, ScriptedProvider, build_channel_recap_provider_request,
    prepare_channel_recap_context,
};

/// A provider whose scripted calls return and whose later calls never complete, like a call in
/// flight when the process is killed.
#[derive(Debug)]
struct StallingProvider {
    script: Mutex<VecDeque<Result<ProviderResponse, ProviderError>>>,
}

impl StallingProvider {
    fn new(script: impl IntoIterator<Item = Result<ProviderResponse, ProviderError>>) -> Self {
        Self {
            script: Mutex::new(script.into_iter().collect()),
        }
    }
}

impl LlmProvider for StallingProvider {
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
        let next = self
            .script
            .lock()
            .ok()
            .and_then(|mut script| script.pop_front());
        async move {
            match next {
                Some(outcome) => outcome.map_err(ProviderFailure::from),
                None => std::future::pending().await,
            }
        }
    }
}

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Harness {
    database: TestDatabase,
    blobs: BlobStore,
    _root: TemporaryBlobRoot,
    seeded: SeededRecap,
    context: PreparedChannelRecapContext,
}

impl Harness {
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let database = TestDatabase::create().await?;
        let seeded = seed_recap_run(
            &database.database,
            vec![fixture_source(700, 1, "resume fixture source")],
            "en",
        )
        .await?;
        let context = prepare_channel_recap_context(
            &seeded.verified,
            ChannelRecapContextPolicy {
                max_sources: 100,
                max_channels: 20,
                max_characters: 100_000,
            },
        )?;
        let root = TemporaryBlobRoot::create().await?;
        let blobs = BlobStore::new(root.path(), 65_536);
        Ok(Self {
            database,
            blobs,
            _root: root,
            seeded,
            context,
        })
    }

    fn db(&self) -> &Database {
        &self.database.database
    }

    fn pipeline<'a, P: LlmProvider>(&'a self, provider: &'a P) -> ChannelRecapPipeline<'a, P> {
        ChannelRecapPipeline::new(self.db(), provider, &self.blobs, Duration::from_secs(30))
    }

    /// Runs one execution attempt, retrying only while a dying executor's session lock is still
    /// being released by the server.
    async fn execute<P: LlmProvider>(
        &self,
        provider: &P,
    ) -> Result<ChannelDigestRecap, ChannelRecapPipelineError> {
        for _ in 0..80 {
            let request = build_channel_recap_provider_request(
                &self.context,
                ChannelRecapOutputLanguage::En,
                2_048,
            )
            .map_err(|_| ChannelRecapPipelineError::Invalid)?;
            match self
                .pipeline(provider)
                .execute(
                    self.seeded.run_id,
                    request,
                    &self.context,
                    &self.seeded.verified.digest_hex,
                    ChannelRecapOutputLanguage::En,
                )
                .await
            {
                Err(ChannelRecapPipelineError::State) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                other => return other,
            }
        }
        Err(ChannelRecapPipelineError::State)
    }

    /// Starts an execution against `provider` and abandons it, like a kill, as soon as `attempts`
    /// attempt rows exist, which is while its last provider call is in flight.
    async fn kill_at_attempt<P: LlmProvider>(
        &self,
        provider: &P,
        attempts: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let request = build_channel_recap_provider_request(
            &self.context,
            ChannelRecapOutputLanguage::En,
            2_048,
        )?;
        let pipeline = self.pipeline(provider);
        let execution = pipeline.execute(
            self.seeded.run_id,
            request,
            &self.context,
            &self.seeded.verified.digest_hex,
            ChannelRecapOutputLanguage::En,
        );
        tokio::pin!(execution);
        let reached = async {
            for _ in 0..250 {
                if self.attempts().await?.len() >= attempts {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err::<(), Box<dyn std::error::Error>>(
                "the execution never reached its provider call".into(),
            )
        };
        tokio::select! {
            outcome = &mut execution => {
                Err(format!("the stalled execution finished unexpectedly: {outcome:?}").into())
            }
            reached = reached => reached,
        }
    }

    async fn run_state(&self) -> Result<(String, i16), sqlx::Error> {
        sqlx::query_as(
            "select state, attempt_count from knowledge.channel_recap_runs where recap_run_id = $1",
        )
        .bind(self.seeded.run_id)
        .fetch_one(self.db().pool())
        .await
    }

    async fn attempts(&self) -> Result<Vec<(i16, String, String)>, sqlx::Error> {
        sqlx::query_as(
            "select ordinal, reason, outcome from knowledge.channel_recap_attempts
             where recap_run_id = $1 order by ordinal",
        )
        .bind(self.seeded.run_id)
        .fetch_all(self.db().pool())
        .await
    }

    fn valid_response(&self, label: &str) -> Result<ProviderResponse, serde_json::Error> {
        provider_response(&model_output(&self.context, &[]), label)
    }

    /// Seeds the durable state a killed process leaves after storing a raw response: the attempt
    /// holds the stored bytes and the run sits in `state`.
    async fn seed_stored_response(
        &self,
        raw: &[u8],
        state: &str,
        attempt_outcome: &str,
        validation_code: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stored = self.blobs.store_raw(raw).await?;
        let digest = stored.digest.hex.to_string();
        sqlx::query(
            "update knowledge.channel_recap_runs
             set state = 'model_requested', attempt_count = 1 where recap_run_id = $1",
        )
        .bind(self.seeded.run_id)
        .execute(self.db().pool())
        .await?;
        sqlx::query(
            "insert into knowledge.channel_recap_attempts
                 (recap_run_id, ordinal, reason, provider, model, raw_response,
                  raw_response_digest_hex, outcome, validation_code, duration_ms)
             values ($1, 1, 'initial', 'scripted_fake', 'fake_default_v1', $2, $3, $4, $5, 5)",
        )
        .bind(self.seeded.run_id)
        .bind(serde_json::to_value(&stored)?)
        .bind(digest)
        .bind(attempt_outcome)
        .bind(validation_code)
        .execute(self.db().pool())
        .await?;
        sqlx::query("update knowledge.channel_recap_runs set state = $2 where recap_run_id = $1")
            .bind(self.seeded.run_id)
            .bind(state)
            .execute(self.db().pool())
            .await?;
        Ok(())
    }

    async fn assert_completed(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (state, _) = self.run_state().await?;
        assert_eq!(state, "completed");
        let results: i64 = sqlx::query_scalar(
            "select count(*) from knowledge.channel_recap_results where recap_run_id = $1",
        )
        .bind(self.seeded.run_id)
        .fetch_one(self.db().pool())
        .await?;
        let outbox: i64 = sqlx::query_scalar(
            "select count(*) from knowledge.channel_recap_outbox where recap_run_id = $1",
        )
        .bind(self.seeded.run_id)
        .fetch_one(self.db().pool())
        .await?;
        assert_eq!((results, outbox), (1, 1));
        Ok(())
    }
}

#[tokio::test]
async fn resumes_after_kill_in_model_requested() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::start().await?;
    let stalled = StallingProvider::new([]);
    harness.kill_at_attempt(&stalled, 1).await?;
    assert_eq!(
        harness.run_state().await?,
        ("model_requested".to_owned(), 1)
    );
    assert_eq!(
        harness.attempts().await?,
        vec![(1, "initial".to_owned(), "requested".to_owned())]
    );

    let healthy = ScriptedProvider::new([Ok(harness.valid_response("scripted-resume")?)]);
    harness.execute(&healthy).await?;

    assert_eq!(healthy.call_count()?, 1, "exactly one new provider call");
    assert_eq!(
        harness.attempts().await?,
        vec![
            (1, "initial".to_owned(), "interrupted".to_owned()),
            (2, "retry".to_owned(), "accepted".to_owned()),
        ]
    );
    harness.assert_completed().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn resumes_from_response_received_without_a_provider_call()
-> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::start().await?;
    let raw = serde_json::to_vec(&model_output(&harness.context, &[]))?;
    harness
        .seed_stored_response(&raw, "response_received", "response_received", None)
        .await?;

    let provider = ScriptedProvider::new([]);
    harness.execute(&provider).await?;

    assert_eq!(provider.call_count()?, 0, "the stored response is reused");
    assert_eq!(
        harness.attempts().await?,
        vec![(1, "initial".to_owned(), "accepted".to_owned())]
    );
    harness.assert_completed().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn resumes_from_schema_validated() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::start().await?;
    let raw = serde_json::to_vec(&model_output(&harness.context, &[]))?;
    harness
        .seed_stored_response(&raw, "schema_validated", "response_received", None)
        .await?;

    let provider = ScriptedProvider::new([]);
    harness.execute(&provider).await?;

    assert_eq!(provider.call_count()?, 0);
    assert_eq!(
        harness.attempts().await?,
        vec![(1, "initial".to_owned(), "accepted".to_owned())]
    );
    harness.assert_completed().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn resumes_from_repaired() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::start().await?;
    harness
        .seed_stored_response(br#"{"headline":""}"#, "repaired", "invalid", Some("schema"))
        .await?;

    let provider = ScriptedProvider::new([Ok(harness.valid_response("scripted-repair")?)]);
    harness.execute(&provider).await?;

    assert_eq!(provider.call_count()?, 1);
    let requests = provider.requests()?;
    let sent = requests.first().ok_or("the repair call was not made")?;
    assert!(
        sent.task_instruction
            .contains("Repair validation code: channel_recap_schema"),
        "the repair call carries the repair instruction"
    );
    assert_eq!(
        harness.attempts().await?,
        vec![
            (1, "initial".to_owned(), "invalid".to_owned()),
            (2, "repair".to_owned(), "accepted".to_owned()),
        ]
    );
    harness.assert_completed().await?;
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn kill_in_model_requested_on_the_second_attempt_settles_provider_unavailable()
-> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::start().await?;
    let stalled_second = StallingProvider::new([Err(ProviderError::Transient)]);
    harness.kill_at_attempt(&stalled_second, 2).await?;
    assert_eq!(
        harness.run_state().await?,
        ("model_requested".to_owned(), 2)
    );

    let healthy = ScriptedProvider::new([Ok(harness.valid_response("never-called")?)]);
    let outcome = harness.execute(&healthy).await;

    assert!(
        outcome.is_err(),
        "the provider budget of two calls was already spent"
    );
    assert_eq!(healthy.call_count()?, 0, "no third provider call is made");
    assert_eq!(harness.run_state().await?.0, "failed");
    let failure: String = sqlx::query_scalar(
        "select payload ->> 'failure_code' from knowledge.channel_recap_outbox
         where recap_run_id = $1",
    )
    .bind(harness.seeded.run_id)
    .fetch_one(harness.db().pool())
    .await?;
    assert_eq!(failure, "provider_unavailable");
    let outcomes: Vec<String> = harness
        .attempts()
        .await?
        .into_iter()
        .map(|(_, _, outcome)| outcome)
        .collect();
    assert_eq!(outcomes, ["transient_failure", "interrupted"]);
    harness.database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_second_executor_gets_state_and_does_not_touch_the_attempt_row()
-> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::start().await?;
    exercise_concurrent_executors(&harness).await?;
    harness.database.cleanup().await?;
    Ok(())
}

/// Runs a stalled first executor and a second one against the same run.
async fn exercise_concurrent_executors(harness: &Harness) -> TestResult {
    let stalled = StallingProvider::new([]);
    let idle = ScriptedProvider::new([]);
    let first_request = build_channel_recap_provider_request(
        &harness.context,
        ChannelRecapOutputLanguage::En,
        2_048,
    )?;
    let first_pipeline = harness.pipeline(&stalled);
    let first = first_pipeline.execute(
        harness.seeded.run_id,
        first_request,
        &harness.context,
        &harness.seeded.verified.digest_hex,
        ChannelRecapOutputLanguage::En,
    );
    tokio::pin!(first);

    let second = async {
        // Wait until the first executor has reached its provider call.
        for _ in 0..100 {
            if harness.attempts().await?.len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let before = harness.attempts().await?;
        assert_eq!(
            before.len(),
            1,
            "the first executor never reached the provider"
        );
        let second_request = build_channel_recap_provider_request(
            &harness.context,
            ChannelRecapOutputLanguage::En,
            2_048,
        )?;
        let second_pipeline = harness.pipeline(&idle);
        let outcome = second_pipeline
            .execute(
                harness.seeded.run_id,
                second_request,
                &harness.context,
                &harness.seeded.verified.digest_hex,
                ChannelRecapOutputLanguage::En,
            )
            .await;
        assert!(
            matches!(outcome, Err(ChannelRecapPipelineError::State)),
            "a run held by another executor is a State conflict, got {outcome:?}"
        );
        assert_eq!(idle.call_count()?, 0);
        assert_eq!(harness.attempts().await?, before);
        assert_eq!(
            harness.run_state().await?,
            ("model_requested".to_owned(), 1)
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    };

    tokio::select! {
        outcome = &mut first => {
            return Err(format!("the stalled executor finished unexpectedly: {outcome:?}").into());
        }
        checked = second => checked?,
    }
    Ok(())
}
