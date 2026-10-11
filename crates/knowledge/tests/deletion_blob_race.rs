//! XR-021 CONTRACTS.md R2-07: blob registration and blob erasure follow one digest-lock protocol,
//! so no committed row references a missing blob.
//!
//! Each case drives the real eraser (`collect_pending_blobs`) and the real registrar (the recap
//! pipeline storing a raw response) on separate connections. The interleaving is forced from the
//! database side: the test holds the digest advisory lock itself so both actors queue behind it in
//! a known order, and for the eraser-wins cases a trigger on the erasure queue parks the eraser,
//! still holding the lock, after it removed the bytes.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ratatoskr_knowledge::test_support::recap::{
    fixture_source, model_output, provider_response, seed_recap_run,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, ChannelDigestRecap, ChannelRecapContextPolicy, ChannelRecapOutputLanguage,
    ChannelRecapPipeline, ChannelRecapPipelineError, Database, ScriptedProvider,
    build_channel_recap_provider_request, collect_pending_blobs, prepare_channel_recap_context,
};
use sha2::{Digest as _, Sha256};
use sqlx::{Postgres, Transaction};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The gate the parked eraser waits on; any key distinct from the digest keys.
const GATE_KEY: i64 = 424_242;

struct Race {
    database: TestDatabase,
    /// A pool large enough for the two actors, the session lock of the run and both holders.
    pool: Database,
    root: TemporaryBlobRoot,
    blobs: BlobStore,
    run_id: uuid::Uuid,
    digest: String,
    bytes: Vec<u8>,
    request: ratatoskr_knowledge::ChannelRecapProviderRequest,
    context: ratatoskr_knowledge::PreparedChannelRecapContext,
    manifest_digest: String,
}

impl Race {
    /// A recap run ready for its provider call, and a queued erasure of the digest that call will
    /// produce, whose bytes are still on disk.
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let database = TestDatabase::create().await?;
        let name: String = sqlx::query_scalar("select current_database()")
            .fetch_one(database.database.pool())
            .await?;
        let admin = std::env::var("KNOWLEDGE_TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgres://extractor:extractor@127.0.0.1:5434/extractor".to_owned()
        });
        let (server, _) = admin.rsplit_once('/').ok_or("invalid test database URL")?;
        let pool =
            Database::connect(&format!("{server}/{name}"), 12, Duration::from_secs(10)).await?;

        let seeded = seed_recap_run(
            &database.database,
            vec![fixture_source(820, 1, "blob race fixture")],
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
        let request =
            build_channel_recap_provider_request(&context, ChannelRecapOutputLanguage::En, 2_048)?;
        let bytes = provider_response(&model_output(&context, &[]), "race")?.bytes;
        let digest = format!("{:x}", Sha256::digest(&bytes));

        let root = TemporaryBlobRoot::create().await?;
        let blobs = BlobStore::new(root.path(), 65_536);
        blobs.store_raw(&bytes).await?;
        sqlx::query(
            "insert into knowledge.blob_erasure_queue (digest_hex, deletion_id)
             values ($1, gen_random_uuid())",
        )
        .bind(&digest)
        .execute(database.database.pool())
        .await?;
        Ok(Self {
            database,
            pool,
            root,
            blobs,
            run_id: seeded.run_id,
            digest,
            bytes,
            request,
            context,
            manifest_digest: seeded.verified.digest_hex,
        })
    }

    fn blob_path(&self) -> PathBuf {
        self.root
            .path()
            .join("sha256")
            .join(self.digest.get(..2).unwrap_or_default())
            .join(&self.digest)
    }

    /// Takes the digest lock exactly as the protocol names it and keeps it until commit.
    async fn hold_digest_lock(&self) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
        let mut transaction = self.pool.pool().begin().await?;
        sqlx::query("select pg_advisory_xact_lock(hashtextextended('blob:' || $1, 0))")
            .bind(&self.digest)
            .execute(&mut *transaction)
            .await?;
        Ok(transaction)
    }

    /// Parks every delete from the erasure queue behind [`GATE_KEY`], and holds that key.
    async fn close_gate(&self) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
        sqlx::raw_sql(&format!(
            "create function knowledge.park_queue_delete() returns trigger language plpgsql as
             $$ begin perform pg_advisory_xact_lock({GATE_KEY}); return old; end $$;
             create trigger park_queue_delete before delete on knowledge.blob_erasure_queue
             for each row execute function knowledge.park_queue_delete();"
        ))
        .execute(self.pool.pool())
        .await?;
        let mut gate = self.pool.pool().begin().await?;
        sqlx::query("select pg_advisory_xact_lock($1)")
            .bind(GATE_KEY)
            .execute(&mut *gate)
            .await?;
        Ok(gate)
    }

    /// Waits until `count` sessions wait for this race's digest lock.
    async fn wait_for_waiters(&self, count: i64, what: &str) -> TestResult {
        let started = Instant::now();
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "select count(*) from pg_locks
                 where locktype = 'advisory' and not granted
                   and database = (select oid from pg_database where datname = current_database())
                   and ((classid::bigint << 32) | objid::bigint)
                       = hashtextextended('blob:' || $1, 0)",
            )
            .bind(&self.digest)
            .fetch_one(self.pool.pool())
            .await?;
            if waiting >= count {
                return Ok(());
            }
            if started.elapsed() > Duration::from_secs(10) {
                return Err(
                    format!("{what}: {waiting} of {count} waiters after 10 seconds").into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_blob(&self, present: bool) -> TestResult {
        let started = Instant::now();
        while tokio::fs::try_exists(self.blob_path()).await? != present {
            if started.elapsed() > Duration::from_secs(10) {
                return Err(format!("the blob did not become present={present}").into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    fn spawn_eraser(&self) -> tokio::task::JoinHandle<Result<Vec<String>, String>> {
        let pool = self.pool.clone();
        let blobs = self.blobs.clone();
        tokio::spawn(async move {
            collect_pending_blobs(&pool, &blobs)
                .await
                .map_err(|error| error.to_string())
        })
    }

    /// The registrar: the recap pipeline's provider call stores the raw response and registers it
    /// on the run's attempt row.
    fn spawn_registrar(
        &self,
    ) -> tokio::task::JoinHandle<Result<ChannelDigestRecap, ChannelRecapPipelineError>> {
        let pool = self.pool.clone();
        let blobs = self.blobs.clone();
        let provider = Arc::new(ScriptedProvider::new([Ok(
            ratatoskr_knowledge::ProviderResponse {
                bytes: self.bytes.clone(),
                request_id: Some("race".to_owned()),
                usage: ratatoskr_knowledge::ProviderUsage {
                    input_tokens: 30,
                    output_tokens: 10,
                },
            },
        )]));
        let (run_id, request, context, manifest_digest) = (
            self.run_id,
            self.request.clone(),
            self.context.clone(),
            self.manifest_digest.clone(),
        );
        tokio::spawn(async move {
            ChannelRecapPipeline::new(&pool, provider.as_ref(), &blobs, Duration::from_secs(30))
                .execute(
                    run_id,
                    request,
                    &context,
                    &manifest_digest,
                    ChannelRecapOutputLanguage::En,
                )
                .await
        })
    }

    /// Digests that attempt rows of the run reference.
    async fn referenced(&self) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar(
            "select raw_response -> 'digest' ->> 'hex' from knowledge.channel_recap_attempts
             where raw_response is not null",
        )
        .fetch_all(self.database.database.pool())
        .await
    }

    async fn queued(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("select count(*) from knowledge.blob_erasure_queue")
            .fetch_one(self.database.database.pool())
            .await
    }

    async fn finish(self) -> TestResult {
        self.pool.close().await;
        self.database.cleanup().await?;
        Ok(())
    }
}

fn restore(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    let _ignored = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755));
}

/// (a) The eraser wins: it removed the bytes and is parked holding the lock when the registrar,
/// which already saw the bytes present, arrives. The registrar waits, finds the blob gone and
/// stores it again, so the committed row references an existing blob.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registrar_restores_a_blob_the_eraser_removed_first() -> TestResult {
    let race = Race::start().await?;
    let digest_lock = race.hold_digest_lock().await?;
    let gate = race.close_gate().await?;

    let erasing = race.spawn_eraser();
    race.wait_for_waiters(1, "the eraser must wait for the digest lock")
        .await?;
    let registrar = race.spawn_registrar();
    race.wait_for_waiters(2, "the registrar must wait for the digest lock")
        .await?;

    digest_lock.commit().await?;
    race.wait_for_blob(false).await?;
    gate.commit().await?;

    let removed = erasing.await??;
    let recap = registrar.await?;
    assert_eq!(
        removed,
        std::slice::from_ref(&race.digest),
        "the eraser won the race"
    );
    recap?;
    assert_eq!(race.referenced().await?, std::slice::from_ref(&race.digest));
    assert!(
        tokio::fs::try_exists(race.blob_path()).await?,
        "the committed row must reference an existing blob"
    );
    assert_eq!(race.queued().await?, 0);
    race.finish().await
}

/// (b) The registrar wins: it commits its reference first, so the eraser re-checks under the lock,
/// keeps the blob and only removes the queue row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eraser_keeps_a_blob_the_registrar_committed_first() -> TestResult {
    let race = Race::start().await?;
    let digest_lock = race.hold_digest_lock().await?;

    let registrar = race.spawn_registrar();
    race.wait_for_waiters(1, "the registrar must wait for the digest lock")
        .await?;
    let erasing = race.spawn_eraser();
    race.wait_for_waiters(2, "the eraser must wait for the digest lock")
        .await?;

    digest_lock.commit().await?;
    registrar.await??;
    let removed = erasing.await??;

    assert!(
        removed.is_empty(),
        "a referenced digest is never erased: {removed:?}"
    );
    assert_eq!(race.referenced().await?, std::slice::from_ref(&race.digest));
    assert!(tokio::fs::try_exists(race.blob_path()).await?);
    assert_eq!(race.queued().await?, 0, "the queue row is dropped");
    race.finish().await
}

/// (c) The eraser wins and the registrar cannot store the bytes again: it fails and registers
/// nothing, so no row references a missing blob.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registrar_that_cannot_restore_registers_nothing() -> TestResult {
    let race = Race::start().await?;
    let digest_lock = race.hold_digest_lock().await?;
    let gate = race.close_gate().await?;

    let erasing = race.spawn_eraser();
    race.wait_for_waiters(1, "the eraser must wait for the digest lock")
        .await?;
    let registrar = race.spawn_registrar();
    race.wait_for_waiters(2, "the registrar must wait for the digest lock")
        .await?;

    digest_lock.commit().await?;
    race.wait_for_blob(false).await?;
    // The eraser is parked after removing the bytes: make storing them again impossible.
    let directory = race
        .blob_path()
        .parent()
        .map(Path::to_path_buf)
        .ok_or("the blob has a directory")?;
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o555))?;
    }
    gate.commit().await?;

    let removed = erasing.await??;
    let outcome = registrar.await?;
    restore(&directory);
    assert_eq!(removed, std::slice::from_ref(&race.digest));
    assert!(
        outcome.is_err(),
        "the registrar must fail when it cannot restore the blob"
    );
    assert!(
        race.referenced().await?.is_empty(),
        "nothing may reference the missing blob"
    );
    assert!(!tokio::fs::try_exists(race.blob_path()).await?);
    race.finish().await
}
