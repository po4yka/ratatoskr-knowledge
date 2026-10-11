//! Privacy deletion collects only the blobs of the rows it deletes (XR-021 CONTRACTS.md S12):
//! no other tenant's bytes, no unregistered in-flight bytes, and a durable queue that heals a
//! crash between the row commit and the byte collection.

use std::path::{Path, PathBuf};

use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType, TenantRef,
};
use ratatoskr_knowledge::test_support::recap::{
    FIXTURE_OWNER, SeededRecap, fixture_source, model_output, provider_response, seed_recap_run,
    seed_recap_run_for,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, ChannelRecapContextPolicy, ChannelRecapOutputLanguage, ChannelRecapPipeline,
    DeletionCounts, DeletionScope, ScriptedProvider, SourceReference,
    build_channel_recap_provider_request, collect_pending_blobs, delete_source, delete_tenant,
    execute_deletion, prepare_channel_recap_context, verify_scope_empty,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

const KNOWLEDGE: &str = "ratatoskr-knowledge";

fn blob_path(root: &Path, hex: &str) -> PathBuf {
    root.join("sha256")
        .join(hex.get(..2).unwrap_or(hex))
        .join(hex)
}

async fn exists(root: &Path, hex: &str) -> bool {
    tokio::fs::try_exists(blob_path(root, hex))
        .await
        .unwrap_or(false)
}

fn sha_hex(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn content_digest(hex: &str) -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(hex)?,
    })
}

/// Registers a source whose `source_blob` is `blob`.
async fn register(
    database: &TestDatabase,
    tenant: TenantRef,
    owner_context: &str,
    blob: BlobRef,
    marker: &str,
) -> Result<(Uuid, DocumentId), Box<dyn std::error::Error>> {
    let document_id = DocumentId::new_v7();
    let source = database
        .database
        .register_source(&SourceReference {
            tenant,
            owner_context: owner_context.to_owned(),
            ai_archive_id: String::new(),
            document_id,
            content_digest: content_digest(&sha_hex(marker))?,
            source_blob: blob,
        })
        .await?;
    Ok((source.id, document_id))
}

/// Adds a completed run with an invalid attempt and an accepted output that reference `blobs`.
async fn seed_responses(
    database: &TestDatabase,
    source_ref_id: Uuid,
    attempt: &BlobRef,
    outputs: &[&BlobRef],
) -> Result<(), Box<dyn std::error::Error>> {
    let run_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         ) values ($1, $2, 'article-analysis.v1', 'v1', 'v1', 'fake_default_v1', 'completed')",
    )
    .bind(run_id)
    .bind(source_ref_id)
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.analysis_attempts (
             run_id, ordinal, reason, provider, model_policy, model, raw_response, outcome,
             validation_code
         ) values ($1, 1, 'initial', 'openrouter', 'fake_default_v1', 'openai/gpt-oss-20b',
                   $2, 'invalid', 'schema_mismatch')",
    )
    .bind(run_id)
    .bind(serde_json::to_value(attempt)?)
    .execute(database.database.pool())
    .await?;
    for (position, output) in outputs.iter().enumerate() {
        sqlx::query(
            "insert into knowledge.analysis_outputs (output_id, run_id, result, raw_response, accepted)
             values ($1, $2, '{}', $3, $4)",
        )
        .bind(Uuid::now_v7())
        .bind(run_id)
        .bind(serde_json::to_value(output)?)
        .bind(position == 0)
        .execute(database.database.pool())
        .await?;
    }
    Ok(())
}

fn tenant(text: &str) -> Result<TenantRef, ratatoskr_identifiers::IdentifierError> {
    text.parse()
}

/// The sweep used to delete every file nothing in the attempt and output tables referenced, which
/// destroyed another tenant's recap raw response and Knowledge-owned source snapshot.
#[tokio::test]
async fn delete_tenant_of_an_unrelated_tenant_keeps_recap_and_snapshot_blobs()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);

    let snapshot = blobs.store_raw(b"tenant b snapshot bytes").await?;
    register(
        &database,
        tenant(FIXTURE_OWNER)?,
        KNOWLEDGE,
        snapshot.clone(),
        "tenant-b-snapshot",
    )
    .await?;
    let seeded = seed_recap_run(
        &database.database,
        vec![fixture_source(800, 1, "recap fixture")],
        "en",
    )
    .await?;
    let recap_raw = blobs.store_raw(b"tenant b recap raw response").await?;
    sqlx::query(
        "insert into knowledge.channel_recap_attempts
             (recap_run_id, ordinal, reason, provider, model, raw_response,
              raw_response_digest_hex, outcome, duration_ms)
         values ($1, 1, 'initial', 'scripted_fake', 'fake_default_v1', $2, $3, 'accepted', 5)",
    )
    .bind(seeded.run_id)
    .bind(serde_json::to_value(&recap_raw)?)
    .bind(recap_raw.digest.hex.as_str())
    .execute(database.database.pool())
    .await?;

    let receipt = delete_tenant(
        &database.database,
        &blobs,
        "user:018f0000-0000-7000-8000-0000000000aa",
    )
    .await?;

    assert!(
        exists(root.path(), snapshot.digest.hex.as_str()).await,
        "another tenant's source snapshot was deleted"
    );
    assert!(
        exists(root.path(), recap_raw.digest.hex.as_str()).await,
        "another tenant's recap raw response was deleted"
    );
    assert_eq!(blobs.read(&snapshot).await?, b"tenant b snapshot bytes");
    assert_eq!(
        blobs.read(&recap_raw).await?,
        b"tenant b recap raw response"
    );
    assert!(receipt.recovered_digests_removed.is_empty());
    assert!(receipt.blob_digests_removed.is_empty());
    assert_eq!(receipt.counts, DeletionCounts::default());
    database.cleanup().await?;
    Ok(())
}

/// Bytes are stored before their row is registered; no deletion may take them for garbage.
#[tokio::test]
async fn blob_stored_but_not_yet_registered_survives_any_deletion()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let in_flight = blobs.store_raw(b"stored, row not committed yet").await?;

    let receipt = delete_tenant(
        &database.database,
        &blobs,
        "user:018f0000-0000-7000-8000-0000000000aa",
    )
    .await?;

    assert!(exists(root.path(), in_flight.digest.hex.as_str()).await);
    assert!(receipt.recovered_digests_removed.is_empty());
    let drained = collect_pending_blobs(&database.database, &blobs).await?;
    assert!(drained.is_empty());
    assert!(exists(root.path(), in_flight.digest.hex.as_str()).await);
    database.cleanup().await?;
    Ok(())
}

/// A deletion that committed its rows and stopped before collecting its bytes is finished by the
/// next drain: exactly the digests no remaining row references are removed.
#[tokio::test]
async fn committed_deletion_whose_collection_crashed_is_healed_by_the_queue()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let owner = tenant("user:018f0000-0000-7000-8000-0000000000bb")?;
    let extractor_blob = |marker: &str| -> Result<BlobRef, Box<dyn std::error::Error>> {
        Ok(BlobRef {
            owner_service: BlobOwner::parse("ratatoskr-extractor")?,
            digest: content_digest(&sha_hex(marker))?,
            media_type: MediaType::parse("application/json")?,
            length_bytes: 128,
        })
    };

    let attempt = blobs.store_raw(b"deleted source attempt").await?;
    let output = blobs.store_raw(b"deleted source output").await?;
    let shared = blobs.store_raw(b"shared with a surviving source").await?;
    let (deleted_ref, deleted_document) = register(
        &database,
        owner,
        "ratatoskr-extractor",
        extractor_blob("deleted")?,
        "deleted",
    )
    .await?;
    seed_responses(&database, deleted_ref, &attempt, &[&output, &shared]).await?;
    let (survivor_ref, _) = register(
        &database,
        owner,
        "ratatoskr-extractor",
        extractor_blob("survivor")?,
        "survivor",
    )
    .await?;
    let survivor_attempt = blobs.store_raw(b"survivor attempt").await?;
    seed_responses(&database, survivor_ref, &survivor_attempt, &[&shared]).await?;

    let scope = DeletionScope::Source {
        tenant_ref: owner.to_string(),
        owner_context: "ratatoskr-extractor".to_owned(),
        source_document_id: deleted_document.to_string(),
    };
    let mut transaction = database.database.pool().begin().await?;
    execute_deletion(&mut transaction, &scope).await?;
    transaction.commit().await?;
    // The process stopped here: rows are gone, nothing was collected.

    let mut queued: Vec<String> =
        sqlx::query_scalar("select digest_hex from knowledge.blob_erasure_queue")
            .fetch_all(database.database.pool())
            .await?;
    queued.sort();
    let mut expected = vec![
        attempt.digest.hex.to_string(),
        output.digest.hex.to_string(),
        shared.digest.hex.to_string(),
    ];
    expected.sort();
    assert_eq!(queued, expected, "the deletion queues every owned digest");
    for reference in [&attempt, &output, &shared] {
        assert!(exists(root.path(), reference.digest.hex.as_str()).await);
    }

    let mut collected = collect_pending_blobs(&database.database, &blobs).await?;
    collected.sort();
    let mut private = vec![
        attempt.digest.hex.to_string(),
        output.digest.hex.to_string(),
    ];
    private.sort();
    assert_eq!(collected, private, "only unreferenced digests are removed");
    assert!(!exists(root.path(), attempt.digest.hex.as_str()).await);
    assert!(!exists(root.path(), output.digest.hex.as_str()).await);
    assert!(
        exists(root.path(), shared.digest.hex.as_str()).await,
        "a digest a surviving row references is dropped from the queue untouched"
    );
    assert!(exists(root.path(), survivor_attempt.digest.hex.as_str()).await);
    let remaining: i64 = sqlx::query_scalar("select count(*) from knowledge.blob_erasure_queue")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(remaining, 0);
    database.cleanup().await?;
    Ok(())
}

/// A Knowledge-owned source snapshot is collected with its source; a sibling's snapshot and an
/// externally owned blob are not.
#[tokio::test]
async fn delete_source_collects_the_knowledge_owned_snapshot_blob()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let owner = tenant("user:018f0000-0000-7000-8000-0000000000cc")?;

    let doomed = blobs.store_raw(b"snapshot of the deleted source").await?;
    let sibling = blobs.store_raw(b"snapshot of the sibling source").await?;
    let mut github = blobs
        .store_raw(b"bytes a github blob reference names")
        .await?;
    github.owner_service = BlobOwner::parse("ratatoskr-github")?;
    github.media_type = MediaType::parse("application/json")?;

    let (_, doomed_document) =
        register(&database, owner, KNOWLEDGE, doomed.clone(), "doomed").await?;
    register(&database, owner, KNOWLEDGE, sibling.clone(), "sibling").await?;
    let (_, external_document) = register(
        &database,
        owner,
        "ratatoskr-github",
        github.clone(),
        "external",
    )
    .await?;

    let receipt = delete_source(
        &database.database,
        &blobs,
        &owner.to_string(),
        KNOWLEDGE,
        &doomed_document.to_string(),
    )
    .await?;
    assert!(
        !exists(root.path(), doomed.digest.hex.as_str()).await,
        "the deleted source's own snapshot must be collected"
    );
    assert_eq!(
        receipt.blob_digests_removed,
        vec![doomed.digest.hex.to_string()]
    );
    assert!(
        exists(root.path(), sibling.digest.hex.as_str()).await,
        "a sibling source's snapshot must stay"
    );

    let external = delete_source(
        &database.database,
        &blobs,
        &owner.to_string(),
        "ratatoskr-github",
        &external_document.to_string(),
    )
    .await?;
    assert!(
        exists(root.path(), github.digest.hex.as_str()).await,
        "a blob owned by another service is never removed"
    );
    assert_eq!(
        external.external_source_blob_digests,
        vec![github.digest.hex.to_string()]
    );
    assert!(external.blob_digests_removed.is_empty());
    database.cleanup().await?;
    Ok(())
}

const RECAP_TABLES: [&str; 6] = [
    "channel_recap_outbox",
    "channel_recap_results",
    "channel_recap_attempts",
    "channel_recap_manifests",
    "channel_recap_runs",
    "channel_recap_inbox",
];

/// Drives one recap through the real pipeline: inbox, run, manifest, attempt with a raw-response
/// blob, result and outbox row for `owner`.
async fn complete_recap(
    database: &TestDatabase,
    blobs: &BlobStore,
    owner: &str,
    variant: u32,
) -> Result<SeededRecap, Box<dyn std::error::Error>> {
    let seeded = seed_recap_run_for(
        &database.database,
        owner,
        vec![fixture_source(810, 1, "shared recap fixture")],
        "en",
        variant,
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
    let provider = ScriptedProvider::new([Ok(provider_response(
        &model_output(&context, &[]),
        "deletion-fixture",
    )?)]);
    ChannelRecapPipeline::new(
        &database.database,
        &provider,
        blobs,
        std::time::Duration::from_secs(30),
    )
    .execute(
        seeded.run_id,
        request,
        &context,
        &seeded.verified.digest_hex,
        ChannelRecapOutputLanguage::En,
    )
    .await?;
    Ok(seeded)
}

/// Row counts of the six recap tables for one owner, in [`RECAP_TABLES`] order.
async fn recap_rows(
    database: &TestDatabase,
    owner: &str,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let mut counts = Vec::new();
    for table in RECAP_TABLES {
        let sql = match table {
            "channel_recap_inbox" | "channel_recap_runs" | "channel_recap_manifests" => {
                format!("select count(*) from knowledge.{table} where owner_ref = $1")
            }
            _ => format!(
                "select count(*) from knowledge.{table} t
                 join knowledge.channel_recap_runs r on r.recap_run_id = t.recap_run_id
                 where r.owner_ref = $1"
            ),
        };
        counts.push(
            sqlx::query_scalar(&sql)
                .bind(owner)
                .fetch_one(database.database.pool())
                .await?,
        );
    }
    Ok(counts)
}

/// XR-021 CONTRACTS.md R2-06: tenant deletion removes the channel recap tables of that tenant,
/// queues its raw-response digests, and leaves every other tenant and every other scope alone.
#[tokio::test]
async fn delete_tenant_removes_channel_recap_state_of_that_tenant_only()
-> Result<(), Box<dyn std::error::Error>> {
    const TENANT_A: &str = "user:018f0000-0000-7000-8000-0000000000a1";
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);

    // Both tenants' recaps produced the same model bytes, so one raw-response digest is shared.
    let run_a = complete_recap(&database, &blobs, TENANT_A, 1).await?;
    complete_recap(&database, &blobs, FIXTURE_OWNER, 2).await?;
    let private = blobs.store_raw(b"tenant a repair response").await?;
    sqlx::query(
        "insert into knowledge.channel_recap_attempts
             (recap_run_id, ordinal, reason, provider, model, raw_response,
              raw_response_digest_hex, outcome, validation_code, duration_ms)
         values ($1, 2, 'repair', 'scripted_fake', 'fake_default_v1', $2, $3, 'invalid',
                 'schema', 5)",
    )
    .bind(run_a.run_id)
    .bind(serde_json::to_value(&private)?)
    .bind(private.digest.hex.as_str())
    .execute(database.database.pool())
    .await?;
    let shared: String = sqlx::query_scalar(
        "select raw_response -> 'digest' ->> 'hex' from knowledge.channel_recap_attempts
         where recap_run_id = $1 and ordinal = 1",
    )
    .bind(run_a.run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(recap_rows(&database, TENANT_A).await?, [1, 1, 2, 1, 1, 1]);
    assert_eq!(
        recap_rows(&database, FIXTURE_OWNER).await?,
        [1, 1, 1, 1, 1, 1]
    );

    // Source and archive scope never touch a recap, which is not tied to one source document.
    delete_source(
        &database.database,
        &blobs,
        FIXTURE_OWNER,
        "ratatoskr-extractor",
        "018f0000-0000-7000-8000-0000000000ff",
    )
    .await?;
    let mut transaction = database.database.pool().begin().await?;
    let archive = execute_deletion(
        &mut transaction,
        &DeletionScope::Archive {
            tenant_ref: TENANT_A.to_owned(),
            ai_archive_id: "archive-without-sources".to_owned(),
        },
    )
    .await?;
    transaction.commit().await?;
    assert_eq!(archive.channel_recap_rows, 0);
    assert_eq!(recap_rows(&database, TENANT_A).await?, [1, 1, 2, 1, 1, 1]);
    assert_eq!(
        recap_rows(&database, FIXTURE_OWNER).await?,
        [1, 1, 1, 1, 1, 1]
    );

    let receipt = delete_tenant(&database.database, &blobs, TENANT_A).await?;

    assert_eq!(receipt.counts.channel_recap_rows, 7);
    assert_eq!(recap_rows(&database, TENANT_A).await?, [0; 6]);
    assert_eq!(
        recap_rows(&database, FIXTURE_OWNER).await?,
        [1, 1, 1, 1, 1, 1],
        "the other tenant's recap survives"
    );
    assert_eq!(
        receipt.blob_digests_removed,
        [private.digest.hex.to_string()],
        "only the digest no remaining row references is erased"
    );
    assert!(!exists(root.path(), private.digest.hex.as_str()).await);
    assert!(
        exists(root.path(), &shared).await,
        "the digest the other tenant still references survives"
    );
    verify_scope_empty(
        &database.database,
        &DeletionScope::Tenant {
            tenant_ref: TENANT_A.to_owned(),
        },
    )
    .await?;
    let audited: i32 = sqlx::query_scalar(
        "select channel_recap_rows_deleted from knowledge.deletion_records
         where tenant_ref = $1 and scope = 'tenant'",
    )
    .bind(TENANT_A)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(audited, 7, "the audit row carries the recap count");
    database.cleanup().await?;
    Ok(())
}
