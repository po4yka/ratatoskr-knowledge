//! Privacy deletion removes the content-bearing inbox, tombstone, request and outbox state of its
//! scope and keeps the ordering guards (XR-021 CONTRACTS.md S12).

use ratatoskr_ai_archive_contracts::AiArchiveTombstone;
use ratatoskr_github_contracts::{
    ReadmeAbsenceReason, ReadmeRevision, RepositoryAnalysisAttributes, RepositoryAnalysisContract,
    RepositoryAnalysisRequested, RepositoryAnalysisRevision, RepositoryFullName,
};
use ratatoskr_identifiers::{
    ContentDigest, DigestAlgorithm, DigestHex, EntityRef, Extensions, RepositoryAnalysisRequestId,
    RepositoryId, TenantRef,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, DeletionCounts, DeletionError, DeletionScope, EventCause,
    RepositoryAnalysisConsumer, SourceInbox, SourceInboxAdmission, delete_source, delete_tenant,
    execute_deletion, verify_scope_empty,
};
use uuid::Uuid;

const T: &str = "user:018f0000-0000-7000-8000-0000000000d1";
const U: &str = "user:018f0000-0000-7000-8000-0000000000d2";
const KNOWLEDGE: &str = "ratatoskr-knowledge";
const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const TOMBSTONED: &str = "ai_archive.subject.tombstoned.v1";

fn uuid(tail: u32) -> Uuid {
    Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 + u128::from(tail))
}

/// A claimed delivery, optionally the head of its source.
async fn seed_inbox(
    database: &TestDatabase,
    tenant: &str,
    family: &str,
    archive_id: Option<&str>,
    source_id: &str,
    subject: &str,
    head: bool,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    let event_id = Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.source_analysis_inbox
             (event_id, subject, family, tenant_ref, archive_id, source_id, content_digest_hex,
              observed_at, snapshot)
         values ($1, $2, $3, $4, $5, $6, $7, '2026-08-17T10:00:00Z',
                 '{\"text\":\"private user text\"}'::jsonb)",
    )
    .bind(event_id)
    .bind(subject)
    .bind(family)
    .bind(tenant)
    .bind(archive_id)
    .bind(source_id)
    .bind(DIGEST)
    .execute(database.database.pool())
    .await?;
    if head {
        sqlx::query(
            "insert into knowledge.source_analysis_heads
                 (family, tenant_ref, source_id, content_digest_hex, observed_at, inbox_event_id)
             values ($1, $2, $3, $4, '2026-08-17T10:00:00Z', $5)",
        )
        .bind(family)
        .bind(tenant)
        .bind(source_id)
        .bind(DIGEST)
        .bind(event_id)
        .execute(database.database.pool())
        .await?;
    }
    Ok(event_id)
}

async fn seed_object(
    database: &TestDatabase,
    tenant: &str,
    archive_id: &str,
    object_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into knowledge.ai_archive_object_inbox
             (event_id, subject, tenant_ref, archive_id, object_kind, object_id, observed_at,
              payload)
         values ($1, 'ai_archive.project.added.v1', $2, $3, 'project', $4,
                 '2026-08-17T10:00:00Z', '{\"title\":\"private\"}'::jsonb)",
    )
    .bind(Uuid::now_v7())
    .bind(tenant)
    .bind(archive_id)
    .bind(object_id)
    .execute(database.database.pool())
    .await
    .map(|_| ())
}

async fn seed_ai_tombstone(
    database: &TestDatabase,
    tenant: &str,
    archive_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into knowledge.ai_archive_tombstones
             (event_id, tenant_ref, archive_id, subject_kind, subject_id, observed_at)
         values ($1, $2, $3, 'archive', null, '2026-08-17T10:00:00Z')",
    )
    .bind(Uuid::now_v7())
    .bind(tenant)
    .bind(archive_id)
    .execute(database.database.pool())
    .await
    .map(|_| ())
}

async fn seed_social_tombstone(
    database: &TestDatabase,
    tenant: &str,
    source_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into knowledge.social_tombstones (event_id, tenant_ref, source_id, reason, removed_at)
         values ($1, $2, $3, 'user_requested', '2026-08-17T10:00:00Z')",
    )
    .bind(Uuid::now_v7())
    .bind(tenant)
    .bind(source_id)
    .execute(database.database.pool())
    .await
    .map(|_| ())
}

async fn seed_outbox(database: &TestDatabase, tenant: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into knowledge.event_outbox
             (outbox_id, subject, tenant_ref, aggregate_id, correlation_id, payload, dedupe_key)
         values ($1, 'knowledge.analysis.completed.v1', $2,
                 'social_source:018f0000-0000-7000-8000-000000000201',
                 'operation:018f0000-0000-7000-8000-000000000902', '{}'::jsonb, $3)",
    )
    .bind(Uuid::now_v7())
    .bind(tenant)
    .bind(format!("seed:{}", Uuid::now_v7()))
    .execute(database.database.pool())
    .await
    .map(|_| ())
}

async fn seed_repository_request(
    database: &TestDatabase,
    tenant: &str,
    repository_id: Uuid,
    request_id: Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&request_id.simple().to_string().repeat(2))?,
    };
    let request = RepositoryAnalysisRequested {
        owner: tenant.parse::<TenantRef>()?,
        repository_id: RepositoryId::parse(&repository_id.to_string())?,
        github_repository_numeric_id: 42,
        request_id: RepositoryAnalysisRequestId::parse(&request_id.to_string())?,
        source_revision: RepositoryAnalysisRevision {
            attributes_digest: digest.clone(),
            readme: ReadmeRevision::Absent {
                reason: ReadmeAbsenceReason::NotFound,
            },
        },
        repository_attributes: RepositoryAnalysisAttributes {
            repository_full_name: RepositoryFullName::parse("owner/repository")?,
            description: None,
            primary_language: None,
        },
        requested_contract: RepositoryAnalysisContract::RepositoryAnalysis,
        idempotency_key: digest,
        extensions: Extensions::new(),
    };
    let cause = EventCause {
        event_id: Uuid::now_v7(),
        correlation_id: EntityRef::parse("operation:018f0000-0000-7000-8000-000000000902")?,
    };
    RepositoryAnalysisConsumer::new(&database.database)
        .accept(&request, &cause)
        .await?;
    Ok(())
}

/// One of every content-bearing row kind for `tenant`, keyed by `n` so tenants never collide.
async fn seed_everything(
    database: &TestDatabase,
    tenant: &str,
    n: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let social_source = uuid(n * 10 + 1).to_string();
    let conversation = uuid(n * 10 + 2).to_string();
    let archive = uuid(n * 10 + 3).to_string();
    seed_inbox(
        database,
        tenant,
        "social",
        None,
        &social_source,
        "social.source.captured.v1",
        true,
    )
    .await?;
    seed_inbox(
        database,
        tenant,
        "ai_archive",
        Some(&archive),
        &conversation,
        "ai_archive.conversation.added.v1",
        true,
    )
    .await?;
    seed_object(database, tenant, &archive, &uuid(n * 10 + 4).to_string()).await?;
    seed_repository_request(database, tenant, uuid(n * 10 + 5), uuid(n * 10 + 6)).await?;
    seed_ai_tombstone(database, tenant, &archive).await?;
    seed_social_tombstone(database, tenant, &social_source).await?;
    seed_outbox(database, tenant).await?;
    Ok(())
}

const STATE_TABLES: [&str; 7] = [
    "source_analysis_inbox",
    "source_analysis_heads",
    "ai_archive_object_inbox",
    "repository_analysis_requests",
    "ai_archive_tombstones",
    "social_tombstones",
    "event_outbox",
];

async fn rows(database: &TestDatabase, table: &str, tenant: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "select count(*) from knowledge.{table} where tenant_ref = $1"
    ))
    .bind(tenant)
    .fetch_one(database.database.pool())
    .await
}

#[tokio::test]
async fn delete_tenant_removes_all_content_bearing_state() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    seed_everything(&database, T, 1).await?;
    seed_everything(&database, U, 2).await?;

    let receipt = delete_tenant(&database.database, &blobs, T).await?;

    for table in STATE_TABLES {
        assert_eq!(
            rows(&database, table, T).await?,
            0,
            "{table} kept tenant rows"
        );
    }
    for (table, expected) in [
        ("source_analysis_inbox", 2),
        ("source_analysis_heads", 2),
        ("ai_archive_object_inbox", 1),
        ("repository_analysis_requests", 1),
        ("ai_archive_tombstones", 1),
        ("social_tombstones", 1),
        ("event_outbox", 1),
    ] {
        assert_eq!(
            rows(&database, table, U).await?,
            expected,
            "{table} lost another tenant's rows"
        );
    }
    assert_eq!(
        receipt.counts,
        DeletionCounts {
            source_analysis_inbox: 2,
            source_analysis_heads: 2,
            ai_archive_object_inbox: 1,
            repository_analysis_requests: 1,
            ai_archive_tombstones: 1,
            social_tombstones: 1,
            event_outbox: 1,
            ..DeletionCounts::default()
        }
    );
    let audited: (i32, i32, i32, i32, i32, i32, i32) = sqlx::query_as(
        "select source_analysis_inbox_deleted, source_analysis_heads_deleted,
                ai_archive_object_inbox_deleted, repository_analysis_requests_deleted,
                ai_archive_tombstones_deleted, social_tombstones_deleted, event_outbox_deleted
         from knowledge.deletion_records where tenant_ref = $1",
    )
    .bind(T)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(audited, (2, 2, 1, 1, 1, 1, 1));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn delete_source_removes_matching_state_and_keeps_siblings_and_tombstone_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let doomed = uuid(901).to_string();
    let sibling = uuid(902).to_string();
    for source in [&doomed, &sibling] {
        seed_inbox(
            &database,
            T,
            "social",
            None,
            source,
            "social.source.updated.v1",
            true,
        )
        .await?;
        seed_object(&database, T, &uuid(903).to_string(), source).await?;
    }
    seed_repository_request(&database, T, uuid(901), uuid(911)).await?;
    seed_repository_request(&database, T, uuid(902), uuid(912)).await?;
    seed_social_tombstone(&database, T, &doomed).await?;
    seed_ai_tombstone(&database, T, &uuid(903).to_string()).await?;
    let receipt_event = seed_inbox(
        &database,
        T,
        "ai_archive",
        Some(&uuid(903).to_string()),
        &doomed,
        TOMBSTONED,
        false,
    )
    .await?;

    delete_source(&database.database, &blobs, T, KNOWLEDGE, &doomed).await?;

    let remaining: Vec<(String,)> = sqlx::query_as(
        "select source_id from knowledge.source_analysis_inbox
         where tenant_ref = $1 and subject <> $2 order by source_id",
    )
    .bind(T)
    .bind(TOMBSTONED)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(
        remaining,
        [(sibling.clone(),)],
        "only the sibling inbox row stays"
    );
    let heads: Vec<(String,)> = sqlx::query_as(
        "select source_id from knowledge.source_analysis_heads where tenant_ref = $1",
    )
    .bind(T)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(heads, [(sibling.clone(),)]);
    let objects: Vec<(String,)> = sqlx::query_as(
        "select object_id from knowledge.ai_archive_object_inbox where tenant_ref = $1",
    )
    .bind(T)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(objects, [(sibling.clone(),)]);
    let requests: Vec<(Uuid,)> = sqlx::query_as(
        "select repository_id from knowledge.repository_analysis_requests where tenant_ref = $1",
    )
    .bind(T)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(requests, [(uuid(902),)]);
    assert_eq!(
        rows(&database, "social_tombstones", T).await?,
        1,
        "the ordering guard stays"
    );
    assert_eq!(rows(&database, "ai_archive_tombstones", T).await?, 1);
    let receipt_kept: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.source_analysis_inbox where event_id = $1",
    )
    .bind(receipt_event)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(
        receipt_kept, 1,
        "a tombstone's own dedup receipt is not derived state"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn archive_scope_removes_archive_keyed_state() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let (doomed, kept) = (uuid(921).to_string(), uuid(922).to_string());
    for (archive, conversation) in [(&doomed, uuid(923)), (&kept, uuid(924))] {
        seed_inbox(
            &database,
            T,
            "ai_archive",
            Some(archive),
            &conversation.to_string(),
            "ai_archive.conversation.added.v1",
            true,
        )
        .await?;
        seed_object(&database, T, archive, &uuid(925).to_string()).await?;
    }
    seed_ai_tombstone(&database, T, &doomed).await?;

    let mut transaction = database.database.pool().begin().await?;
    let counts = execute_deletion(
        &mut transaction,
        &DeletionScope::Archive {
            tenant_ref: T.to_owned(),
            ai_archive_id: doomed.clone(),
        },
    )
    .await?;
    transaction.commit().await?;

    assert_eq!(counts.source_analysis_inbox, 1);
    assert_eq!(counts.source_analysis_heads, 1);
    assert_eq!(counts.ai_archive_object_inbox, 1);
    assert_eq!(
        counts.ai_archive_tombstones, 0,
        "the archive guard outlives the archive scope"
    );
    let archives: Vec<(Option<String>,)> = sqlx::query_as(
        "select archive_id from knowledge.source_analysis_inbox where tenant_ref = $1",
    )
    .bind(T)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(archives, [(Some(kept.clone()),)]);
    assert_eq!(rows(&database, "ai_archive_object_inbox", T).await?, 1);
    assert_eq!(rows(&database, "ai_archive_tombstones", T).await?, 1);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn verify_scope_empty_fails_when_inbox_rows_survive() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    seed_everything(&database, T, 3).await?;
    let archive = uuid(33).to_string();
    let social = uuid(31).to_string();

    for scope in [
        DeletionScope::Tenant {
            tenant_ref: T.to_owned(),
        },
        DeletionScope::Source {
            tenant_ref: T.to_owned(),
            owner_context: KNOWLEDGE.to_owned(),
            source_document_id: social,
        },
        DeletionScope::Archive {
            tenant_ref: T.to_owned(),
            ai_archive_id: archive,
        },
    ] {
        let verdict = verify_scope_empty(&database.database, &scope).await;
        assert!(
            matches!(verdict, Err(DeletionError::VerificationFailed)),
            "{scope:?} survivors must fail verification: {verdict:?}"
        );
    }
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn tombstone_path_keeps_its_own_dedup_receipt() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let inbox = SourceInbox::new(&database.database);
    let conversation = "018f0000-0000-7000-8000-000000000403";
    let archive = "018f0000-0000-7000-8000-000000000402";
    let owner = "user:018f0000-0000-7000-8000-000000000005";
    seed_inbox(
        &database,
        owner,
        "ai_archive",
        Some(archive),
        conversation,
        "ai_archive.conversation.added.v1",
        true,
    )
    .await?;
    let tombstone: AiArchiveTombstone = serde_json::from_value(serde_json::json!({
        "ai_archive_id": archive,
        "provider": "chatgpt",
        "owner": owner,
        "subject": {"subject_kind": "conversation", "ai_conversation_id": conversation},
        "reason": "provider_deletion_event",
        "evidence_ref": {
            "owner_service": "ratatoskr-chatgpt",
            "digest": {"algorithm": "sha256", "hex": DIGEST},
            "media_type": "application/json",
            "length_bytes": 512
        },
        "observed_at": "2026-08-27T06:00:00Z"
    }))?;
    let event = uuid(41);

    assert_eq!(
        inbox
            .accept_ai_tombstone(event, TOMBSTONED, &tombstone)
            .await?,
        SourceInboxAdmission::AcceptedCurrent
    );
    let kept: Vec<(String,)> =
        sqlx::query_as("select subject from knowledge.source_analysis_inbox where tenant_ref = $1")
            .bind(owner)
            .fetch_all(database.database.pool())
            .await?;
    assert_eq!(
        kept,
        [(TOMBSTONED.to_owned(),)],
        "the conversation delivery is removed and the tombstone receipt stays"
    );
    assert_eq!(
        inbox
            .accept_ai_tombstone(event, TOMBSTONED, &tombstone)
            .await?,
        SourceInboxAdmission::Duplicate,
        "a redelivered tombstone is still a duplicate after its deletion ran"
    );
    database.cleanup().await?;
    Ok(())
}

/// Changeset OBS-023: the notes guards name the tenant and one file or one connection, and a
/// tenant deletion removes them with everything else the tenant owns.
#[tokio::test]
async fn tenant_deletion_removes_the_notes_guards() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    for (tenant, tail) in [(T, 1), (U, 2)] {
        sqlx::query(
            "insert into knowledge.notes_files
                 (tenant_ref, file_id, connection_id, connection_sequence, deleted)
             values ($1, $2, $3, 1, false), ($1, $4, $3, 2, true)",
        )
        .bind(tenant)
        .bind(uuid(100 + tail))
        .bind(uuid(200 + tail))
        .bind(uuid(300 + tail))
        .execute(database.database.pool())
        .await?;
        sqlx::query(
            "insert into knowledge.notes_disconnections (tenant_ref, connection_id)
             values ($1, $2)",
        )
        .bind(tenant)
        .bind(uuid(400 + tail))
        .execute(database.database.pool())
        .await?;
    }

    let receipt = delete_tenant(&database.database, &blobs, T).await?;

    for table in ["notes_files", "notes_disconnections"] {
        assert_eq!(
            rows(&database, table, T).await?,
            0,
            "{table} rows of the deleted tenant survived"
        );
    }
    assert_eq!(rows(&database, "notes_files", U).await?, 2);
    assert_eq!(rows(&database, "notes_disconnections", U).await?, 1);
    assert_eq!(
        receipt.counts,
        DeletionCounts {
            notes_files: 2,
            notes_disconnections: 1,
            ..DeletionCounts::default()
        }
    );
    let audited: (i32, i32) = sqlx::query_as(
        "select notes_files_deleted, notes_disconnections_deleted
         from knowledge.deletion_records where tenant_ref = $1",
    )
    .bind(T)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(audited, (2, 1), "the audit row counts the notes guards");
    database.cleanup().await?;
    Ok(())
}
