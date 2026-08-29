//! Real process startup test.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport, TestDatabase};

#[tokio::test]
async fn configured_process_serves_admin_without_inference_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let database_name: String = sqlx::query_scalar("select current_database()")
        .fetch_one(database.database.pool())
        .await?;
    let database_url = test_database_url(&database_name)?;
    let reserved = TcpListener::bind("127.0.0.1:0")?;
    let address = reserved.local_addr()?;
    let blob_root = std::env::temp_dir().join(format!("knowledge-boot-{database_name}"));
    std::fs::create_dir_all(&blob_root)?;

    let check = configured_command(address, &database_url, &blob_root)
        .arg("check-config")
        .status()?;
    assert!(check.success());
    drop(reserved);

    let mut child = configured_command(address, &database_url, &blob_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let result = exercise_process(&mut child, address);
    stop_process(&mut child)?;

    let _ignored = std::fs::remove_dir_all(blob_root);
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn channel_recap_scripted_configuration_requires_no_inference_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let database_url = "postgres://knowledge:knowledge@127.0.0.1:5432/knowledge";
    let reserved = TcpListener::bind("127.0.0.1:0")?;
    let address = reserved.local_addr()?;
    let source = TcpListener::bind("127.0.0.1:0")?;
    let blob_root = std::env::temp_dir().join(format!("knowledge-recap-boot-{}", address.port()));
    std::fs::create_dir_all(&blob_root)?;

    let status = configured_command(address, database_url, &blob_root)
        .env("RATATOSKR__CHANNEL_RECAP__ENABLED", "true")
        .env("RATATOSKR__CHANNEL_RECAP__PROVIDER_MODE", "scripted")
        .env(
            "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_BASE_URL",
            format!("http://{}/", source.local_addr()?),
        )
        .env(
            "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_SERVICE_SECRET",
            "synthetic-service-secret",
        )
        .env(
            "RATATOSKR__CHANNEL_RECAP__BUS_ENDPOINT",
            "nats://127.0.0.1:4222",
        )
        .env("RATATOSKR__CHANNEL_RECAP__BUS_STREAM", "ratatoskr_commands")
        .env(
            "RATATOSKR__CHANNEL_RECAP__BUS_DURABLE",
            "ratatoskr_knowledge_channel_recap",
        )
        .env(
            "RATATOSKR__CHANNEL_RECAP__BUS_SUBJECT",
            "cmd.knowledge.channel_digest_recap.requested.v1",
        )
        .arg("check-config")
        .status()?;

    assert!(
        status.success(),
        "scripted recap configuration must not require inference credentials"
    );
    drop(source);
    drop(reserved);
    let _ignored = std::fs::remove_dir_all(blob_root);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn channel_recap_consumer_source_readiness_drains_and_resumes()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let database_name: String = sqlx::query_scalar("select current_database()")
        .fetch_one(database.database.pool())
        .await?;
    let database_url = test_database_url(&database_name)?;
    let blob_root = std::env::temp_dir().join(format!("knowledge-recap-runtime-{database_name}"));
    std::fs::create_dir_all(&blob_root)?;
    let source = FakeTransport::start(
        std::iter::repeat_with(|| FakeReply::bytes(200, Vec::new()))
            .take(16)
            .collect(),
    )
    .await?;
    let nats_url = std::env::var("KNOWLEDGE_TEST_NATS_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:14223".to_owned());
    provision_recap_consumer(&nats_url).await?;

    for _restart in 0..2 {
        let reserved = TcpListener::bind("127.0.0.1:0")?;
        let address = reserved.local_addr()?;
        let mut command = configured_command(address, &database_url, &blob_root);
        configure_recap_runtime(&mut command, source.local_addr(), &nats_url);
        drop(reserved);
        let mut child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        exercise_process(&mut child, address)?;
        stop_process(&mut child)?;
    }

    let requests = source.recorded()?;
    assert!(
        requests.len() >= 2,
        "source readiness must be reprobed after restart"
    );
    assert!(requests.iter().all(|request| request.path == "/ready"));
    assert!(requests.iter().all(|request| {
        request.authorization.as_deref() == Some("Bearer synthetic-service-secret")
    }));
    let _ignored = std::fs::remove_dir_all(blob_root);
    database.cleanup().await?;
    Ok(())
}

fn configure_recap_runtime(command: &mut Command, source: SocketAddr, nats_url: &str) {
    command
        .env("RATATOSKR__CHANNEL_RECAP__ENABLED", "true")
        .env("RATATOSKR__CHANNEL_RECAP__PROVIDER_MODE", "scripted")
        .env(
            "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_BASE_URL",
            format!("http://{source}/"),
        )
        .env(
            "RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_SERVICE_SECRET",
            "synthetic-service-secret",
        )
        .env("RATATOSKR__CHANNEL_RECAP__BUS_ENDPOINT", nats_url)
        .env("RATATOSKR__CHANNEL_RECAP__BUS_STREAM", "ratatoskr_commands")
        .env(
            "RATATOSKR__CHANNEL_RECAP__BUS_DURABLE",
            "ratatoskr_knowledge_channel_recap",
        )
        .env(
            "RATATOSKR__CHANNEL_RECAP__BUS_SUBJECT",
            "cmd.knowledge.channel_digest_recap.requested.v1",
        );
}

async fn provision_recap_consumer(nats_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect(nats_url).await?;
    let context = async_nats::jetstream::new(client);
    let stream = context
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "ratatoskr_commands".to_owned(),
            subjects: vec!["cmd.knowledge.channel_digest_recap.requested.v1".to_owned()],
            max_messages: 1_000,
            max_bytes: 16_777_216,
            ..async_nats::jetstream::stream::Config::default()
        })
        .await?;
    stream
        .get_or_create_consumer(
            "ratatoskr_knowledge_channel_recap",
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("ratatoskr_knowledge_channel_recap".to_owned()),
                filter_subject: "cmd.knowledge.channel_digest_recap.requested.v1".to_owned(),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                ..async_nats::jetstream::consumer::pull::Config::default()
            },
        )
        .await?;
    Ok(())
}

/// One source seeded with every derived row kind for deletion.
struct SeededSource {
    tenant_ref: String,
    owner_context: String,
    source_document_id: String,
}

/// Seeds one source's derived rows through SQL and stores its response
/// bytes in the temporary blob root.
async fn seed_deletable_source(
    database: &TestDatabase,
    blob_root: &std::path::Path,
) -> Result<SeededSource, Box<dyn std::error::Error>> {
    let blobs = ratatoskr_knowledge::BlobStore::new(blob_root, 4_096);
    let response = blobs.store_raw(br#"{"seeded":"response"}"#).await?;
    let reference = serde_json::to_value(&response)?;

    let seeded = seed_source_revision(database, &reference).await?;
    let output_id = seed_run_with_responses(database, &seeded, &reference).await?;
    seed_projection_and_vectors(database, &seeded, output_id).await?;
    Ok(seeded)
}

/// Inserts the immutable source revision row and returns its identity.
async fn seed_source_revision(
    database: &TestDatabase,
    reference: &serde_json::Value,
) -> Result<SeededSource, Box<dyn std::error::Error>> {
    let row: (String, String) = sqlx::query_as(
        "insert into knowledge.source_refs (
             source_ref_id, tenant_ref, owner_context, source_document_id,
             content_digest_algorithm, content_digest_hex, source_blob
         ) values (gen_random_uuid(), 'user:' || gen_random_uuid()::text,
                   'ratatoskr-extractor', gen_random_uuid()::text,
                   'sha256', $1, $2)
         returning tenant_ref, source_document_id",
    )
    .bind("a".repeat(64))
    .bind(reference)
    .fetch_one(database.database.pool())
    .await?;
    Ok(SeededSource {
        tenant_ref: row.0,
        owner_context: "ratatoskr-extractor".to_owned(),
        source_document_id: row.1,
    })
}

/// Inserts the run, its invalid attempt, and its accepted output.
async fn seed_run_with_responses(
    database: &TestDatabase,
    seeded: &SeededSource,
    reference: &serde_json::Value,
) -> Result<sqlx::types::Uuid, Box<dyn std::error::Error>> {
    let (run_id,): (sqlx::types::Uuid,) = sqlx::query_as(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         ) values (gen_random_uuid(),
                   (select source_ref_id from knowledge.source_refs
                    where tenant_ref = $1 and source_document_id = $2),
                   'article-analysis.v1', 'v1', 'v1', 'fake_default_v1',
                   'completed')
         returning run_id",
    )
    .bind(&seeded.tenant_ref)
    .bind(&seeded.source_document_id)
    .fetch_one(database.database.pool())
    .await?;

    sqlx::query(
        "insert into knowledge.analysis_attempts (
             run_id, ordinal, reason, provider, model_policy, model,
             raw_response, outcome
         ) values ($1, 1, 'initial', 'openrouter', 'fake_default_v1',
                   'openai/gpt-oss-20b', $2, 'invalid')",
    )
    .bind(run_id)
    .bind(reference)
    .execute(database.database.pool())
    .await?;

    let (output_id,): (sqlx::types::Uuid,) = sqlx::query_as(
        "insert into knowledge.analysis_outputs (output_id, run_id, result, raw_response)
         values (gen_random_uuid(), $1, '{}', $2) returning output_id",
    )
    .bind(run_id)
    .bind(reference)
    .fetch_one(database.database.pool())
    .await?;
    Ok(output_id)
}

/// Inserts the projection, two identity-distinct chunks, and a failure.
async fn seed_projection_and_vectors(
    database: &TestDatabase,
    seeded: &SeededSource,
    output_id: sqlx::types::Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "insert into knowledge.search_projection_inputs (
             source_ref_id, latest_output_id, tenant_ref, owner_context,
             document_id, title, lead, body, updated_at
         ) values (
                   (select source_ref_id from knowledge.source_refs
                    where tenant_ref = $1 and source_document_id = $2),
                   $3, $1, 'ratatoskr-extractor',
                   (select source_document_id::uuid from knowledge.source_refs
                    where tenant_ref = $1 and source_document_id = $2),
                   'Boot title', 'Boot lead.', 'Boot body.', now())",
    )
    .bind(&seeded.tenant_ref)
    .bind(&seeded.source_document_id)
    .bind(output_id)
    .execute(database.database.pool())
    .await?;
    let vector_text = format!("[{}]", vec!["0"; 1536].join(","));

    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref,
             owner_context, document_id, title, lead, body, updated_at
         ) values (gen_random_uuid(),
                   (select source_ref_id from knowledge.source_refs
                    where tenant_ref = $1 and source_document_id = $2),
                   $3, $1, 'ratatoskr-extractor',
                   (select source_document_id::uuid from knowledge.source_refs
                    where tenant_ref = $1 and source_document_id = $2),
                   'Boot title', 'Boot lead.', 'Boot body.', now())",
    )
    .bind(&seeded.tenant_ref)
    .bind(&seeded.source_document_id)
    .bind(output_id)
    .execute(database.database.pool())
    .await?;

    for provider in ["scripted_fake", "legacy_embedder"] {
        sqlx::query(&format!(
            "insert into knowledge.embedding_chunks (
                 embedding_chunk_id, source_ref_id, output_id, tenant_ref,
                 owner_context, document_id, ordinal, chunk_text,
                 chunk_digest_hex, chunking_version, provider, model,
                 dimensions, prompt_version, embedding
             ) values (gen_random_uuid(),
                       (select source_ref_id from knowledge.source_refs
                        where tenant_ref = $1 and source_document_id = $2),
                       $3, $1, 'ratatoskr-extractor',
                       (select source_document_id::uuid from knowledge.source_refs
                        where tenant_ref = $1 and source_document_id = $2),
                       0, 'Boot chunk.', $4, 'article-chunks.v1', '{provider}',
                       'model_v1', 1536, 'none.v1', $5::vector)"
        ))
        .bind(&seeded.tenant_ref)
        .bind(&seeded.source_document_id)
        .bind(output_id)
        .bind("b".repeat(64))
        .bind(&vector_text)
        .execute(database.database.pool())
        .await?;
    }

    sqlx::query(
        "insert into knowledge.embedding_failures (
             failure_id, source_ref_id, output_id, tenant_ref,
             chunking_version, provider, model, prompt_version,
             error_class, attempt
         ) values (gen_random_uuid(),
                   (select source_ref_id from knowledge.source_refs
                    where tenant_ref = $1 and source_document_id = $2),
                   $3, $1, 'article-chunks.v1', 'legacy_embedder',
                   'model_v1', 'none.v1', 'rate_limited', 1)",
    )
    .bind(&seeded.tenant_ref)
    .bind(&seeded.source_document_id)
    .bind(output_id)
    .execute(database.database.pool())
    .await?;
    Ok(())
}

#[tokio::test]
async fn delete_source_subcommand_prints_receipt_and_exits_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let database_name: String = sqlx::query_scalar("select current_database()")
        .fetch_one(database.database.pool())
        .await?;
    let database_url = test_database_url(&database_name)?;
    let blob_root = std::env::temp_dir().join(format!("knowledge-delete-{database_name}"));
    std::fs::create_dir_all(&blob_root)?;
    let seeded = seed_deletable_source(&database, &blob_root).await?;

    // Reserve a port so the configuration stays valid; an unrecognized
    // argument makes the process attempt to boot its admin listener there
    // instead of running the deletion.
    let reserved = TcpListener::bind("127.0.0.1:0")?;
    let mut child = configured_command(reserved.local_addr()?, &database_url, &blob_root)
        .args([
            "delete-source",
            &seeded.tenant_ref,
            &seeded.owner_context,
            &seeded.source_document_id,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let outcome = bounded_exit_with_output(&mut child, Duration::from_mins(1));
    drop(reserved);
    let (status, stdout) = outcome?;

    assert!(
        status.success(),
        "delete-source must exit zero; stdout was: {stdout}"
    );
    for expected in [
        "scope=source",
        "source_refs=1",
        "analysis_runs=1",
        "analysis_attempts=1",
        "analysis_outputs=1",
        "search_projection_inputs=1",
        "search_documents=1",
        "embedding_chunks=2",
        "embedding_failures=1",
        "removed_blobs=",
    ] {
        assert!(
            stdout.contains(expected),
            "receipt must carry {expected}; stdout was: {stdout}"
        );
    }

    let (remaining,): (i64,) = sqlx::query_as(
        "select (
             select count(*) from knowledge.source_refs where tenant_ref = $1
         ) + (select count(*) from knowledge.analysis_runs r
              join knowledge.source_refs s on s.source_ref_id = r.source_ref_id
              where s.tenant_ref = $1)
           + (select count(*) from knowledge.search_documents where tenant_ref = $1)
           + (select count(*) from knowledge.search_projection_inputs where tenant_ref = $1)
           + (select count(*) from knowledge.embedding_chunks where tenant_ref = $1)
           + (select count(*) from knowledge.embedding_failures where tenant_ref = $1)",
    )
    .bind(&seeded.tenant_ref)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(remaining, 0, "no scoped row may survive");

    let _ignored = std::fs::remove_dir_all(blob_root);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reindex_embeddings_subcommand_reports_totals_and_exits_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let database_name: String = sqlx::query_scalar("select current_database()")
        .fetch_one(database.database.pool())
        .await?;
    let database_url = test_database_url(&database_name)?;
    let blob_root = std::env::temp_dir().join(format!("knowledge-reindex-{database_name}"));
    std::fs::create_dir_all(&blob_root)?;

    // A usable embeddings configuration is part of the job contract; the
    // reserved loopback port keeps it valid without any reachable provider,
    // and an empty plan must never dial it.
    let reserved = TcpListener::bind("127.0.0.1:0")?;
    let address = reserved.local_addr()?;
    let mut child = configured_command(address, &database_url, &blob_root)
        .args(["reindex-embeddings"])
        .env("RATATOSKR__PROVIDER__EMBEDDINGS__API_KEY", "test-key")
        .env("RATATOSKR__PROVIDER__EMBEDDINGS__MODEL", "fake-embedder")
        .env(
            "RATATOSKR__PROVIDER__EMBEDDINGS__BASE_URL",
            format!("http://{address}/v1"),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let outcome = bounded_exit_with_output(&mut child, Duration::from_mins(1));
    drop(reserved);
    let (status, stdout) = outcome?;

    assert!(
        status.success(),
        "reindex-embeddings must exit zero on an empty plan; stdout was: {stdout}"
    );
    assert!(
        stdout.contains("processed=0"),
        "totals must report zero processed; stdout was: {stdout}"
    );
    assert!(
        stdout.contains("failed=0"),
        "totals must report zero failed; stdout was: {stdout}"
    );

    let _ignored = std::fs::remove_dir_all(blob_root);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn embeddings_reindex_without_credential_fails_fast_cleanly()
-> Result<(), Box<dyn std::error::Error>> {
    let blob_root = std::env::temp_dir().join("knowledge-reindex-missing-credential");
    std::fs::create_dir_all(&blob_root)?;
    let reserved = TcpListener::bind("127.0.0.1:0")?;
    let mut child = configured_command(
        reserved.local_addr()?,
        "postgres://knowledge:knowledge@127.0.0.1:1/knowledge",
        &blob_root,
    )
    .arg("reindex-embeddings")
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;
    let (status, output) = bounded_exit_with_output(&mut child, Duration::from_secs(5))?;
    drop(reserved);

    assert!(!status.success(), "a missing embeddings identity must fail");
    assert!(
        output.contains("embeddings configuration"),
        "the error must name the missing configuration without opening the database: {output}"
    );

    let _ignored = std::fs::remove_dir_all(blob_root);
    Ok(())
}

#[tokio::test]
async fn job_output_lists_sources_in_ascending_order_with_totals()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let database_name: String = sqlx::query_scalar("select current_database()")
        .fetch_one(database.database.pool())
        .await?;
    let database_url = test_database_url(&database_name)?;
    let blob_root = std::env::temp_dir().join(format!("knowledge-search-reindex-{database_name}"));
    std::fs::create_dir_all(&blob_root)?;
    let first = seed_deletable_source(&database, &blob_root).await?;
    let second = seed_deletable_source(&database, &blob_root).await?;
    let mut expected = vec![
        source_ref_id(&database, &first).await?.to_string(),
        source_ref_id(&database, &second).await?.to_string(),
    ];
    expected.sort();
    sqlx::query(
        "update knowledge.search_documents set title = 'damaged'
         where tenant_ref = any($1)",
    )
    .bind(vec![first.tenant_ref.clone(), second.tenant_ref.clone()])
    .execute(database.database.pool())
    .await?;

    let reserved = TcpListener::bind("127.0.0.1:0")?;
    let mut child = configured_command(reserved.local_addr()?, &database_url, &blob_root)
        .arg("reindex-search-documents")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let outcome = bounded_exit_with_output(&mut child, Duration::from_secs(5));
    drop(reserved);
    let (status, stdout) = outcome?;

    assert!(
        status.success(),
        "reindex-search-documents must exit zero; stdout was: {stdout}"
    );
    let sources: Vec<String> = stdout
        .lines()
        .filter_map(|line| line.strip_prefix("source "))
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_owned)
        .collect();
    assert_eq!(sources, expected, "progress must follow source id order");
    assert!(
        stdout.contains("reindex-search-documents processed=2 failed=0"),
        "totals must match the committed rows; stdout was: {stdout}"
    );

    let _ignored = std::fs::remove_dir_all(blob_root);
    database.cleanup().await?;
    Ok(())
}

/// Resolves the durable source revision identifier for one process fixture.
async fn source_ref_id(
    database: &TestDatabase,
    seeded: &SeededSource,
) -> Result<sqlx::types::Uuid, Box<dyn std::error::Error>> {
    Ok(sqlx::query_scalar(
        "select source_ref_id from knowledge.source_refs
         where tenant_ref = $1 and owner_context = $2 and source_document_id = $3",
    )
    .bind(&seeded.tenant_ref)
    .bind(&seeded.owner_context)
    .bind(&seeded.source_document_id)
    .fetch_one(database.database.pool())
    .await?)
}

/// Waits for one bounded exit and collects piped stdout.
fn bounded_exit_with_output(
    child: &mut Child,
    bound: Duration,
) -> Result<(ExitStatus, String), Box<dyn std::error::Error>> {
    let mut stdout_pipe = child.stdout.take().ok_or("stdout was not piped")?;
    let mut stderr_pipe = child.stderr.take().ok_or("stderr was not piped")?;
    let stdout_reader = std::thread::spawn(move || -> std::io::Result<String> {
        let mut text = String::new();
        stdout_pipe.read_to_string(&mut text)?;
        Ok(text)
    });
    let stderr_reader = std::thread::spawn(move || -> std::io::Result<String> {
        let mut text = String::new();
        stderr_pipe.read_to_string(&mut text)?;
        Ok(text)
    });
    let deadline = Instant::now() + bound;
    loop {
        if let Some(status) = child.try_wait()? {
            let stdout = stdout_reader
                .join()
                .map_err(|_| "stdout reader panicked")??;
            let stderr = stderr_reader
                .join()
                .map_err(|_| "stderr reader panicked")??;
            return Ok((status, format!("{stdout}{stderr}")));
        }
        if Instant::now() >= deadline {
            child.kill()?;
            return Err("process did not exit within the bound".into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn configured_command(
    address: SocketAddr,
    database_url: &str,
    blob_root: &std::path::Path,
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ratatoskr-knowledge-service"));
    command
        .env("RATATOSKR__ADMIN__LISTEN_ADDRESS", address.to_string())
        .env("RATATOSKR__STORAGE__DATABASE_URL", database_url)
        .env("RATATOSKR__STORAGE__BLOB_ROOT", blob_root);
    command
}

fn exercise_process(
    child: &mut Child,
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(format!("process exited before readiness: {status}").into());
        }
        if http_status(address, "/ready").is_ok_and(|status| status == 200) {
            break;
        }
        if Instant::now() >= deadline {
            return Err("readiness did not arrive".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(http_status(address, "/live")?, 200);
    assert_eq!(http_status(address, "/analyze")?, 404);
    Ok(())
}

fn http_status(address: SocketAddr, path: &str) -> Result<u16, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(100))?;
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("missing HTTP status")?
        .parse()?;
    Ok(status)
}

fn stop_process(child: &mut Child) -> Result<(), Box<dyn std::error::Error>> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    let signal = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()?;
    if !signal.success() {
        return Err("could not signal process".into());
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            child.kill()?;
            return Err("process did not stop within the shutdown bound".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

fn test_database_url(database_name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let admin_url = std::env::var("KNOWLEDGE_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://extractor:extractor@127.0.0.1:5434/extractor".to_owned());
    let (server, _) = admin_url
        .rsplit_once('/')
        .ok_or("invalid test database URL")?;
    Ok(format!("{server}/{database_name}"))
}
