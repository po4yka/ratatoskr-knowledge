//! The real service process with the ingest worker enabled in fixture mode: a published document
//! becomes searchable through the loopback search route with no inference credential and no SQL
//! seeding, and an ingest configuration that cannot finish its work is refused at startup (XR-021
//! CONTRACTS.md S02 and S09).

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, pull};
use async_nats::jetstream::{self, stream};
use ratatoskr_document_contracts::{
    ContentDocumentExtracted, Document, DocumentAddress, DocumentBlock,
};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, Extensions,
    MediaType,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge_service::{EVENTS_STREAM, IngestFamily};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";

fn command(address: SocketAddr, database_url: &str, blob_root: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ratatoskr-knowledge-service"));
    command
        .env("RATATOSKR__ADMIN__LISTEN_ADDRESS", address.to_string())
        .env("RATATOSKR__API__LISTEN_ADDRESS", free_loopback_address())
        .env("RATATOSKR__STORAGE__DATABASE_URL", database_url)
        .env("RATATOSKR__STORAGE__BLOB_ROOT", blob_root)
        .env("RATATOSKR__INGEST__ENABLED", "true")
        .env("RATATOSKR__INGEST__PROVIDER_MODE", "fixture");
    command
}

fn http_get(address: SocketAddr, path: &str) -> Result<(u16, String), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(200))?;
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
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
    let body = response
        .split_once("\r\n\r\n")
        .map_or_else(String::new, |(_, body)| body.to_owned());
    Ok((status, body))
}

fn wait_until(
    child: &mut Child,
    what: &str,
    mut check: impl FnMut() -> Result<bool, Box<dyn std::error::Error>>,
) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(format!("the process exited ({status}) while waiting for: {what}").into());
        }
        if check().unwrap_or(false) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for: {what}").into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn stop(child: &mut Child) -> TestResult {
    let signalled = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()?;
    if !signalled.success() {
        return Err("could not signal the process".into());
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            child.kill()?;
            return Err("the process did not stop within the shutdown bound".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

async fn provision(url: &str) -> Result<jetstream::Context, Box<dyn std::error::Error>> {
    let context = jetstream::new(async_nats::connect(url).await?);
    let _absent_is_fine = context.delete_stream(EVENTS_STREAM).await;
    let events = context
        .create_stream(stream::Config {
            name: EVENTS_STREAM.to_owned(),
            subjects: vec!["evt.>".to_owned()],
            ..stream::Config::default()
        })
        .await?;
    for family in IngestFamily::ALL {
        events
            .create_consumer(pull::Config {
                durable_name: Some(family.durable().to_owned()),
                filter_subject: family.filter().to_owned(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_mins(2),
                deliver_policy: DeliverPolicy::All,
                max_deliver: -1,
                ..pull::Config::default()
            })
            .await?;
    }
    Ok(context)
}

fn extracted_envelope() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&"a".repeat(64))?,
    };
    let payload = ContentDocumentExtracted {
        document: Document {
            document_id: DocumentId::parse("018f0000-0000-7000-8000-000000000a01")?,
            source_address: DocumentAddress::parse("document:quokka-notes")?,
            content_digest: digest.clone(),
            title: Some("Quokka migration notes".to_owned()),
            language: None,
            blocks: vec![DocumentBlock::Paragraph {
                block_id: BlockId::new_v7(),
                text: "Quokkas were observed crossing the island in March.".to_owned(),
            }],
            provenance: Vec::new(),
        },
        document_blob: BlobRef {
            owner_service: BlobOwner::parse("ratatoskr-extractor")?,
            digest,
            media_type: MediaType::parse("application/json")?,
            length_bytes: 256,
        },
        extensions: Extensions::new(),
    };
    Ok(serde_json::to_vec(&serde_json::json!({
        "event_id": "018f0000-0000-7000-8000-000000000a02",
        "event_type": "content.document.extracted.v1",
        "occurred_at": "2026-08-27T06:00:00Z",
        "producer": "ratatoskr-extractor",
        "aggregate_id": "document:018f0000-0000-7000-8000-000000000a01",
        "correlation_id": "operation:018f0000-0000-7000-8000-000000000a03",
        "tenant_id": OWNER,
        "schema_version": 1,
        "payload": serde_json::to_value(payload)?,
    }))?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixture_mode_process_makes_a_published_document_searchable() -> TestResult {
    let database = TestDatabase::create().await?;
    let database_name: String = sqlx::query_scalar("select current_database()")
        .fetch_one(database.database.pool())
        .await?;
    let admin_url = std::env::var("KNOWLEDGE_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://extractor:extractor@127.0.0.1:5434/extractor".to_owned());
    let (server, _) = admin_url
        .rsplit_once('/')
        .ok_or("invalid test database URL")?;
    let database_url = format!("{server}/{database_name}");
    let blob_root = std::env::temp_dir().join(format!("knowledge-ingest-boot-{database_name}"));
    std::fs::create_dir_all(&blob_root)?;
    let nats_url = std::env::var("KNOWLEDGE_TEST_NATS_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:14223".to_owned());
    let context = provision(&nats_url).await?;

    let reserved = TcpListener::bind("127.0.0.1:0")?;
    let address = reserved.local_addr()?;
    let api_reserved = TcpListener::bind("127.0.0.1:0")?;
    let api = api_reserved.local_addr()?;
    let mut ingest = command(address, &database_url, &blob_root);
    ingest
        .env("RATATOSKR__API__LISTEN_ADDRESS", api.to_string())
        .env("RATATOSKR__INGEST__BUS_ENDPOINT", &nats_url)
        .env(
            "RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET",
            "ingest-boot-readme-secret",
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    drop(reserved);
    drop(api_reserved);
    let mut child = ingest.spawn()?;

    wait_until(&mut child, "the ingest worker is ready", || {
        Ok(http_get(address, "/ready")?.0 == 200)
    })?;
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "018f0000-0000-7000-8000-000000000a02");
    context
        .publish_with_headers(
            "evt.content.document.extracted.v1".to_owned(),
            headers,
            extracted_envelope()?.into(),
        )
        .await?
        .await?;
    wait_until(&mut child, "the document is searchable", || {
        let (status, body) = http_get(
            api,
            &format!("/internal/search?tenant={OWNER}&q=quokka&limit=5"),
        )?;
        Ok(status == 200 && body.contains("Quokka migration notes"))
    })?;
    stop(&mut child)?;

    let _ignored = std::fs::remove_dir_all(blob_root);
    database.cleanup().await?;
    Ok(())
}

/// A worker that consumes must also be able to finish what it consumes: without the README source
/// the configuration is refused before the process binds anything.
#[test]
fn ingest_without_its_readme_source_is_refused_at_startup() -> TestResult {
    let reserved = TcpListener::bind("127.0.0.1:0")?;
    let address = reserved.local_addr()?;
    let mut missing_secret = command(
        address,
        "postgres://knowledge:knowledge@127.0.0.1:5432/knowledge",
        std::path::Path::new("/nonexistent/knowledge-blobs"),
    );
    let output = missing_secret
        .arg("check-config")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()?;
    drop(reserved);
    assert!(!output.status.success());
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(
        diagnostic.contains("RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET"),
        "the refusal names the missing key: {diagnostic}"
    );
    Ok(())
}

/// A loopback address the kernel just handed out, so parallel processes never share an API port.
fn free_loopback_address() -> String {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map_or_else(
            |_| "127.0.0.1:18091".to_owned(),
            |address| address.to_string(),
        )
}
