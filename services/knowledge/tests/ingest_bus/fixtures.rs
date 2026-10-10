//! Events, payloads, a routed provider and a path-routed HTTP stub shared by the ingest tests.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use ratatoskr_document_contracts::{
    ContentDocumentExtracted, Document, DocumentAddress, DocumentBlock,
};
use ratatoskr_github_contracts::{
    ReadmeAbsenceReason, ReadmeRevision, RepositoryAnalysisAttributes, RepositoryAnalysisContract,
    RepositoryAnalysisRequested, RepositoryAnalysisRevision, RepositoryFullName,
};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, Extensions,
    MediaType, RepositoryAnalysisRequestId, RepositoryId, TenantRef,
};
use ratatoskr_knowledge::{
    GenerationRequest, LlmProvider, ProviderError, ProviderFailure, ProviderIdentity,
    ProviderResponse, ProviderUsage,
};
use sha2::Digest as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

pub(crate) const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";
pub(crate) const OTHER_OWNER: &str = "user:018f0000-0000-7000-8000-000000000006";
pub(crate) const SOCIAL: &str = r#"{
  "social_source_id":"018f0000-0000-7000-8000-000000000201",
  "platform":"x", "external_post_id":"1234567890123456789",
  "owner":"user:018f0000-0000-7000-8000-000000000005",
  "captured_at":"2026-08-17T10:00:00Z", "text":"A useful post.",
  "content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
  "acquisition":"official_api", "saved_authority":"authoritative_platform_state",
  "completeness":"complete", "upstream_availability":"available"
}"#;
pub(crate) const SOCIAL_ANALYSIS: &str = r#"{"summary":"Useful post.","topics":["rust"],"evidence_excerpt":"useful post","confidence":"grounded"}"#;
pub(crate) const ARCHIVE_ANALYSIS: &str = r#"{"summary":"The user asked for a one-line explanation.","summary_message_ids":["msg-0001"],"decisions":[{"text":"Keep the explanation to one line.","message_id":"msg-0001"}]}"#;
pub(crate) const ARTICLE_ANALYSIS: &str = r#"{"summary":"Quokkas crossed the island in March.","key_points":[{"text":"Quokkas were observed.","source_block_indexes":[0]}]}"#;
pub(crate) const REPOSITORY_ANALYSIS_PRESENT: &str = r#"{"summary":"A Rust library.","topics":["rust"],"evidence_excerpt":"Rust library","readme_evidence":"present"}"#;
pub(crate) const REPOSITORY_ANALYSIS_ABSENT: &str = r#"{"summary":"A Rust library.","topics":["rust"],"evidence_excerpt":"Rust library","readme_evidence":"absent"}"#;
pub(crate) const README: &[u8] = b"# Quokkas\nA Rust library for quokkas.\n";
pub(crate) const CONTRACT_CONVERSATION: &str = include_str!(
    "../../../../crates/knowledge/tests/fixtures/ai_archive.conversation.added.v1/claude-added.json"
);
pub(crate) const CONTRACT_TOMBSTONE: &str = include_str!(
    "../../../../crates/knowledge/tests/fixtures/ai_archive.subject.tombstoned.v1/conversation-tombstoned.json"
);

pub(crate) const CAPTURED: &str = "social.source.captured.v1";
pub(crate) const REMOVED: &str = "social.source.removed.v1";
pub(crate) const CONVERSATION_ADDED: &str = "ai_archive.conversation.added.v1";
pub(crate) const REPOSITORY_REQUESTED: &str = "knowledge.repository_analysis.requested.v1";
pub(crate) const EVENT_A: &str = "018f0000-0000-7000-8000-000000000b11";
pub(crate) const EVENT_B: &str = "018f0000-0000-7000-8000-000000000b12";
pub(crate) const EVENT_C: &str = "018f0000-0000-7000-8000-000000000b13";
pub(crate) const EVENT_D: &str = "018f0000-0000-7000-8000-000000000b14";

/// Answers each analysis family with its own canned response, whatever the call order.
#[derive(Debug)]
pub(crate) struct RoutedProvider {
    answers: Vec<(&'static str, &'static str)>,
}

impl LlmProvider for RoutedProvider {
    fn identity(&self) -> ProviderIdentity {
        ProviderIdentity {
            provider: "scripted_fake".to_owned(),
            model: "fake_default_v1".to_owned(),
        }
    }

    fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> impl Future<Output = Result<ProviderResponse, ProviderFailure>> + Send {
        let answer = self
            .answers
            .iter()
            .find(|(prompt, _)| *prompt == request.prompt_version)
            .map(|(_, body)| *body);
        std::future::ready(match answer {
            Some(body) => Ok(response(body)),
            None => Err(ProviderFailure::from(ProviderError::Permanent)),
        })
    }
}

pub(crate) fn routed(answers: &[(&'static str, &'static str)]) -> Arc<RoutedProvider> {
    Arc::new(RoutedProvider {
        answers: answers.to_vec(),
    })
}

pub(crate) fn response(body: &str) -> ProviderResponse {
    ProviderResponse {
        bytes: body.as_bytes().to_vec(),
        request_id: Some("scripted-ingest".to_owned()),
        usage: ProviderUsage {
            input_tokens: 20,
            output_tokens: 10,
        },
    }
}

/// An event envelope as a producer publishes it; `tenant` is omitted when `None`.
pub(crate) fn envelope(
    event_id: &str,
    event_type: &str,
    producer: &str,
    tenant: Option<&str>,
    payload: &serde_json::Value,
) -> Vec<u8> {
    let mut value = serde_json::json!({
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": "2026-08-27T06:00:00Z",
        "producer": producer,
        "aggregate_id": "social_source:018f0000-0000-7000-8000-000000000201",
        "correlation_id": "operation:018f0000-0000-7000-8000-000000000b01",
        "schema_version": 1,
        "payload": payload,
    });
    if let (Some(tenant), Some(object)) = (tenant, value.as_object_mut()) {
        object.insert("tenant_id".to_owned(), serde_json::json!(tenant));
    }
    serde_json::to_vec(&value).unwrap_or_default()
}

pub(crate) fn social_payload(captured_at: &str) -> Result<serde_json::Value, serde_json::Error> {
    let mut source: serde_json::Value = serde_json::from_str(SOCIAL)?;
    if let Some(object) = source.as_object_mut() {
        object.insert("captured_at".to_owned(), serde_json::json!(captured_at));
    }
    Ok(serde_json::json!({ "source": source }))
}

pub(crate) fn removal_payload(removed_at: &str) -> serde_json::Value {
    serde_json::json!({
        "social_source_id": "018f0000-0000-7000-8000-000000000201",
        "owner": OWNER,
        "reason": "user_requested",
        "removed_at": removed_at,
    })
}

pub(crate) fn extracted_document() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
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
    Ok(serde_json::to_value(payload)?)
}

/// A repository-analysis request for `owner`.
///
/// With `readme` the request references those bytes at the GitHub-owned README route; without it
/// the request states that the repository has no README, so no byte source is consulted.
pub(crate) fn repository_request(
    owner: &str,
    readme: Option<&[u8]>,
) -> Result<RepositoryAnalysisRequested, Box<dyn std::error::Error>> {
    let readme = match readme {
        Some(bytes) => ReadmeRevision::Present {
            content_ref: BlobRef {
                owner_service: BlobOwner::parse("ratatoskr-github")?,
                digest: ContentDigest {
                    algorithm: DigestAlgorithm::Sha256,
                    hex: DigestHex::parse(&readme_hex(bytes))?,
                },
                media_type: MediaType::parse("text/markdown")?,
                length_bytes: u64::try_from(bytes.len())?,
            },
        },
        None => ReadmeRevision::Absent {
            reason: ReadmeAbsenceReason::NotFound,
        },
    };
    Ok(RepositoryAnalysisRequested {
        owner: owner.parse::<TenantRef>()?,
        repository_id: RepositoryId::parse("018f0000-0000-7000-8000-000000000701")?,
        github_repository_numeric_id: 42,
        request_id: RepositoryAnalysisRequestId::parse("018f0000-0000-7000-8000-000000000702")?,
        source_revision: RepositoryAnalysisRevision {
            attributes_digest: repeated_digest('a')?,
            readme,
        },
        repository_attributes: RepositoryAnalysisAttributes {
            repository_full_name: RepositoryFullName::parse("owner/quokkas")?,
            description: Some("A Rust library for quokkas.".parse()?),
            primary_language: None,
        },
        requested_contract: RepositoryAnalysisContract::RepositoryAnalysis,
        idempotency_key: repeated_digest('b')?,
        extensions: Extensions::new(),
    })
}

/// The lowercase SHA-256 of `bytes`.
pub(crate) fn readme_hex(bytes: &[u8]) -> String {
    format!("{:x}", sha2::Sha256::digest(bytes))
}

fn repeated_digest(digit: char) -> Result<ContentDigest, Box<dyn std::error::Error>> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    })
}

/// One request the stub observed.
#[derive(Debug, Clone)]
pub(crate) struct StubRequest {
    pub(crate) path: String,
    pub(crate) authorization: Option<String>,
}

/// A loopback HTTP server that answers by request path: the configured status and body for a
/// known path, `404` for any other. Unlike an ordered script, a probe that arrives at an
/// unexpected moment cannot consume another route's reply.
#[derive(Debug)]
pub(crate) struct HttpStub {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<StubRequest>>>,
    accept: tokio::task::JoinHandle<()>,
}

impl HttpStub {
    /// Serves `routes`, each `(path, status, body)`.
    pub(crate) async fn start(routes: Vec<(String, u16, Vec<u8>)>) -> Result<Self, std::io::Error> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let routes: Arc<HashMap<String, (u16, Vec<u8>)>> = Arc::new(
            routes
                .into_iter()
                .map(|(path, status, body)| (path, (status, body)))
                .collect(),
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let accept = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let routes = Arc::clone(&routes);
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let _ignored = serve(stream, &routes, &seen).await;
                });
            }
        });
        Ok(Self {
            address,
            requests,
            accept,
        })
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    /// Every request seen so far, in arrival order.
    pub(crate) fn requests(&self) -> Vec<StubRequest> {
        self.requests
            .lock()
            .map(|requests| requests.clone())
            .unwrap_or_default()
    }

    /// How many requests asked for `path`.
    pub(crate) fn hits(&self, path: &str) -> usize {
        self.requests()
            .iter()
            .filter(|request| request.path == path)
            .count()
    }
}

impl Drop for HttpStub {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn serve(
    mut stream: tokio::net::TcpStream,
    routes: &HashMap<String, (u16, Vec<u8>)>,
    seen: &Mutex<Vec<StubRequest>>,
) -> Result<(), std::io::Error> {
    let mut head = Vec::new();
    let mut buffer = [0_u8; 2_048];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buffer).await?;
        if read == 0 || head.len() > 65_536 {
            return Ok(());
        }
        head.extend_from_slice(buffer.get(..read).unwrap_or_default());
    }
    let text = String::from_utf8_lossy(&head).into_owned();
    let path = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or_default()
        .to_owned();
    let authorization = text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim()
            .eq_ignore_ascii_case("authorization")
            .then(|| value.trim().to_owned())
    });
    if let Ok(mut requests) = seen.lock() {
        requests.push(StubRequest {
            path: path.clone(),
            authorization,
        });
    }
    let (status, body) = routes
        .get(&path)
        .map_or((404, Vec::new()), |(status, body)| (*status, body.clone()));
    let head = format!(
        "HTTP/1.1 {status} STUB\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}
