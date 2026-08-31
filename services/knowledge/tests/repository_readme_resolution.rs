//! Production GitHub README resolver boundary tests.

#![allow(
    clippy::excessive_nesting,
    clippy::indexing_slicing,
    clippy::while_let_loop,
    reason = "the bounded synthetic HTTP server keeps one request transcript visible in the test"
)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ratatoskr_github_contracts::RepositoryAnalysisRequested;
use ratatoskr_identifiers::BlobRef;
use ratatoskr_knowledge::{
    GithubReadmeSettings, GithubRepositoryReadmeResolver, ProviderSecret, RepositoryReadmeError,
    RepositoryReadmeResolver,
};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const TOKEN: &str = "synthetic-knowledge-service-token";

#[tokio::test]
async fn repository_analysis_uses_authenticated_bounded_blob_resolution()
-> Result<(), Box<dyn std::error::Error>> {
    let readme = b"# Immutable README\n".to_vec();
    let server = TestServer::start(vec![
        Reply::new(400, "application/json", Vec::new()),
        Reply::new(200, "text/markdown", readme.clone()),
        Reply::new(503, "application/json", Vec::new()),
        Reply::new(403, "application/json", Vec::new()),
        Reply::new(413, "application/json", Vec::new()),
        Reply::new(404, "application/json", Vec::new()),
        Reply::new(200, "text/markdown", b"# Corrupt README\n".to_vec()),
    ])
    .await?;
    let (request, reference) = request(&readme)?;
    let resolver = GithubRepositoryReadmeResolver::new(GithubReadmeSettings {
        base_url: reqwest::Url::parse(&format!("http://{}/", server.address))?,
        service_token: ProviderSecret::new(TOKEN.to_owned()),
        timeout: Duration::from_secs(2),
        response_bytes: 1_048_576,
    })?;
    resolver.probe().await?;

    assert_eq!(resolver.read_readme(&request, &reference).await?, readme);
    assert!(matches!(
        resolver.read_readme(&request, &reference).await,
        Err(RepositoryReadmeError::Unavailable)
    ));
    assert!(matches!(
        resolver.read_readme(&request, &reference).await,
        Err(RepositoryReadmeError::Unauthorized)
    ));
    assert!(matches!(
        resolver.read_readme(&request, &reference).await,
        Err(RepositoryReadmeError::Oversized)
    ));
    assert!(matches!(
        resolver.read_readme(&request, &reference).await,
        Err(RepositoryReadmeError::Missing)
    ));
    assert!(matches!(
        resolver.read_readme(&request, &reference).await,
        Err(RepositoryReadmeError::Integrity)
    ));

    let requests = server.requests.lock().map_err(|_| "capture poisoned")?;
    assert_eq!(requests.len(), 7);
    assert!(requests.iter().all(|request| {
        request.starts_with("POST /internal/v1/repository-readmes/resolve HTTP/1.1")
            && request
                .to_ascii_lowercase()
                .contains("authorization: bearer synthetic-knowledge-service-token")
            && !request.contains("file://")
            && !request.contains("http://")
    }));
    Ok(())
}

fn request(
    readme: &[u8],
) -> Result<(RepositoryAnalysisRequested, BlobRef), Box<dyn std::error::Error>> {
    let digest = format!("{:x}", Sha256::digest(readme));
    let value = serde_json::json!({
        "owner": "user:018f0000-0000-7000-8000-000000000005",
        "repository_id": "018f0000-0000-7000-8000-000000000601",
        "github_repository_numeric_id": 42,
        "request_id": "018f0000-0000-7000-8000-000000000602",
        "source_revision": {
            "attributes_digest": {"algorithm": "sha256", "hex": "a".repeat(64)},
            "readme": {"state": "present", "content_ref": {
                "owner_service": "ratatoskr-github",
                "digest": {"algorithm": "sha256", "hex": digest},
                "media_type": "text/markdown",
                "length_bytes": readme.len()
            }}
        },
        "repository_attributes": {"repository_full_name": "owner/repository"},
        "requested_contract": "repository_analysis",
        "idempotency_key": {"algorithm": "sha256", "hex": "b".repeat(64)}
    });
    let request: RepositoryAnalysisRequested = serde_json::from_value(value)?;
    let ratatoskr_github_contracts::ReadmeRevision::Present { content_ref } =
        &request.source_revision.readme
    else {
        return Err("README reference missing".into());
    };
    Ok((request.clone(), content_ref.clone()))
}

#[derive(Debug)]
struct Reply {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl Reply {
    fn new(status: u16, content_type: &'static str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type,
            body,
        }
    }
}

#[derive(Debug)]
struct TestServer {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start(replies: Vec<Reply>) -> Result<Self, std::io::Error> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut head = Vec::new();
                let mut buffer = [0_u8; 2_048];
                loop {
                    let Ok(read) = stream.read(&mut buffer).await else {
                        break;
                    };
                    if read == 0 {
                        break;
                    }
                    head.extend_from_slice(&buffer[..read]);
                    if let Some(end) = head.windows(4).position(|window| window == b"\r\n\r\n") {
                        let head_text = String::from_utf8_lossy(&head[..end]).into_owned();
                        let length = head_text
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                            .unwrap_or_default();
                        let body_start = end + 4;
                        while head.len().saturating_sub(body_start) < length {
                            let Ok(read) = stream.read(&mut buffer).await else {
                                break;
                            };
                            if read == 0 {
                                break;
                            }
                            head.extend_from_slice(&buffer[..read]);
                        }
                        let request = String::from_utf8_lossy(&head).into_owned();
                        if let Ok(mut requests) = captured.lock() {
                            requests.push(request);
                        }
                        let reply = replies
                            .lock()
                            .ok()
                            .and_then(|mut replies| replies.pop_front());
                        if let Some(reply) = reply {
                            let response = format!(
                                "HTTP/1.1 {} Test\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                                reply.status,
                                reply.content_type,
                                reply.body.len()
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                            let _ = stream.write_all(&reply.body).await;
                        }
                        break;
                    }
                }
            }
        });
        Ok(Self {
            address,
            requests,
            _task: task,
        })
    }
}
