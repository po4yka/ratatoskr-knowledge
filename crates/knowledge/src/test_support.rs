//! Disposable database and transport support for integration tests.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use sqlx::Executor as _;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::{Database, PersistenceError};

/// One scripted HTTP reply served by [`FakeTransport`].
#[derive(Debug, Clone)]
pub struct FakeReply {
    status: u16,
    body: FakeBody,
}

/// Scripted response body behaviors.
#[derive(Debug, Clone)]
pub enum FakeBody {
    /// A complete body sent with an exact content length.
    Bytes(Vec<u8>),
    /// Streams the total byte count in chunks, then abandons the connection.
    Oversized {
        /// Total body bytes the fake claims and streams before abandoning.
        total_bytes: usize,
    },
    /// Sends response headers and never sends a body byte.
    Stall,
}

impl FakeReply {
    /// Builds one complete reply.
    #[must_use]
    pub fn bytes(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            body: FakeBody::Bytes(body),
        }
    }

    /// Builds a reply that streams more bytes than any sane cap.
    #[must_use]
    pub fn oversized(total_bytes: usize) -> Self {
        Self {
            status: 200,
            body: FakeBody::Oversized { total_bytes },
        }
    }

    /// Builds a reply whose body never arrives.
    #[must_use]
    pub fn stall() -> Self {
        Self {
            status: 200,
            body: FakeBody::Stall,
        }
    }
}

/// One request observed by the fake transport.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    /// Request path including any base path.
    pub path: String,
    /// Authorization header value exactly as received.
    pub authorization: Option<String>,
    /// Lower-cased request headers exactly as received by the fake.
    pub headers: BTreeMap<String, String>,
    /// Received body byte count.
    pub body_bytes: usize,
}

/// Loopback HTTP/1.1 fake provider transport for offline tests.
#[derive(Debug)]
pub struct FakeTransport {
    local_addr: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl FakeTransport {
    /// Starts the fake transport with one scripted reply per request.
    ///
    /// Requests beyond the scripted replies receive an empty server-fault
    /// response, so flaky transports need no extra scripting.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when no loopback port can be bound.
    pub async fn start(replies: Vec<FakeReply>) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let local_addr = listener.local_addr()?;
        let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let scripted: Arc<Mutex<VecDeque<FakeReply>>> = Arc::new(Mutex::new(replies.into()));
        let accept_requests = Arc::clone(&requests);
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let reply_source = Arc::clone(&scripted);
                let request_sink = Arc::clone(&accept_requests);
                tokio::spawn(async move {
                    let _ignored = serve(stream, reply_source, request_sink).await;
                });
            }
        });
        Ok(Self {
            local_addr,
            requests,
            accept_task,
        })
    }

    /// Returns the loopback address the transport listens on.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns how many requests the transport has observed.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the capture lock was poisoned.
    pub fn request_count(&self) -> Result<usize, std::io::Error> {
        let requests = self.requests.lock().map_err(poison_error)?;
        Ok(requests.len())
    }

    /// Returns every observed request in arrival order.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the capture lock was poisoned.
    pub fn recorded(&self) -> Result<Vec<RecordedRequest>, std::io::Error> {
        let requests = self.requests.lock().map_err(poison_error)?;
        Ok(requests.clone())
    }
}

impl Drop for FakeTransport {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn serve(
    mut stream: tokio::net::TcpStream,
    scripted: Arc<Mutex<VecDeque<FakeReply>>>,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
) -> Result<(), std::io::Error> {
    let head_end = b"\r\n\r\n";
    let mut head = Vec::new();
    let mut buffer = [0_u8; 2_048];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        let received = buffer.get(..read).ok_or_else(head_error)?;
        head.extend_from_slice(received);
        if let Some(position) = find_subsequence(&head, head_end) {
            let head_bytes = head.get(..position).ok_or_else(head_error)?;
            let head_text = String::from_utf8_lossy(head_bytes).into_owned();
            let body_length = content_length(&head_text);
            let mut body = head
                .get(position + head_end.len()..)
                .ok_or_else(head_error)?
                .to_vec();
            while body.len() < body_length {
                let read = stream.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                let received = buffer.get(..read).ok_or_else(head_error)?;
                body.extend_from_slice(received);
            }
            let authorization = header_value(&head_text, "authorization");
            let headers = request_headers(&head_text);
            let path = request_path(&head_text);
            requests
                .lock()
                .map_err(poison_error)?
                .push(RecordedRequest {
                    path,
                    authorization,
                    headers,
                    body_bytes: body.len(),
                });
            let reply = scripted
                .lock()
                .map_err(poison_error)?
                .pop_front()
                .unwrap_or_else(|| FakeReply::bytes(500, Vec::new()));
            return write_reply(&mut stream, &reply).await;
        }
        if head.len() > 65_536 {
            return Ok(());
        }
    }
}

fn request_headers(head_text: &str) -> BTreeMap<String, String> {
    head_text
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect()
}

async fn write_reply(
    stream: &mut tokio::net::TcpStream,
    reply: &FakeReply,
) -> Result<(), std::io::Error> {
    match &reply.body {
        FakeBody::Bytes(bytes) => {
            let head = format!(
                "HTTP/1.1 {} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                reply.status,
                bytes.len()
            );
            stream.write_all(head.as_bytes()).await?;
            stream.write_all(bytes).await?;
            stream.flush().await
        }
        FakeBody::Oversized { total_bytes } => {
            let head = format!(
                "HTTP/1.1 {} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                reply.status, total_bytes
            );
            stream.write_all(head.as_bytes()).await?;
            stream.flush().await?;
            let chunk = vec![b'x'; 1_024];
            let mut written = 0_usize;
            while written < *total_bytes {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let size = chunk.len().min(*total_bytes - written);
                let part = chunk.get(..size).ok_or_else(head_error)?;
                stream.write_all(part).await?;
                stream.flush().await?;
                written += size;
            }
            Ok(())
        }
        FakeBody::Stall => {
            let head = format!(
                "HTTP/1.1 {} OK\r\ncontent-type: application/json\r\n\r\n",
                reply.status
            );
            stream.write_all(head.as_bytes()).await?;
            stream.flush().await?;
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            Ok(())
        }
    }
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn request_path(head_text: &str) -> String {
    head_text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or_default()
        .to_owned()
}

fn header_value(head_text: &str, name: &str) -> Option<String> {
    head_text.lines().find_map(|line| {
        let (header, value) = line.split_once(':')?;
        if header.trim().eq_ignore_ascii_case(name) {
            Some(value.trim().to_owned())
        } else {
            None
        }
    })
}

fn content_length(head_text: &str) -> usize {
    header_value(head_text, "content-length")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn head_error() -> std::io::Error {
    std::io::Error::other("fake transport read out of bounds")
}

fn poison_error<T>(_: std::sync::PoisonError<T>) -> std::io::Error {
    std::io::Error::other("fake transport capture lock was poisoned")
}

/// Temporary Knowledge-owned blob root.
#[derive(Debug)]
pub struct TemporaryBlobRoot {
    path: std::path::PathBuf,
}

/// An isolated disposable Knowledge database.
#[derive(Debug)]
pub struct TestDatabase {
    /// Connected Knowledge database.
    pub database: Database,
    name: String,
}

impl TestDatabase {
    /// Creates an empty isolated database.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when database creation or connection fails.
    pub async fn create() -> Result<Self, PersistenceError> {
        let name = format!("knowledge_test_{}", Uuid::now_v7().simple());
        let admin_url = admin_url();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .map_err(PersistenceError::Connect)?;
        admin
            .execute(format!(r#"create database "{name}""#).as_str())
            .await
            .map_err(PersistenceError::Query)?;
        admin.close().await;

        let options = admin_url
            .parse::<PgConnectOptions>()
            .map_err(PersistenceError::Connect)?
            .database(&name);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .map_err(PersistenceError::Connect)?;
        let database = Database::from_pool(pool);
        database.apply_schema().await?;
        Ok(Self { database, name })
    }

    /// Closes and drops the database.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when cleanup fails.
    pub async fn cleanup(self) -> Result<(), PersistenceError> {
        self.database.close().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url())
            .await
            .map_err(PersistenceError::Connect)?;
        admin
            .execute(format!(r#"drop database if exists "{}" with (force)"#, self.name).as_str())
            .await
            .map_err(PersistenceError::Query)?;
        admin.close().await;
        Ok(())
    }
}

impl TemporaryBlobRoot {
    /// Creates a unique empty blob root.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the root cannot be created.
    pub async fn create() -> Result<Self, std::io::Error> {
        let path = std::env::temp_dir().join(format!("ratatoskr-knowledge-{}", Uuid::now_v7()));
        tokio::fs::create_dir_all(&path).await?;
        Ok(Self { path })
    }

    /// Returns the root path.
    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TemporaryBlobRoot {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.path);
    }
}

fn admin_url() -> String {
    match std::env::var("KNOWLEDGE_TEST_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => "postgres://extractor:extractor@127.0.0.1:5434/extractor".to_owned(),
    }
}
