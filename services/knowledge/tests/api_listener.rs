//! XR-021 CONTRACTS.md R2-01: the real service binds a dedicated API listener beside the operator
//! listener, and each listener serves only its own routes.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ratatoskr_knowledge::test_support::TestDatabase;

/// One HTTP reply: status line code and body text.
struct Reply {
    status: u16,
    body: String,
}

#[tokio::test]
async fn api_listener_serves_the_library_surface_and_the_operator_listener_does_not()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let database_name: String = sqlx::query_scalar("select current_database()")
        .fetch_one(database.database.pool())
        .await?;
    let database_url = test_database_url(&database_name)?;
    let blob_root = std::env::temp_dir().join(format!("knowledge-api-listener-{database_name}"));
    std::fs::create_dir_all(&blob_root)?;

    let operator_reserved = TcpListener::bind("127.0.0.1:0")?;
    let api_reserved = TcpListener::bind("127.0.0.1:0")?;
    let operator = operator_reserved.local_addr()?;
    let api = api_reserved.local_addr()?;
    drop(operator_reserved);
    drop(api_reserved);
    let mut child = Command::new(env!("CARGO_BIN_EXE_ratatoskr-knowledge-service"))
        .env("RATATOSKR__ADMIN__LISTEN_ADDRESS", operator.to_string())
        .env("RATATOSKR__API__LISTEN_ADDRESS", api.to_string())
        .env("RATATOSKR__STORAGE__DATABASE_URL", &database_url)
        .env("RATATOSKR__STORAGE__BLOB_ROOT", &blob_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let outcome = exercise(&mut child, operator, api);
    let stopped = stop_process(&mut child);

    let _ignored = std::fs::remove_dir_all(blob_root);
    database.cleanup().await?;
    outcome?;
    stopped
}

fn exercise(
    child: &mut Child,
    operator: SocketAddr,
    api: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_until_ready(child, operator)?;

    let capabilities = request(api, "GET", "/v1/capabilities", None)?;
    assert_eq!(capabilities.status, 200);
    let document: serde_json::Value = serde_json::from_str(&capabilities.body)?;
    assert_eq!(
        document,
        serde_json::json!({
            "service": "knowledge",
            "capabilities": ["library.search", "library.read_state"]
        })
    );

    let search = request(
        api,
        "GET",
        "/internal/search?tenant=api-listener&q=none",
        None,
    )?;
    assert_eq!(search.status, 200);
    let page: serde_json::Value = serde_json::from_str(&search.body)?;
    assert_eq!(page.get("results"), Some(&serde_json::json!([])));
    assert_eq!(
        request(api, "GET", "/internal/search?q=none", None)?.status,
        400
    );

    let command = request(
        api,
        "POST",
        "/internal/user-content/command",
        Some(r#"{"operation":"create_tag","tenant":"user:api-listener","name":"Reachable"}"#),
    )?;
    assert_eq!(command.status, 200, "{}", command.body);
    assert_eq!(
        request(
            api,
            "GET",
            "/internal/user-content/collection?tenant=user:api-listener\
             &collection_id=00000000-0000-7000-8000-000000000001",
            None,
        )?
        .status,
        404
    );

    for operator_only in ["/live", "/ready", "/metrics", "/version"] {
        assert_eq!(
            request(api, "GET", operator_only, None)?.status,
            404,
            "{operator_only} must not be served on the API listener"
        );
        assert_eq!(
            request(operator, "GET", operator_only, None)?.status,
            200,
            "{operator_only} must be served on the operator listener"
        );
    }
    for library in [
        "/v1/capabilities",
        "/internal/search?tenant=api-listener&q=none",
    ] {
        assert_eq!(
            request(operator, "GET", library, None)?.status,
            404,
            "{library} must not be served on the operator listener"
        );
    }
    assert_eq!(
        request(
            operator,
            "POST",
            "/internal/user-content/command",
            Some(r#"{"operation":"create_tag","tenant":"user:api-listener","name":"Moved"}"#),
        )?
        .status,
        404
    );
    Ok(())
}

fn wait_until_ready(
    child: &mut Child,
    operator: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(format!("process exited before readiness: {status}").into());
        }
        if request(operator, "GET", "/ready", None).is_ok_and(|reply| reply.status == 200) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("readiness did not arrive".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<Reply, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let body = body.unwrap_or_default();
    let framing = if body.is_empty() {
        String::new()
    } else {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        )
    };
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{framing}Connection: close\r\n\r\n{body}"
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
    Ok(Reply { status, body })
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
    let deadline = Instant::now() + Duration::from_secs(5);
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
