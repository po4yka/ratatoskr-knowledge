//! Real process startup test.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ratatoskr_knowledge::test_support::TestDatabase;

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
