//! Telemetry privacy boundary tests.

use std::sync::{Arc, Mutex};

use ratatoskr_knowledge::{ValidationClass, record_validation_failure};

#[test]
fn validation_telemetry_excludes_source_and_response_text() -> Result<(), std::io::Error> {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(SharedWriter(Arc::clone(&bytes)))
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        record_validation_failure(
            ValidationClass::Schema,
            "source says LEAKME",
            "response says LEAKME",
        );
    });

    let captured = String::from_utf8(bytes.lock().map_err(lock_error)?.clone())
        .map_err(std::io::Error::other)?;
    assert!(captured.contains("schema"));
    assert!(captured.contains("article_analysis"));
    assert!(!captured.contains("LEAKME"));
    Ok(())
}

#[derive(Debug, Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for SharedWriter {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

impl std::io::Write for SharedWriter {
    fn write(&mut self, buffer: &[u8]) -> Result<usize, std::io::Error> {
        let mut bytes = self.0.lock().map_err(lock_error)?;
        bytes.write(buffer)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

fn lock_error<T>(_error: std::sync::PoisonError<T>) -> std::io::Error {
    std::io::Error::other("telemetry capture lock was poisoned")
}
