//! Captures the log lines a test produces, so a test can assert the class the service logged.
//!
//! The capture is installed on the current thread only, so it sees the spawned workers of a test
//! that runs on the default current-thread runtime and nothing from a test running beside it.

use std::io::Write;
use std::sync::{Arc, Mutex};

use tracing::subscriber::DefaultGuard;

/// The log text recorded since [`capture`] returned.
#[derive(Debug, Clone, Default)]
pub(crate) struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    /// Everything logged so far.
    pub(crate) fn text(&self) -> String {
        self.0
            .lock()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_default()
    }

    /// How many log lines contain `needle`.
    pub(crate) fn lines_containing(&self, needle: &str) -> usize {
        self.text()
            .lines()
            .filter(|line| line.contains(needle))
            .count()
    }
}

struct Sink(Arc<Mutex<Vec<u8>>>);

impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Ok(mut buffer) = self.0.lock() {
            buffer.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Installs the capture for the current thread; it ends when the guard is dropped.
pub(crate) fn capture() -> (Captured, DefaultGuard) {
    let captured = Captured::default();
    let sink = Arc::clone(&captured.0);
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || Sink(Arc::clone(&sink)))
        .finish();
    (captured, tracing::subscriber::set_default(subscriber))
}
