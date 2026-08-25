//! Bounded label-free operator counters rendered as Prometheus text.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use ratatoskr_knowledge::RankingPath;

/// Label-free counters shared by the indexing worker and the operator plane.
///
/// Every field is an independent monotonic counter; no labels exist, so the
/// exposition stays bounded regardless of traffic shape.
#[derive(Debug, Default)]
pub struct Metrics {
    embedding_index_passes: AtomicU64,
    embedding_sources_indexed: AtomicU64,
    embedding_index_failures: AtomicU64,
    search_browse: AtomicU64,
    search_lexical: AtomicU64,
    search_hybrid: AtomicU64,
}

impl Metrics {
    /// Creates zeroed counters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one completed background indexing pass.
    pub fn record_index_pass(&self) {
        self.embedding_index_passes.fetch_add(1, Ordering::Relaxed);
    }

    /// Records sources embedded and persisted during one pass.
    pub fn record_indexed(&self, count: usize) {
        self.embedding_sources_indexed
            .fetch_add(saturate(count), Ordering::Relaxed);
    }

    /// Records sources whose indexing failed during one pass.
    pub fn record_index_failures(&self, count: usize) {
        self.embedding_index_failures
            .fetch_add(saturate(count), Ordering::Relaxed);
    }

    /// Records which ranking path served one search request.
    pub fn record_ranking_path(&self, path: RankingPath) {
        let counter = match path {
            RankingPath::BrowseRecent => &self.search_browse,
            RankingPath::LexicalOnly => &self.search_lexical,
            RankingPath::Hybrid => &self.search_hybrid,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Renders the Prometheus exposition text.
    #[must_use]
    pub fn render(&self) -> String {
        let mut text =
            String::from("# TYPE knowledge_process_info gauge\nknowledge_process_info 1\n");
        push_counter(
            &mut text,
            "embedding_index_passes_total",
            self.embedding_index_passes.load(Ordering::Relaxed),
        );
        push_counter(
            &mut text,
            "embedding_sources_indexed_total",
            self.embedding_sources_indexed.load(Ordering::Relaxed),
        );
        push_counter(
            &mut text,
            "embedding_index_failures_total",
            self.embedding_index_failures.load(Ordering::Relaxed),
        );
        push_counter(
            &mut text,
            "embedding_sources_indexed_total",
            self.embedding_sources_indexed.load(Ordering::Relaxed),
        );
        push_counter(
            &mut text,
            "embedding_index_failures_total",
            self.embedding_index_failures.load(Ordering::Relaxed),
        );
        push_counter(
            &mut text,
            "search_browse_total",
            self.search_browse.load(Ordering::Relaxed),
        );
        push_counter(
            &mut text,
            "search_lexical_total",
            self.search_lexical.load(Ordering::Relaxed),
        );
        push_counter(
            &mut text,
            "search_hybrid_total",
            self.search_hybrid.load(Ordering::Relaxed),
        );
        text
    }
}

fn saturate(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

fn push_counter(text: &mut String, name: &str, value: u64) {
    let _ = writeln!(text, "# TYPE {name} counter\n{name} {value}");
}
