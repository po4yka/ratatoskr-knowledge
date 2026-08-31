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
    primary_admitted: AtomicU64,
    primary_rejected: AtomicU64,
    primary_collisions: AtomicU64,
    primary_retries: AtomicU64,
    primary_uncertain: AtomicU64,
    outbox_published: AtomicU64,
    outbox_retries: AtomicU64,
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

    /// Records one newly admitted primary fact.
    pub fn record_primary_admitted(&self) {
        self.primary_admitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one content-free permanent primary rejection.
    pub fn record_primary_rejected(&self) {
        self.primary_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one immutable event-id collision.
    pub fn record_primary_collision(&self) {
        self.primary_collisions.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one bounded dependency retry.
    pub fn record_primary_retry(&self) {
        self.primary_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one provider outcome requiring operator review.
    pub fn record_primary_uncertain(&self) {
        self.primary_uncertain.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one broker-acknowledged terminal publication.
    pub fn record_outbox_published(&self) {
        self.outbox_published.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one retained outbox publication retry.
    pub fn record_outbox_retry(&self) {
        self.outbox_retries.fetch_add(1, Ordering::Relaxed);
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
        for (name, value) in [
            (
                "primary_admitted_total",
                self.primary_admitted.load(Ordering::Relaxed),
            ),
            (
                "primary_rejected_total",
                self.primary_rejected.load(Ordering::Relaxed),
            ),
            (
                "primary_collisions_total",
                self.primary_collisions.load(Ordering::Relaxed),
            ),
            (
                "primary_retries_total",
                self.primary_retries.load(Ordering::Relaxed),
            ),
            (
                "primary_uncertain_total",
                self.primary_uncertain.load(Ordering::Relaxed),
            ),
            (
                "outbox_published_total",
                self.outbox_published.load(Ordering::Relaxed),
            ),
            (
                "outbox_retries_total",
                self.outbox_retries.load(Ordering::Relaxed),
            ),
        ] {
            push_counter(&mut text, name, value);
        }
        text
    }
}

fn saturate(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

fn push_counter(text: &mut String, name: &str, value: u64) {
    let _ = writeln!(text, "# TYPE {name} counter\n{name} {value}");
}
