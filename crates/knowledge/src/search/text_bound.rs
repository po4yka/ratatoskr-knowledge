//! The bound on the text one search document indexes.

use super::SearchDocumentProjection;

/// Most bytes of title, lead and body that one search document indexes.
///
/// `PostgreSQL` refuses a `tsvector` of 1 MiB or more, and the vector of distinct short words is up
/// to about 2.3 times the size of its text, so an unbounded body would make the whole analysis
/// fail for a long AI-archive conversation (a fact may be as large as the 8 MiB broker payload,
/// XR-021 CONTRACTS.md R5-03). The cut is deterministic: title first, then lead, then body, each at
/// a character boundary. The analysis itself reads its own bounded context, not this projection.
pub const SEARCH_TEXT_MAX_BYTES: usize = 300_000;

impl SearchDocumentProjection {
    /// The title, lead and body that are stored and indexed, bounded by
    /// [`SEARCH_TEXT_MAX_BYTES`] over the three together.
    pub(super) fn bounded_text(&self) -> (&str, &str, &str) {
        let (title, remaining) = cut_at_boundary(&self.title, SEARCH_TEXT_MAX_BYTES);
        let (lead, remaining) = cut_at_boundary(&self.lead, remaining);
        let (body, _remaining) = cut_at_boundary(&self.body, remaining);
        (title, lead, body)
    }
}

/// The longest prefix of `text` of at most `limit` bytes that ends at a character boundary, and
/// the budget left after it.
fn cut_at_boundary(text: &str, limit: usize) -> (&str, usize) {
    let end = text.floor_char_boundary(limit.min(text.len()));
    (text.get(..end).unwrap_or_default(), limit - end)
}
