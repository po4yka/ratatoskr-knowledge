//! Deterministic article chunking policy `article-chunks.v1`.
//!
//! The policy is a pure function over the projected `(title, lead, body)`
//! text: it normalizes line endings, splits the body into paragraphs on
//! blank lines, greedily packs paragraphs into character-bounded windows,
//! hard-splits any oversized paragraph on char boundaries, and prepends the
//! final configured overlap characters of the previous raw window onto the
//! next. Every stored chunk text is `title + "\n\n" + window`, recorded with
//! its lowercase SHA-256 digest. No clock, randomness, or map iteration
//! order participates, so identical input always yields an identical chunk
//! sequence under one policy value.

use sha2::{Digest, Sha256};

/// Immutable identifier of the chunking policy implemented by this module.
///
/// The constant is compiled into the writer and stamped on every stored
/// embedding chunk row, so changing the policy semantics requires changing
/// this identifier instead of mutating historical behavior.
pub const CHUNKING_VERSION: &str = "article-chunks.v1";

/// Separator placed between paragraphs inside one window and between the
/// title and the window in the stored chunk text.
const PARAGRAPH_SEPARATOR: &str = "\n\n";

/// Error returned when a [`ChunkPolicy`] would be constructed with invalid
/// bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChunkPolicyError {
    /// The configured target size was zero.
    #[error("chunk target_chars must be greater than zero")]
    ZeroTargetChars,
    /// The configured overlap size was zero.
    #[error("chunk overlap_chars must be greater than zero")]
    ZeroOverlapChars,
    /// The configured overlap was not strictly smaller than the target.
    #[error("chunk overlap_chars ({overlap}) must be less than target_chars ({target})")]
    OverlapAtLeastTarget {
        /// Configured maximum window size in characters.
        target: usize,
        /// Configured carried-overlap size in characters.
        overlap: usize,
    },
}

/// Windowing bounds for [`chunk_article`].
///
/// Construct one through [`ChunkPolicy::new`]; the fields stay private so
/// every policy value satisfies the validated bounds invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkPolicy {
    /// Maximum number of characters packed into one window.
    target_chars: usize,
    /// Number of characters carried from the previous raw window onto the
    /// next one.
    overlap_chars: usize,
}

impl ChunkPolicy {
    /// Creates a policy with the given window target and overlap, both in
    /// characters.
    ///
    /// # Errors
    ///
    /// - [`ChunkPolicyError::ZeroTargetChars`] when `target_chars` is zero.
    /// - [`ChunkPolicyError::ZeroOverlapChars`] when `overlap_chars` is zero.
    /// - [`ChunkPolicyError::OverlapAtLeastTarget`] when `overlap_chars` is
    ///   not strictly less than `target_chars`.
    pub fn new(target_chars: usize, overlap_chars: usize) -> Result<Self, ChunkPolicyError> {
        if target_chars == 0 {
            return Err(ChunkPolicyError::ZeroTargetChars);
        }
        if overlap_chars == 0 {
            return Err(ChunkPolicyError::ZeroOverlapChars);
        }
        if overlap_chars >= target_chars {
            return Err(ChunkPolicyError::OverlapAtLeastTarget {
                target: target_chars,
                overlap: overlap_chars,
            });
        }
        Ok(Self {
            target_chars,
            overlap_chars,
        })
    }

    /// Configured maximum window size in characters.
    #[must_use]
    pub const fn target_chars(&self) -> usize {
        self.target_chars
    }

    /// Configured carried-overlap size in characters.
    #[must_use]
    pub const fn overlap_chars(&self) -> usize {
        self.overlap_chars
    }
}

/// One stored chunk of a chunked article.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// Zero-based position of this chunk in the chunk sequence.
    pub ordinal: usize,
    /// Stored text: the title, a blank line, then the window text.
    pub text: String,
    /// Lowercase hex SHA-256 digest of [`Chunk::text`].
    pub digest_hex: String,
}

/// Chunks the projected `(title, lead, body)` text of one accepted article
/// analysis under the given policy.
///
/// The body is the paragraph source; when it is empty the lead paragraphs
/// are used instead, and when both are empty the result is one chunk
/// carrying the title only. Ordinals are zero-based and stable for a given
/// input and policy.
///
/// The function never panics and performs no I/O; identical arguments always
/// produce an identical sequence.
#[must_use]
pub fn chunk_article(title: &str, lead: &str, body: &str, policy: ChunkPolicy) -> Vec<Chunk> {
    let normalized_body = normalize_line_endings(body);
    let source = if normalized_body.is_empty() {
        normalize_line_endings(lead)
    } else {
        normalized_body
    };

    let paragraphs = paragraphs_of(&source);
    let windows = if paragraphs.is_empty() {
        vec![String::new()]
    } else {
        pack_windows(&paragraphs, policy.target_chars)
    };

    let mut chunks = Vec::with_capacity(windows.len());
    let mut previous_raw: Option<String> = None;
    for (ordinal, window) in windows.into_iter().enumerate() {
        let carried = previous_raw.as_deref().map_or(String::new(), |previous| {
            tail_chars(previous, policy.overlap_chars)
        });
        let stored = format!("{title}{PARAGRAPH_SEPARATOR}{carried}{window}");
        let digest_hex = sha256_hex(stored.as_bytes());
        chunks.push(Chunk {
            ordinal,
            text: stored,
            digest_hex,
        });
        previous_raw = Some(window);
    }
    chunks
}

/// Replaces CRLF line endings with plain LF so paragraph detection is stable
/// across source projections.
fn normalize_line_endings(text: &str) -> String {
    text.replace("\r\n", "\n")
}

/// Splits a paragraph source on blank lines, trimming each paragraph and
/// dropping the empty segments that consecutive blank lines produce.
fn paragraphs_of(source: &str) -> Vec<&str> {
    source
        .split(PARAGRAPH_SEPARATOR)
        .map(str::trim)
        .filter(|paragraph| !paragraph.is_empty())
        .collect()
}

/// Splits one oversized paragraph into pieces of at most `target_chars`
/// characters each, measured in characters so multibyte text never panics.
fn split_oversized(paragraph: &str, target_chars: usize) -> Vec<String> {
    let chars: Vec<char> = paragraph.chars().collect();
    chars
        .chunks(target_chars)
        .map(|piece| piece.iter().collect::<String>())
        .collect()
}

/// Greedily packs paragraphs into consecutive windows of at most
/// `target_chars` characters, joining packed paragraphs with a blank line.
fn pack_windows(paragraphs: &[&str], target_chars: usize) -> Vec<String> {
    let mut pieces: Vec<String> = Vec::new();
    for paragraph in paragraphs {
        if paragraph.chars().count() > target_chars {
            pieces.extend(split_oversized(paragraph, target_chars));
        } else {
            pieces.push((*paragraph).to_owned());
        }
    }

    let mut windows: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut current_len = 0usize;
    for piece in pieces {
        let piece_len = piece.chars().count();
        if current_len > 0 && current_len + PARAGRAPH_SEPARATOR.len() + piece_len > target_chars {
            windows.push(std::mem::take(&mut current));
            current_len = 0;
        }
        if current_len > 0 {
            current.push_str(PARAGRAPH_SEPARATOR);
            current_len += PARAGRAPH_SEPARATOR.len();
        }
        current.push_str(&piece);
        current_len += piece_len;
    }
    if !current.is_empty() {
        windows.push(current);
    }
    windows
}

/// Returns the final `count` characters of `text`, or all of it when the
/// text is shorter, so the carried overlap never exceeds the previous
/// window.
fn tail_chars(text: &str, count: usize) -> String {
    let total = text.chars().count();
    if total <= count {
        return text.to_owned();
    }
    text.chars().skip(total - count).collect()
}

/// Lowercase hex SHA-256 digest of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::{CHUNKING_VERSION, ChunkPolicy, ChunkPolicyError, chunk_article};

    fn hex_digest(text: &str) -> String {
        use std::fmt::Write as _;

        Sha256::digest(text.as_bytes())
            .iter()
            .fold(String::with_capacity(64), |mut hex, byte| {
                let _ = write!(hex, "{byte:02x}");
                hex
            })
    }

    #[test]
    fn chunking_same_input_yields_identical_sequence() {
        let title = "Determinism Title";
        let lead = "Lead sentence.";
        let body = "First short paragraph.\n\nSecond short paragraph.";
        let policy = ChunkPolicy::new(200, 20).expect("valid policy");

        let first = chunk_article(title, lead, body, policy);
        let second = chunk_article(title, lead, body, policy);

        assert_eq!(first.len(), second.len());
        assert_eq!(first, second, "identical inputs yield identical chunks");
        assert_eq!(
            first.len(),
            1,
            "below-target input yields exactly one chunk"
        );
        let only = first.first().expect("one chunk");
        assert_eq!(only.ordinal, 0);
        assert!(
            only.text.starts_with(title),
            "chunk text begins with the title"
        );
        assert_eq!(only.text, format!("{title}\n\n{body}"));
        assert_eq!(only.digest_hex, hex_digest(&only.text));
        assert_eq!(CHUNKING_VERSION, "article-chunks.v1");
    }

    #[test]
    fn policy_constructor_rejects_invalid_bounds() {
        assert_eq!(
            ChunkPolicy::new(0, 5),
            Err(ChunkPolicyError::ZeroTargetChars)
        );
        assert_eq!(
            ChunkPolicy::new(10, 0),
            Err(ChunkPolicyError::ZeroOverlapChars)
        );
        assert_eq!(
            ChunkPolicy::new(10, 10),
            Err(ChunkPolicyError::OverlapAtLeastTarget {
                target: 10,
                overlap: 10,
            })
        );

        let policy = ChunkPolicy::new(10, 9).expect("overlap below target is valid");
        assert_eq!(policy.target_chars(), 10);
        assert_eq!(policy.overlap_chars(), 9);
    }

    #[test]
    fn empty_body_chunks_the_lead_instead() {
        let policy = ChunkPolicy::new(200, 20).expect("valid policy");

        let chunks = chunk_article("Title", "Lead paragraph.", "", policy);

        assert_eq!(chunks.len(), 1);
        let only = chunks.first().expect("one chunk");
        assert_eq!(only.text, "Title\n\nLead paragraph.");
    }

    #[test]
    fn empty_body_and_lead_yield_a_single_title_only_chunk() {
        let policy = ChunkPolicy::new(200, 20).expect("valid policy");

        let chunks = chunk_article("Title", "", "", policy);

        assert_eq!(chunks.len(), 1);
        let only = chunks.first().expect("one chunk");
        assert_eq!(only.ordinal, 0);
        assert_eq!(only.text, "Title\n\n");
    }

    #[test]
    fn hard_split_respects_char_boundaries_for_multibyte_text() {
        let paragraph = "日本語の段落です。絵文字🦀も扱います。".repeat(3);
        let policy = ChunkPolicy::new(7, 3).expect("valid policy");

        let chunks = chunk_article("マルチバイト", "", &paragraph, policy);

        assert_eq!(
            chunks.len(),
            9,
            "57 characters split into ceil(57 / 7) hard-split pieces"
        );
        let mut raw_pieces: Vec<String> = Vec::new();
        for (expected_ordinal, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.ordinal, expected_ordinal);
            let window = chunk
                .text
                .strip_prefix("マルチバイト\n\n")
                .expect("every chunk carries the title prefix");
            assert!(
                window.chars().count() <= 7 + 3,
                "window stays within the target plus overlap allowance"
            );
            let chars: Vec<char> = window.chars().collect();
            let (carried, raw): (String, String) = if expected_ordinal == 0 {
                (String::new(), chars.into_iter().collect())
            } else {
                (
                    chars.iter().take(3).collect(),
                    chars.into_iter().skip(3).collect(),
                )
            };
            if expected_ordinal > 0 {
                let previous = &raw_pieces[expected_ordinal - 1];
                let expected_carry: String = previous
                    .chars()
                    .skip(previous.chars().count() - 3)
                    .collect();
                assert_eq!(
                    carried, expected_carry,
                    "each carried overlap equals the previous raw piece's tail"
                );
            }
            assert!(
                raw.chars().count() <= 7,
                "hard-split piece stays within the target"
            );
            raw_pieces.push(raw);
        }
        let rejoined: String = raw_pieces.concat();
        assert_eq!(rejoined, paragraph, "the hard split loses no characters");
    }

    #[test]
    fn chunking_respects_target_and_overlap_bounds() {
        let title = "Bounds Title";
        let paragraphs: Vec<String> = (0..6)
            .map(|i| format!("{}end{i:02}", "x".repeat(15)))
            .collect();
        let body = paragraphs.join("\n\n");
        let wide = ChunkPolicy::new(50, 6).expect("valid policy");

        let chunks = chunk_article(title, "", &body, wide);

        assert_eq!(
            chunks.len(),
            3,
            "two 20-char paragraphs fit one 50-char window"
        );
        let windows: Vec<&str> = chunks
            .iter()
            .map(|chunk| {
                chunk
                    .text
                    .strip_prefix(&format!("{title}\n\n"))
                    .expect("every chunk carries the title prefix")
            })
            .collect();
        for (ordinal, window) in windows.iter().enumerate() {
            let allowance = if ordinal == 0 { 50 } else { 50 + 6 };
            assert!(
                window.chars().count() <= allowance,
                "window {ordinal} exceeds the target plus overlap allowance"
            );
        }
        assert_eq!(
            windows[0],
            format!("{}\n\n{}", paragraphs[0], paragraphs[1])
        );
        assert_eq!(
            windows[1],
            format!("{}{}\n\n{}", "xend01", paragraphs[2], paragraphs[3]),
            "the second window carries the previous raw window's final characters"
        );
        assert_eq!(
            windows[2],
            format!("{}{}\n\n{}", "xend03", paragraphs[4], paragraphs[5])
        );

        let narrow = ChunkPolicy::new(25, 4).expect("valid policy");
        let shrunk = chunk_article(title, "", &body, narrow);

        assert_eq!(shrunk.len(), 6, "shrinking the target splits per paragraph");
        for (ordinal, chunk) in shrunk.iter().enumerate() {
            let window = chunk
                .text
                .strip_prefix(&format!("{title}\n\n"))
                .expect("every chunk carries the title prefix");
            assert!(window.chars().count() <= 25 + 4);
            if ordinal > 0 {
                let previous = &paragraphs[ordinal - 1];
                let carry: String = previous
                    .chars()
                    .skip(previous.chars().count() - 4)
                    .collect();
                assert!(
                    window.starts_with(&carry),
                    "window {ordinal} must start with the carried overlap {carry:?}"
                );
            }
        }
    }
}
