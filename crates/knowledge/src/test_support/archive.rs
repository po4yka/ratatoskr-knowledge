//! Fixtures for AI-archive conversations that carry the digest the contract computes.

use ratatoskr_ai_archive_contracts::AiConversation;

/// Parses a conversation fixture and sets the content digest the contract computes for its
/// messages, the only digest a producer may publish.
///
/// # Errors
///
/// Returns an error when `json` is not a conversation or its messages cannot be encoded.
pub fn conversation_from_json(json: &str) -> Result<AiConversation, Box<dyn std::error::Error>> {
    let mut conversation: AiConversation = serde_json::from_str(json)?;
    conversation.content_digest = AiConversation::compute_content_digest(&conversation.messages)?;
    Ok(conversation)
}
