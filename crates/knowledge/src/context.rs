use ratatoskr_document_contracts::{Document, DocumentBlock};

/// Deterministic source context supplied to one provider call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedContext {
    /// Version-one normalized source rendering.
    pub source: String,
    /// Block indexes included in provider-visible order.
    pub included_block_indexes: Vec<u32>,
    /// Complete tail-block indexes omitted from provider input.
    pub omitted_block_indexes: Vec<u32>,
    /// Configured Unicode character budget.
    pub character_budget: usize,
    /// Whether any complete tail block was omitted.
    pub truncated: bool,
}

/// Deterministic context preparation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ContextError {
    /// The finite budget cannot hold required document identity fields.
    #[error("the context character budget is too small")]
    BudgetTooSmall,
    /// The document has more blocks than the shared index type can address.
    #[error("the document has too many blocks")]
    TooManyBlocks,
    /// A Document IR string could not be encoded deterministically.
    #[error("the document context could not be encoded")]
    Encode,
}

/// Prepares version-one provider-visible source context.
///
/// # Errors
///
/// Returns [`ContextError`] when the budget cannot hold required metadata.
pub fn prepare_context(
    document: &Document,
    character_budget: usize,
) -> Result<PreparedContext, ContextError> {
    let title = serde_json::to_string(&document.title).map_err(|_| ContextError::Encode)?;
    let language = serde_json::to_string(
        &document
            .language
            .as_ref()
            .map(ratatoskr_document_contracts::LanguageTag::as_str),
    )
    .map_err(|_| ContextError::Encode)?;
    let mut source = format!("title: {title}\nlanguage: {language}\n");
    if source.chars().count() > character_budget {
        return Err(ContextError::BudgetTooSmall);
    }

    let mut included_block_indexes = Vec::new();
    let mut omitted_block_indexes = Vec::new();
    let mut truncated = false;
    for (position, block) in document.blocks.iter().enumerate() {
        let index = u32::try_from(position).map_err(|_| ContextError::TooManyBlocks)?;
        let Some(rendered) = render_block(index, block)? else {
            truncated = true;
            omitted_block_indexes.push(index);
            continue;
        };
        let fits = source.chars().count() + rendered.chars().count() <= character_budget;
        if truncated || !fits {
            truncated = true;
            omitted_block_indexes.push(index);
        } else {
            source.push_str(&rendered);
            included_block_indexes.push(index);
        }
    }

    Ok(PreparedContext {
        source,
        included_block_indexes,
        omitted_block_indexes,
        character_budget,
        truncated,
    })
}

fn render_block(index: u32, block: &DocumentBlock) -> Result<Option<String>, ContextError> {
    let rendered = match block {
        DocumentBlock::Heading { level, text } => {
            let text = serde_json::to_string(text).map_err(|_| ContextError::Encode)?;
            Some(format!("block {index} heading {level}: {text}\n"))
        }
        DocumentBlock::Paragraph { text } => {
            let text = serde_json::to_string(text).map_err(|_| ContextError::Encode)?;
            Some(format!("block {index} paragraph: {text}\n"))
        }
        _ => None,
    };
    Ok(rendered)
}
