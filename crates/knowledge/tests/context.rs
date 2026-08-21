//! Deterministic Document IR context tests.

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock, LanguageTag};
use ratatoskr_identifiers::{ContentDigest, DigestAlgorithm, DigestHex, DocumentId};
use ratatoskr_knowledge::prepare_context;

#[test]
fn builder_is_deterministic_and_omits_only_complete_tail_blocks()
-> Result<(), Box<dyn std::error::Error>> {
    let document = document()?;
    let first = prepare_context(&document, 100)?;
    let second = prepare_context(&document, 100)?;

    assert_eq!(first, second);
    assert_eq!(first.included_block_indexes, [0, 1]);
    assert_eq!(first.omitted_block_indexes, [2]);
    assert!(first.truncated);
    assert!(first.source.contains("Heading"));
    assert!(first.source.contains("Short paragraph."));
    assert!(
        !first
            .source
            .contains("This complete tail block must be omitted")
    );
    Ok(())
}

fn document() -> Result<Document, ratatoskr_identifiers::IdentifierError> {
    Ok(Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:context-fixture")?,
        content_digest: ContentDigest {
            algorithm: DigestAlgorithm::Sha256,
            hex: DigestHex::parse(&"a".repeat(64))?,
        },
        title: Some("Demo".to_owned()),
        language: Some(LanguageTag::parse("en")?),
        blocks: vec![
            DocumentBlock::Heading {
                level: 1,
                text: "Heading".to_owned(),
            },
            DocumentBlock::Paragraph {
                text: "Short paragraph.".to_owned(),
            },
            DocumentBlock::Paragraph {
                text: "This complete tail block must be omitted, never cut.".to_owned(),
            },
        ],
        provenance: Vec::new(),
    })
}
