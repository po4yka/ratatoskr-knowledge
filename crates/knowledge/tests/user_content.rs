//! Durable user-content ownership invariants.

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    AnalysisState, CollectionTarget, FeedbackCategory, HighlightStyle, ReadState, UserContentError,
    add_collection_item, create_collection, create_tag, list_collection_items, merge_tags,
    move_collection_item, record_feedback, set_analysis_state, tag_analysis, tag_name,
    validate_highlight_anchor,
};
use uuid::Uuid;

#[tokio::test]
async fn tag_names_are_unique_within_a_tenant_only() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    create_tag(
        database.database.pool(),
        "user:tenant-a",
        tag_name("Research")?,
    )
    .await?;
    assert!(matches!(
        create_tag(
            database.database.pool(),
            "user:tenant-a",
            tag_name(" research ")?
        )
        .await,
        Err(UserContentError::Conflict)
    ));
    create_tag(
        database.database.pool(),
        "user:tenant-b",
        tag_name("Research")?,
    )
    .await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn tag_merge_deduplicates_analysis_taggings() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let (_, output) = seed_accepted(&database, "user:tags").await?;
    let source = create_tag(database.database.pool(), "user:tags", tag_name("old")?).await?;
    let destination = create_tag(database.database.pool(), "user:tags", tag_name("new")?).await?;
    tag_analysis(database.database.pool(), "user:tags", source, output).await?;
    tag_analysis(database.database.pool(), "user:tags", destination, output).await?;
    merge_tags(database.database.pool(), "user:tags", source, destination).await?;
    let count: i64 =
        sqlx::query_scalar("select count(*) from knowledge.analysis_taggings where output_id=$1")
            .bind(output)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(count, 1);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn collection_moves_preserve_unaffected_order() -> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let (first, _) = seed_accepted(&database, "user:collections").await?;
    let (middle, _) = seed_accepted(&database, "user:collections").await?;
    let (last, _) = seed_accepted(&database, "user:collections").await?;
    let collection =
        create_collection(database.database.pool(), "user:collections", "Reading").await?;
    for source in [first, middle, last] {
        add_collection_item(
            database.database.pool(),
            "user:collections",
            collection,
            CollectionTarget::Source(source),
            None,
        )
        .await?;
    }
    move_collection_item(
        database.database.pool(),
        "user:collections",
        collection,
        CollectionTarget::Source(middle),
        2,
    )
    .await?;
    assert_eq!(
        list_collection_items(database.database.pool(), "user:collections", collection).await?,
        vec![
            ratatoskr_knowledge::CollectionItem {
                position: 0,
                target: CollectionTarget::Source(first)
            },
            ratatoskr_knowledge::CollectionItem {
                position: 1,
                target: CollectionTarget::Source(last)
            },
            ratatoskr_knowledge::CollectionItem {
                position: 2,
                target: CollectionTarget::Source(middle)
            },
        ]
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn analysis_state_transitions_are_idempotent_and_tenant_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let (_, output) = seed_accepted(&database, "user:state-a").await?;
    let expected = AnalysisState {
        read_state: ReadState::Read,
        favorite: true,
    };
    assert_eq!(
        set_analysis_state(database.database.pool(), "user:state-a", output, expected).await?,
        expected
    );
    assert_eq!(
        set_analysis_state(database.database.pool(), "user:state-a", output, expected).await?,
        expected
    );
    assert!(matches!(
        set_analysis_state(database.database.pool(), "user:state-b", output, expected).await,
        Err(UserContentError::NotFound)
    ));
    let count: i64 = sqlx::query_scalar(
        "select count(*) from knowledge.analysis_user_states where output_id=$1",
    )
    .bind(output)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(count, 1);
    database.cleanup().await?;
    Ok(())
}

#[test]
fn highlight_rejects_unknown_block_and_out_of_range_unicode_offsets()
-> Result<(), Box<dyn std::error::Error>> {
    let block_id = BlockId::new_v7();
    let document = Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:highlight")?,
        content_digest: ContentDigest {
            algorithm: DigestAlgorithm::Sha256,
            hex: DigestHex::parse(&"a".repeat(64))?,
        },
        title: None,
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            block_id,
            text: "Aé🦊Z".to_owned(),
        }],
        provenance: Vec::new(),
    };
    assert!(validate_highlight_anchor(&document, block_id, 1, 3).is_ok());
    assert!(matches!(
        validate_highlight_anchor(&document, block_id, 1, 5),
        Err(UserContentError::Invalid)
    ));
    assert!(matches!(
        validate_highlight_anchor(&document, BlockId::new_v7(), 0, 1),
        Err(UserContentError::NotFound)
    ));
    assert_eq!(HighlightStyle::Purple, HighlightStyle::Purple);
    Ok(())
}

#[tokio::test]
async fn typed_feedback_does_not_mutate_accepted_analysis() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let (_, output) = seed_accepted(&database, "user:feedback").await?;
    let before: serde_json::Value =
        sqlx::query_scalar("select result from knowledge.analysis_outputs where output_id=$1")
            .bind(output)
            .fetch_one(database.database.pool())
            .await?;
    record_feedback(
        database.database.pool(),
        "user:feedback",
        output,
        FeedbackCategory::UnsupportedClaim,
        Some("No evidence"),
    )
    .await?;
    let after: serde_json::Value =
        sqlx::query_scalar("select result from knowledge.analysis_outputs where output_id=$1")
            .bind(output)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(after, before);
    let categories: Vec<String> = sqlx::query_scalar(
        "select issue_category from knowledge.analysis_feedback where output_id=$1",
    )
    .bind(output)
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(categories, ["unsupported_claim"]);
    database.cleanup().await?;
    Ok(())
}

async fn seed_accepted(
    database: &TestDatabase,
    tenant: &str,
) -> Result<(Uuid, Uuid), Box<dyn std::error::Error>> {
    let source: Uuid = sqlx::query_scalar(
        "insert into knowledge.source_refs (source_ref_id,tenant_ref,owner_context,source_document_id,content_digest_algorithm,content_digest_hex,source_blob)
         values ($1,$2,'test-owner',$3,'sha256',$4,'{}'::jsonb) returning source_ref_id",
    ).bind(Uuid::now_v7()).bind(tenant).bind(Uuid::now_v7().to_string()).bind("a".repeat(64)).fetch_one(database.database.pool()).await?;
    let run = Uuid::now_v7();
    sqlx::query("insert into knowledge.analysis_runs (run_id,source_ref_id,contract_version,prompt_version,context_builder_version,model_policy,state) values ($1,$2,'test','test','test','test','completed')")
        .bind(run).bind(source).execute(database.database.pool()).await?;
    let output: Uuid = sqlx::query_scalar("insert into knowledge.analysis_outputs (output_id,run_id,result,raw_response,accepted) values ($1,$2,'{}'::jsonb,'{}'::jsonb,true) returning output_id")
        .bind(Uuid::now_v7()).bind(run).fetch_one(database.database.pool()).await?;
    Ok((source, output))
}
