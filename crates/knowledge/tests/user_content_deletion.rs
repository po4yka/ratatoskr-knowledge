//! User-content deletion receipt checks.

use ratatoskr_knowledge::BlobStore;
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use uuid::Uuid;

#[tokio::test]
async fn source_and_tenant_deletion_remove_dependent_user_content_with_receipts()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let tenant = "user:deletion";
    let source_id = Uuid::now_v7();
    let document_id = Uuid::now_v7();
    let source_blob = blobs.store_raw(b"source evidence").await?;
    let blob_json = serde_json::to_value(source_blob)?;
    sqlx::query("insert into knowledge.source_refs (source_ref_id,tenant_ref,owner_context,source_document_id,content_digest_algorithm,content_digest_hex,source_blob) values ($1,$2,'test-owner',$3,'sha256',$4,$5)")
        .bind(source_id).bind(tenant).bind(document_id.to_string()).bind("a".repeat(64)).bind(&blob_json).execute(database.database.pool()).await?;
    let run_id = Uuid::now_v7();
    sqlx::query("insert into knowledge.analysis_runs (run_id,source_ref_id,contract_version,prompt_version,context_builder_version,model_policy,state) values ($1,$2,'test','test','test','test','completed')")
        .bind(run_id).bind(source_id).execute(database.database.pool()).await?;
    let output_id = Uuid::now_v7();
    sqlx::query("insert into knowledge.analysis_outputs (output_id,run_id,result,raw_response,accepted) values ($1,$2,'{}'::jsonb,$3,true)")
        .bind(output_id).bind(run_id).bind(&blob_json).execute(database.database.pool()).await?;
    let tag_id = Uuid::now_v7();
    let collection_id = Uuid::now_v7();
    sqlx::query("insert into knowledge.tags (tag_id,tenant_ref,normalized_name,display_name) values ($1,$2,'saved','Saved')").bind(tag_id).bind(tenant).execute(database.database.pool()).await?;
    sqlx::query(
        "insert into knowledge.analysis_taggings (tag_id,output_id,tenant_ref) values ($1,$2,$3)",
    )
    .bind(tag_id)
    .bind(output_id)
    .bind(tenant)
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.collections (collection_id,tenant_ref,name) values ($1,$2,'Saved')",
    )
    .bind(collection_id)
    .bind(tenant)
    .execute(database.database.pool())
    .await?;
    sqlx::query("insert into knowledge.collection_items (collection_id,position,tenant_ref,output_id) values ($1,0,$2,$3)").bind(collection_id).bind(tenant).bind(output_id).execute(database.database.pool()).await?;
    sqlx::query("insert into knowledge.analysis_user_states (tenant_ref,output_id,read_state,favorite) values ($1,$2,'read',true)").bind(tenant).bind(output_id).execute(database.database.pool()).await?;
    sqlx::query("insert into knowledge.highlights (highlight_id,tenant_ref,output_id,source_ref_id,block_id,start_offset,end_offset,style) values ($1,$2,$3,$4,$5,0,1,'yellow')").bind(Uuid::now_v7()).bind(tenant).bind(output_id).bind(source_id).bind(Uuid::now_v7()).execute(database.database.pool()).await?;
    sqlx::query("insert into knowledge.analysis_feedback (feedback_id,tenant_ref,output_id,issue_category) values ($1,$2,$3,'incorrect')").bind(Uuid::now_v7()).bind(tenant).bind(output_id).execute(database.database.pool()).await?;
    let source = ratatoskr_knowledge::delete_source(
        &database.database,
        &blobs,
        tenant,
        "test-owner",
        &document_id.to_string(),
    )
    .await?;
    assert_eq!(
        (
            source.counts.taggings,
            source.counts.collection_items,
            source.counts.analysis_user_states,
            source.counts.highlights,
            source.counts.analysis_feedback
        ),
        (1, 1, 1, 1, 1)
    );
    let tenant = ratatoskr_knowledge::delete_tenant(&database.database, &blobs, tenant).await?;
    assert_eq!((tenant.counts.tags, tenant.counts.collections), (1, 1));
    database.cleanup().await?;
    Ok(())
}
