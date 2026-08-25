//! Durable fake-provider pipeline flow tests.

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{BlobOwner, BlobRef, DocumentId, MediaType, TenantRef, UserId};
use ratatoskr_knowledge::test_support::{
    FakeReply, FakeTransport, TemporaryBlobRoot, TestDatabase,
};
use ratatoskr_knowledge::{
    AnalysisIdentity, ArticlePipeline, BlobStore, ProviderError, ScriptedProvider, SourceReference,
    build_generation_request, prepare_context,
};

mod support;

use support::*;

#[tokio::test]
async fn cancelled_mid_request_keeps_durable_state_and_replays_once()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    let request = build_generation_request(&context)?;
    let stalled = FakeTransport::start(vec![FakeReply::stall()]).await?;
    let stalled_provider = controlled_openrouter(&database, &stalled, 3)?;

    let task_database = database.database.clone();
    let task_blobs = blobs.clone();
    let task_context = context.clone();
    let task_request = request.clone();
    let task_document = document.clone();
    let handle = tokio::spawn(async move {
        let pipeline = ArticlePipeline::new(
            &task_database,
            &stalled_provider,
            &task_blobs,
            std::time::Duration::from_secs(5),
        );
        pipeline
            .execute(run_id, task_request, &task_context, &task_document)
            .await
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while stalled.request_count()? == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the stalled transport never received a request"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    handle.abort();
    let joined = handle.await;
    assert!(joined.is_err(), "the task should have been cancelled");

    let (state, outcome): (String, String) = sqlx::query_as(
        "select runs.state, attempts.outcome
         from knowledge.analysis_runs runs
         join knowledge.analysis_attempts attempts
             on attempts.run_id = runs.run_id and attempts.ordinal = 1
         where runs.run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "model_requested");
    assert_eq!(outcome, "requested");
    let outputs: i64 =
        sqlx::query_scalar("select count(*) from knowledge.analysis_outputs where run_id = $1")
            .bind(run_id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(outputs, 0);

    let healthy = FakeTransport::start(vec![FakeReply::bytes(200, valid_envelope_bytes())]).await?;
    let healthy_provider = controlled_openrouter(&database, &healthy, 3)?;
    let replay_pipeline = ArticlePipeline::new(
        &database.database,
        &healthy_provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );
    let replayed = replay_pipeline
        .execute(run_id, request, &context, &document)
        .await?;
    assert_eq!(replayed.summary, "A grounded summary.");
    let (state, attempts, outputs): (String, i64, i64) = sqlx::query_as(
        "select state,
                (select count(*) from knowledge.analysis_attempts where run_id = $1),
                (select count(*) from knowledge.analysis_outputs where run_id = $1)
         from knowledge.analysis_runs where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "persisted");
    assert_eq!(attempts, 2);
    assert_eq!(outputs, 1);

    database.cleanup().await?;
    Ok(())
}

// A second cancellation consumes the last ordinal; the next replay must fail
// the run explicitly instead of leaving it stuck mid-state.
#[tokio::test]
async fn exhausted_cancellation_replay_fails_explicitly() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (second_run_id, second_context, second_document) = run_and_context(&database).await?;
    sqlx::query(
        "insert into knowledge.analysis_attempts (
            run_id, ordinal, reason, provider, model_policy, outcome
         ) values ($1, 1, 'initial', 'openrouter', 'fake_default_v1', 'requested')",
    )
    .bind(second_run_id)
    .execute(database.database.pool())
    .await?;
    sqlx::query("update knowledge.analysis_runs set state = 'model_requested' where run_id = $1")
        .bind(second_run_id)
        .execute(database.database.pool())
        .await?;
    let exhausted = FakeTransport::start(vec![FakeReply::stall()]).await?;
    let exhausted_provider = controlled_openrouter(&database, &exhausted, 1)?;
    let task_database = database.database.clone();
    let task_blobs = blobs.clone();
    let task_context = second_context.clone();
    let task_document = second_document.clone();
    let second_request = build_generation_request(&second_context)?;
    let second_handle = tokio::spawn(async move {
        let pipeline = ArticlePipeline::new(
            &task_database,
            &exhausted_provider,
            &task_blobs,
            std::time::Duration::from_millis(50),
        );
        pipeline
            .execute(second_run_id, second_request, &task_context, &task_document)
            .await
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while exhausted.request_count()? == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the stalled transport never received a request"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    second_handle.abort();
    let _ignored = second_handle.await;

    let healthy_again =
        FakeTransport::start(vec![FakeReply::bytes(200, valid_envelope_bytes())]).await?;
    let again_provider = controlled_openrouter(&database, &healthy_again, 1)?;
    let again_pipeline = ArticlePipeline::new(
        &database.database,
        &again_provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );
    let replay_outcome = again_pipeline
        .execute(
            second_run_id,
            build_generation_request(&second_context)?,
            &second_context,
            &second_document,
        )
        .await;
    assert!(replay_outcome.is_err());
    let final_state: String =
        sqlx::query_scalar("select state from knowledge.analysis_runs where run_id = $1")
            .bind(second_run_id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(final_state, "failed");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn flaky_transport_keeps_retry_and_repair_bounded() -> Result<(), Box<dyn std::error::Error>>
{
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);

    let (failing_run_id, failing_context, failing_document) = run_and_context(&database).await?;
    let always_fault = FakeTransport::start(Vec::new()).await?;
    let failing_provider = controlled_openrouter(&database, &always_fault, 3)?;
    let failing_pipeline = ArticlePipeline::new(
        &database.database,
        &failing_provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );
    let failing = failing_pipeline
        .execute(
            failing_run_id,
            build_generation_request(&failing_context)?,
            &failing_context,
            &failing_document,
        )
        .await;
    assert!(failing.is_err());
    let (state, attempts): (String, i64) = sqlx::query_as(
        "select state,
                (select count(*) from knowledge.analysis_attempts where run_id = $1)
         from knowledge.analysis_runs where run_id = $1",
    )
    .bind(failing_run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(state, "failed");
    assert_eq!(attempts, 2);
    assert_eq!(always_fault.request_count()?, 6);

    let (repair_run_id, repair_context, repair_document) = run_and_context(&database).await?;
    let invalid_content = FakeReply::bytes(
        200,
        br#"{
            "id": "gen-1755858000-recorded000000003",
            "choices": [{"message": {"role": "assistant",
                "content": "{\"summary\":\"\",\"key_points\":[]}"}}],
            "usage": {"prompt_tokens": 20, "completion_tokens": 4}
        }"#
        .to_vec(),
    );
    let flaky = FakeTransport::start(vec![
        invalid_content,
        FakeReply::bytes(502, Vec::new()),
        FakeReply::bytes(502, Vec::new()),
        FakeReply::bytes(200, valid_envelope_bytes()),
    ])
    .await?;
    let repair_provider = controlled_openrouter(&database, &flaky, 3)?;
    let repair_pipeline = ArticlePipeline::new(
        &database.database,
        &repair_provider,
        &blobs,
        std::time::Duration::from_secs(5),
    );
    let repaired = repair_pipeline
        .execute(
            repair_run_id,
            build_generation_request(&repair_context)?,
            &repair_context,
            &repair_document,
        )
        .await?;
    assert_eq!(repaired.summary, "A grounded summary.");
    let (repair_state, repair_attempts, repair_outputs): (String, i64, i64) = sqlx::query_as(
        "select state,
                (select count(*) from knowledge.analysis_attempts where run_id = $1),
                (select count(*) from knowledge.analysis_outputs where run_id = $1)
         from knowledge.analysis_runs where run_id = $1",
    )
    .bind(repair_run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(repair_state, "persisted");
    assert_eq!(repair_attempts, 2);
    assert_eq!(repair_outputs, 1);
    assert_eq!(flaky.request_count()?, 4);

    database.cleanup().await?;
    Ok(())
}

fn valid_envelope_bytes() -> Vec<u8> {
    br#"{
        "id": "gen-1755858000-recorded000000002",
        "choices": [{"message": {"role": "assistant", "content": "{\"summary\":\"A grounded summary.\",\"key_points\":[{\"text\":\"Evidence exists.\",\"source_block_indexes\":[0]}]}"}}],
        "usage": {"prompt_tokens": 20, "completion_tokens": 10}
    }"#
    .to_vec()
}

/// Shared projection-test fixture: registers one source revision and returns
/// its id together with the analyzed document and prepared context.
async fn projection_fixture(
    database: &TestDatabase,
    digest_char: char,
    title: Option<String>,
    blocks: Vec<DocumentBlock>,
) -> Result<(uuid::Uuid, Document, ratatoskr_knowledge::PreparedContext), Box<dyn std::error::Error>>
{
    let document = Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:pipeline")?,
        content_digest: digest(digest_char)?,
        title,
        language: None,
        blocks,
        provenance: Vec::new(),
    };
    let source = database
        .database
        .register_source(&SourceReference {
            tenant: TenantRef::of_user(UserId::new_v7()),
            owner_context: "ratatoskr-extractor".to_owned(),
            document_id: document.document_id,
            content_digest: document.content_digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse("ratatoskr-extractor")?,
                digest: document.content_digest.clone(),
                media_type: MediaType::parse("application/json")?,
                length_bytes: 128,
            },
        })
        .await?;
    let context = prepare_context(&document, 1_000)?;
    Ok((source.id, document, context))
}

/// A run identity over one source revision with a caller-chosen prompt version.
fn analysis_identity_for(source_ref_id: uuid::Uuid, prompt_version: &str) -> AnalysisIdentity {
    AnalysisIdentity {
        source_revision_id: source_ref_id,
        contract_version: "article_v1".to_owned(),
        prompt_version: prompt_version.to_owned(),
        context_builder_version: "document_context_v1".to_owned(),
        model_policy: "fake_default_v1".to_owned(),
    }
}

/// The single accepted output id recorded for one analysis run.
async fn accepted_output(
    database: &TestDatabase,
    run_id: uuid::Uuid,
) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
    let (output_id,): (uuid::Uuid,) =
        sqlx::query_as("select output_id from knowledge.analysis_outputs where run_id = $1")
            .bind(run_id)
            .fetch_one(database.database.pool())
            .await?;
    Ok(output_id)
}

#[tokio::test]
async fn accepted_run_projects_exactly_one_search_document_row_with_derived_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (run_id, context, document) = run_and_context(&database).await?;
    let provider = ScriptedProvider::new([Ok(valid_response("project"))]);
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );
    pipeline
        .execute(
            run_id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await?;

    let (row_count,): (i64,) = sqlx::query_as("select count(*) from knowledge.search_documents")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(row_count, 1);
    let (
        projected_source,
        projected_output,
        projected_tenant,
        projected_owner,
        projected_document,
        projected_title,
        projected_lead,
        projected_body,
    ): (
        uuid::Uuid,
        uuid::Uuid,
        String,
        String,
        uuid::Uuid,
        String,
        String,
        String,
    ) = sqlx::query_as(
        "select source_ref_id, latest_output_id, tenant_ref, owner_context,
                document_id, title, lead, body
         from knowledge.search_documents",
    )
    .fetch_one(database.database.pool())
    .await?;
    let (expected_source, expected_tenant, expected_owner): (uuid::Uuid, String, String) =
        sqlx::query_as(
            "select r.source_ref_id, s.tenant_ref, s.owner_context
             from knowledge.analysis_runs r
             join knowledge.source_refs s on s.source_ref_id = r.source_ref_id
             where r.run_id = $1",
        )
        .bind(run_id)
        .fetch_one(database.database.pool())
        .await?;
    let (expected_output,): (uuid::Uuid,) =
        sqlx::query_as("select output_id from knowledge.analysis_outputs where run_id = $1")
            .bind(run_id)
            .fetch_one(database.database.pool())
            .await?;

    assert_eq!(projected_source, expected_source);
    assert_eq!(projected_output, expected_output);
    assert_eq!(projected_tenant, expected_tenant);
    assert_eq!(projected_owner, expected_owner);
    assert_eq!(projected_document, document.document_id.0);
    assert_eq!(projected_title, "Pipeline");
    assert_eq!(projected_lead, "Evidence.");
    assert!(projected_body.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_later_failed_run_leaves_the_projected_search_document_row_untouched()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (source_ref_id, document, context) = projection_fixture(
        &database,
        'b',
        Some("Pipeline".to_owned()),
        vec![DocumentBlock::Paragraph {
            text: "Evidence.".to_owned(),
        }],
    )
    .await?;
    let accepted_run = database
        .database
        .create_run(&analysis_identity_for(source_ref_id, "article_prompt_v1"))
        .await?;
    let failing_run = database
        .database
        .create_run(&analysis_identity_for(source_ref_id, "article_prompt_alt"))
        .await?;

    let accepted_provider = ScriptedProvider::new([Ok(valid_response("accept"))]);
    let pipeline = ArticlePipeline::new(
        &database.database,
        &accepted_provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );
    pipeline
        .execute(
            accepted_run.id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await?;
    let before: (String, uuid::Uuid) = sqlx::query_as(
        "select title, latest_output_id from knowledge.search_documents where source_ref_id = $1",
    )
    .bind(source_ref_id)
    .fetch_one(database.database.pool())
    .await?;

    let failing_provider = ScriptedProvider::new([Err(ProviderError::Permanent)]);
    let failing_pipeline = ArticlePipeline::new(
        &database.database,
        &failing_provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );
    assert!(
        failing_pipeline
            .execute(
                failing_run.id,
                build_generation_request(&context)?,
                &context,
                &document,
            )
            .await
            .is_err()
    );

    let (row_count,): (i64,) =
        sqlx::query_as("select count(*) from knowledge.search_documents where source_ref_id = $1")
            .bind(source_ref_id)
            .fetch_one(database.database.pool())
            .await?;
    let after: (String, uuid::Uuid) = sqlx::query_as(
        "select title, latest_output_id from knowledge.search_documents where source_ref_id = $1",
    )
    .bind(source_ref_id)
    .fetch_one(database.database.pool())
    .await?;

    assert_eq!(row_count, 1);
    assert_eq!(after, before);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_newer_accepted_output_replaces_the_search_document()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 4_096);
    let (source_ref_id, document, context) = projection_fixture(
        &database,
        'c',
        Some("Pipeline".to_owned()),
        vec![DocumentBlock::Paragraph {
            text: "Evidence.".to_owned(),
        }],
    )
    .await?;
    let older_run = database
        .database
        .create_run(&analysis_identity_for(source_ref_id, "article_prompt_v1"))
        .await?;
    let newer_run = database
        .database
        .create_run(&analysis_identity_for(source_ref_id, "article_prompt_alt"))
        .await?;
    let provider =
        ScriptedProvider::new([Ok(valid_response("older")), Ok(valid_response("newer"))]);
    let pipeline = ArticlePipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );
    pipeline
        .execute(
            older_run.id,
            build_generation_request(&context)?,
            &context,
            &document,
        )
        .await?;

    let mut revised = document.clone();
    revised.title = Some("Pipeline Updated".to_owned());
    revised.blocks.insert(
        0,
        DocumentBlock::Paragraph {
            text: "Revised evidence.".to_owned(),
        },
    );
    let revised_context = prepare_context(&revised, 1_000)?;
    pipeline
        .execute(
            newer_run.id,
            build_generation_request(&revised_context)?,
            &revised_context,
            &revised,
        )
        .await?;

    let row: (String, String, String, uuid::Uuid) = sqlx::query_as(
        "select title, lead, body, latest_output_id from knowledge.search_documents
         where source_ref_id = $1",
    )
    .bind(source_ref_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(row.0, "Pipeline Updated");
    assert_eq!(row.1, "Revised evidence.");
    assert_eq!(row.2, "Evidence.");
    assert_eq!(row.3, accepted_output(&database, newer_run.id).await?);
    let (row_count,): (i64,) =
        sqlx::query_as("select count(*) from knowledge.search_documents where source_ref_id = $1")
            .bind(source_ref_id)
            .fetch_one(database.database.pool())
            .await?;
    assert_eq!(row_count, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn an_older_projection_delivery_cannot_regress_the_search_document()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let (source_ref_id, document, _context) = projection_fixture(
        &database,
        'd',
        Some("Pipeline".to_owned()),
        vec![DocumentBlock::Paragraph {
            text: "Evidence.".to_owned(),
        }],
    )
    .await?;
    let (tenant_ref, owner_context): (String, String) = sqlx::query_as(
        "select tenant_ref, owner_context from knowledge.source_refs where source_ref_id = $1",
    )
    .bind(source_ref_id)
    .fetch_one(database.database.pool())
    .await?;

    let current = ratatoskr_knowledge::search::SearchDocumentProjection {
        source_ref_id,
        latest_output_id: uuid::Uuid::now_v7(),
        tenant_ref: tenant_ref.clone(),
        owner_context: owner_context.clone(),
        document_id: document.document_id.0,
        title: "Current".to_owned(),
        lead: "Current lead.".to_owned(),
        body: String::new(),
    };
    ratatoskr_knowledge::search::record_search_document(database.database.pool(), &current).await?;

    let stale = ratatoskr_knowledge::search::SearchDocumentProjection {
        source_ref_id,
        latest_output_id: uuid::Uuid::from_u128(1),
        tenant_ref,
        owner_context,
        document_id: document.document_id.0,
        title: "Stale".to_owned(),
        lead: "Stale lead.".to_owned(),
        body: String::new(),
    };
    ratatoskr_knowledge::search::record_search_document(database.database.pool(), &stale).await?;

    let row: (String, uuid::Uuid) = sqlx::query_as(
        "select title, latest_output_id from knowledge.search_documents where source_ref_id = $1",
    )
    .bind(source_ref_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(row.0, "Current");
    assert_eq!(row.1, current.latest_output_id);

    database.cleanup().await?;
    Ok(())
}
