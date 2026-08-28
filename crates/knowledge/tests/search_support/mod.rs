//! Shared fixtures for the ranked search integration test target.

use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
use ratatoskr_identifiers::{
    BlobOwner, BlobRef, BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId, MediaType,
    TenantRef,
};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{ProviderError, SourceReference};

pub(crate) fn digest(digit: char) -> Result<ContentDigest, ratatoskr_identifiers::IdentifierError> {
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&digit.to_string().repeat(64))?,
    })
}

/// Registers one source revision under `tenant` and projects its accepted
/// search row directly, simulating a completed analysis whose output landed
/// `age_seconds` ago. Returns the tenant's canonical text form.
pub(crate) async fn project_row(
    database: &TestDatabase,
    tenant: &TenantRef,
    owner_context: &str,
    title: &str,
    lead: &str,
    body: &str,
    age_seconds: i64,
) -> Result<String, Box<dyn std::error::Error>> {
    let document = Document {
        document_id: DocumentId::new_v7(),
        source_address: DocumentAddress::parse("document:search")?,
        content_digest: digest('a')?,
        title: Some(title.to_owned()),
        language: None,
        blocks: vec![DocumentBlock::Paragraph {
            block_id: BlockId::new_v7(),
            text: lead.to_owned(),
        }],
        provenance: Vec::new(),
    };
    let source = database
        .database
        .register_source(&SourceReference {
            tenant: *tenant,
            owner_context: owner_context.to_owned(),
            ai_archive_id: String::new(),
            document_id: document.document_id,
            content_digest: document.content_digest.clone(),
            source_blob: BlobRef {
                owner_service: BlobOwner::parse(owner_context)?,
                digest: document.content_digest.clone(),
                media_type: MediaType::parse("application/json")?,
                length_bytes: 128,
            },
        })
        .await?;
    let (tenant_ref,): (String,) =
        sqlx::query_as("select tenant_ref from knowledge.source_refs where source_ref_id = $1")
            .bind(source.id)
            .fetch_one(database.database.pool())
            .await?;
    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref,
             owner_context, document_id, title, lead, body, updated_at
         )
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                 now() - make_interval(secs => $10::double precision))",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(source.id)
    .bind(uuid::Uuid::now_v7())
    .bind(&tenant_ref)
    .bind(owner_context)
    .bind(document.document_id.0)
    .bind(title)
    .bind(lead)
    .bind(body)
    .bind(age_seconds)
    .execute(database.database.pool())
    .await?;
    Ok(tenant_ref)
}

pub(crate) async fn attach_accepted_output(
    database: &TestDatabase,
    tenant_ref: &str,
    title: &str,
    read_state: Option<&str>,
) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
    let source_ref_id: uuid::Uuid = sqlx::query_scalar(
        "select source_ref_id
         from knowledge.search_documents
         where tenant_ref = $1 and title = $2",
    )
    .bind(tenant_ref)
    .bind(title)
    .fetch_one(database.database.pool())
    .await?;
    let output_id = uuid::Uuid::now_v7();
    let run_id = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into knowledge.analysis_runs (
             run_id, source_ref_id, contract_version, prompt_version,
             context_builder_version, model_policy, state
         ) values ($1, $2, $3, 'search-state', 'search-state', 'search-state', 'completed')",
    )
    .bind(run_id)
    .bind(source_ref_id)
    .bind(format!("search-state-{output_id}"))
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "insert into knowledge.analysis_outputs (
             output_id, run_id, result, raw_response, accepted
         ) values ($1, $2, '{}'::jsonb, '{}'::jsonb, true)",
    )
    .bind(output_id)
    .bind(run_id)
    .execute(database.database.pool())
    .await?;
    sqlx::query(
        "update knowledge.search_documents
         set latest_output_id = $1
         where source_ref_id = $2",
    )
    .bind(output_id)
    .bind(source_ref_id)
    .execute(database.database.pool())
    .await?;
    if let Some(read_state) = read_state {
        sqlx::query(
            "insert into knowledge.analysis_user_states (tenant_ref, output_id, read_state)
             values ($1, $2, $3)",
        )
        .bind(tenant_ref)
        .bind(output_id)
        .bind(read_state)
        .execute(database.database.pool())
        .await?;
    }
    Ok(output_id)
}

/// Test double whose every call returns one fixed outcome.
pub(crate) struct FixedOutcomeProvider {
    identity: ratatoskr_knowledge::EmbeddingIdentity,
    outcome: Result<ratatoskr_knowledge::EmbeddingResponse, ProviderError>,
}

impl ratatoskr_knowledge::EmbeddingProvider for FixedOutcomeProvider {
    fn identity(&self) -> ratatoskr_knowledge::EmbeddingIdentity {
        self.identity.clone()
    }

    fn embed(
        &self,
        _inputs: Vec<String>,
    ) -> impl std::future::Future<
        Output = Result<
            ratatoskr_knowledge::EmbeddingResponse,
            ratatoskr_knowledge::ProviderFailure,
        >,
    > + Send {
        let outcome = match &self.outcome {
            Ok(response) => Ok(response.clone()),
            Err(error) => Err(ratatoskr_knowledge::ProviderFailure {
                error: *error,
                class: ratatoskr_knowledge::ProviderFailureClass::Unclassified,
                http_status: None,
            }),
        };
        std::future::ready(outcome)
    }
}

pub(crate) fn fixed_provider(
    outcome: Result<ratatoskr_knowledge::EmbeddingResponse, ProviderError>,
) -> FixedOutcomeProvider {
    FixedOutcomeProvider {
        identity: ratatoskr_knowledge::EmbeddingIdentity {
            provider: "scripted_fake".to_owned(),
            model: "fake_default_v1".to_owned(),
            dimensions: 1536,
            prompt_version: "none.v1".to_owned(),
        },
        outcome,
    }
}
