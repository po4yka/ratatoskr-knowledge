//! Deterministic search-document text extracted from canonical Document IR.

mod sql;

use ratatoskr_document_contracts::{Document, DocumentBlock};

use self::sql::HYBRID_SEARCH_SQL;
use crate::ReadState;

/// Searchable text fields extracted from one Document IR revision.
///
/// Field order mirrors the fixed tsvector weights assigned by
/// `knowledge.search_documents`: title carries weight A, lead weight B,
/// body weight C.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchText {
    /// Title text; receives tsvector weight A.
    pub title: String,

    /// Lead paragraph text; receives tsvector weight B.
    pub lead: String,

    /// Remaining prose; receives tsvector weight C.
    pub body: String,
}

/// Extracts the deterministic title, lead, and body text for one document.
///
/// The title comes from `Document.title`, else from the first Heading
/// block, else stays empty. When the title field wins, the first Heading
/// block is discarded rather than demoted into the body, so an article's
/// duplicated H1 is never re-counted at weight C. Every later Heading
/// block joins the body alongside the remaining Paragraph blocks, in
/// document order.
pub(crate) fn extract_search_text(document: &Document) -> SearchText {
    let mut title = document.title.clone().unwrap_or_default();
    let mut lead: Option<String> = None;
    let mut body_parts: Vec<String> = Vec::new();
    let mut first_heading_seen = false;

    for block in &document.blocks {
        match block {
            DocumentBlock::Heading { text, .. } => {
                if first_heading_seen {
                    body_parts.push(text.clone());
                } else {
                    first_heading_seen = true;
                    if title.is_empty() {
                        title.clone_from(text);
                    }
                }
            }
            DocumentBlock::Paragraph { text, .. } => {
                if lead.is_none() {
                    lead = Some(text.clone());
                } else {
                    body_parts.push(text.clone());
                }
            }
            _ => {}
        }
    }

    SearchText {
        title,
        lead: lead.unwrap_or_default(),
        body: body_parts.join("\n\n"),
    }
}

/// The searchable projection payload derived from one accepted analysis result.
#[derive(Debug)]
pub struct SearchDocumentProjection {
    /// Owning immutable source revision.
    pub source_ref_id: uuid::Uuid,
    /// Analysis output this projection reflects; newer identifiers win.
    pub latest_output_id: uuid::Uuid,
    /// Tenant that owns the projected source revision.
    pub tenant_ref: String,
    /// Owner context that captured the projected source revision.
    pub owner_context: String,
    /// Document identity from the analyzed Document IR revision.
    pub document_id: uuid::Uuid,
    /// Extracted weighted-A title text.
    pub title: String,
    /// Extracted weighted-B lead paragraph.
    pub lead: String,
    /// Extracted remaining weighted-C body text.
    pub body: String,
}

/// Record the latest-wins searchable projection row for an accepted result.
///
/// Delivery is idempotent per source revision: an update lands only when its
/// `latest_output_id` is newer than the currently projected one, so a stale
/// redelivery can never regress an already-projected result.
///
/// # Errors
///
/// Returns the underlying [`sqlx::Error`] when the write fails.
pub async fn record_search_document<'e, E>(
    executor: E,
    projection: &SearchDocumentProjection,
) -> Result<u64, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        "insert into knowledge.search_documents (
             search_document_id, source_ref_id, latest_output_id, tenant_ref,
             owner_context, document_id, title, lead, body, updated_at
         )
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
         on conflict (source_ref_id) do update set
             latest_output_id = excluded.latest_output_id,
             tenant_ref = excluded.tenant_ref,
             owner_context = excluded.owner_context,
             document_id = excluded.document_id,
             title = excluded.title,
             lead = excluded.lead,
             body = excluded.body,
             updated_at = now()
         where excluded.latest_output_id > knowledge.search_documents.latest_output_id
            or (
                excluded.latest_output_id = knowledge.search_documents.latest_output_id
                and (
                    knowledge.search_documents.tenant_ref,
                    knowledge.search_documents.owner_context,
                    knowledge.search_documents.document_id,
                    knowledge.search_documents.title,
                    knowledge.search_documents.lead,
                    knowledge.search_documents.body
                ) is distinct from (
                    excluded.tenant_ref,
                    excluded.owner_context,
                    excluded.document_id,
                    excluded.title,
                    excluded.lead,
                    excluded.body
                )
            )",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(projection.source_ref_id)
    .bind(projection.latest_output_id)
    .bind(&projection.tenant_ref)
    .bind(&projection.owner_context)
    .bind(projection.document_id)
    .bind(&projection.title)
    .bind(&projection.lead)
    .bind(&projection.body)
    .execute(executor)
    .await
    .map(|result| result.rows_affected())
}

/// Records the immutable calculated input required to rebuild one projection.
///
/// The input is derived locally while the source Document is still supplied to
/// the persist path. It deliberately contains no source blob and therefore
/// lets the rebuild remain inside Knowledge's bounded context.
///
/// # Errors
///
/// Returns the underlying [`sqlx::Error`] when the write fails.
pub async fn record_search_projection_input<'e, E>(
    executor: E,
    projection: &SearchDocumentProjection,
) -> Result<u64, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        "insert into knowledge.search_projection_inputs (
             source_ref_id, latest_output_id, tenant_ref, owner_context,
             document_id, title, lead, body, updated_at
         ) values ($1, $2, $3, $4, $5, $6, $7, $8, now())
         on conflict (source_ref_id) do update set
             latest_output_id = excluded.latest_output_id,
             tenant_ref = excluded.tenant_ref,
             owner_context = excluded.owner_context,
             document_id = excluded.document_id,
             title = excluded.title,
             lead = excluded.lead,
             body = excluded.body,
             updated_at = now()
         where excluded.latest_output_id > knowledge.search_projection_inputs.latest_output_id",
    )
    .bind(projection.source_ref_id)
    .bind(projection.latest_output_id)
    .bind(&projection.tenant_ref)
    .bind(&projection.owner_context)
    .bind(projection.document_id)
    .bind(&projection.title)
    .bind(&projection.lead)
    .bind(&projection.body)
    .execute(executor)
    .await
    .map(|result| result.rows_affected())
}

/// Reader-side failure modes for ranked retrieval.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SearchError {
    /// The reader could not run against the database.
    #[error("the search reader failed")]
    Unavailable(#[source] sqlx::Error),
    /// Requested parameters violate the documented page bounds.
    #[error("invalid search parameters")]
    InvalidParameters,
}

/// Closed effective-state filter for tenant-scoped search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadStateFilter {
    /// Return only effectively unread analyses.
    Unread,
    /// Return only read analyses.
    Read,
}

impl ReadStateFilter {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Unread => "unread",
            Self::Read => "read",
        }
    }
}

/// Validated parameters for one ranked search page.
///
/// The constructor validates page bounds before any database work; the
/// tenant is required at construction so every reader path stays scoped.
#[derive(Debug, Clone)]
pub struct SearchQuery {
    tenant_ref: String,
    raw_query: String,
    read_state: Option<ReadStateFilter>,
    limit: i64,
    offset: i64,
}

impl SearchQuery {
    /// Creates a validated query for `tenant_ref`.
    ///
    /// # Errors
    ///
    /// Returns [`SearchError::InvalidParameters`] when `limit` falls outside
    /// `1..=100` or `offset` is negative.
    pub fn new(
        tenant_ref: impl Into<String>,
        raw_query: impl Into<String>,
        limit: i64,
        offset: i64,
    ) -> Result<Self, SearchError> {
        if !(1..=100).contains(&limit) || offset < 0 {
            return Err(SearchError::InvalidParameters);
        }
        Ok(Self {
            tenant_ref: tenant_ref.into(),
            raw_query: raw_query.into(),
            read_state: None,
            limit,
            offset,
        })
    }

    /// Owning tenant in its canonical text form.
    #[must_use]
    pub fn tenant_ref(&self) -> &str {
        &self.tenant_ref
    }

    /// Raw user query text; blank means recency browse.
    #[must_use]
    pub fn raw_query(&self) -> &str {
        &self.raw_query
    }

    /// Page size.
    #[must_use]
    pub fn limit(&self) -> i64 {
        self.limit
    }

    /// Page offset.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Restricts the page to one effective read state.
    #[must_use]
    pub const fn with_read_state(mut self, filter: ReadStateFilter) -> Self {
        self.read_state = Some(filter);
        self
    }

    /// Optional effective-state restriction.
    #[must_use]
    pub const fn read_state(&self) -> Option<ReadStateFilter> {
        self.read_state
    }
}

/// One rendered hit from the ranked reader.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchResult {
    /// Accepted analysis output represented by this projection.
    pub analysis_id: uuid::Uuid,
    /// Owner context that captured the projected source.
    pub owner_context: String,
    /// Document identity of the projected revision.
    pub document_id: uuid::Uuid,
    /// Extracted weighted-A title.
    pub title: String,
    /// Word-bounded snippet over lead and body; absent while browsing.
    pub snippet: Option<String>,
    /// Cover-density rank; absent while browsing.
    pub rank: Option<f32>,
    /// Effective state of the accepted analysis; absent rows are unread.
    pub read_state: ReadState,
}

/// One page of ranked results.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchPage {
    /// Ordered hits.
    pub results: Vec<SearchResult>,
    /// Whether another result exists after this page.
    pub has_more: bool,
}

/// Reads one ranked page for the query's tenant.
///
/// A blank or absent query browses by descending update time without snippet
/// or score; any other query compiles through web-search syntax and ranks by
/// cover density.
///
/// # Errors
///
/// Returns [`SearchError`] when parameters are invalid or reading fails.
pub async fn search_page<'e, E>(executor: E, query: &SearchQuery) -> Result<SearchPage, SearchError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    if query.raw_query().trim().is_empty() {
        browse_recent(executor, query).await
    } else {
        rank_matches(executor, query).await
    }
}

/// Fixed Reciprocal Rank Fusion constant: each contributing leg adds
/// one over k plus rank, where k is [`RRF_K`], to a candidate's fused score.
pub const RRF_K: i64 = 60;

/// Slack added to the requested page when sizing each candidate leg.
pub const CANDIDATE_DEPTH_SLACK: i64 = 25;

/// Hard ceiling on how deep each candidate leg may reach.
pub const MAX_CANDIDATE_DEPTH: i64 = 200;

/// Computes the documented deterministic candidate depth for one page.
///
/// Both fusion legs consider the same depth, derived only from the
/// requested pagination so repeated queries see identical candidates.
#[must_use]
pub fn candidate_depth(limit: i64, offset: i64) -> i64 {
    (offset
        .saturating_add(limit)
        .saturating_add(CANDIDATE_DEPTH_SLACK))
    .min(MAX_CANDIDATE_DEPTH)
}

/// One query-time binding of a semantic search leg.
///
/// Every field pins the embedding identity exactly as persisted, so
/// vectors from any other model, prompt, or chunking version stay
/// invisible to this query.
#[derive(Debug, Clone)]
pub struct SemanticLeg {
    /// Query embedding; cosine distance orders the semantic leg.
    pub vector: Vec<f32>,
    /// Provider identifier bound to stored vectors.
    pub provider: String,
    /// Model identifier bound to stored vectors.
    pub model: String,
    /// Prompt-version label bound to stored vectors.
    pub prompt_version: String,
    /// Chunking-version label bound to stored vectors.
    pub chunking_version: String,
}

/// Reads one hybrid-ranked page fusing lexical and semantic legs.
///
/// The lexical leg ranks by the same cover-density ordering as
/// [`rank_matches`]; the semantic leg joins tenant-scoped projections
/// with the best (smallest cosine distance) chunk per document under
/// the leg's exact model identity. Each leg contributes candidates up
/// to [`candidate_depth`] and every candidate accumulates one over
/// [`RRF_K`] plus leg rank; ties break by descending recency then
/// document identity, and pagination applies last.
///
/// # Errors
///
/// Returns [`SearchError`] when reading fails.
pub async fn hybrid_search_page<'e, E>(
    executor: E,
    query: &SearchQuery,
    leg: &SemanticLeg,
) -> Result<SearchPage, SearchError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let depth = candidate_depth(query.limit(), query.offset());
    let mut rows: Vec<(uuid::Uuid, bool, String, uuid::Uuid, String, String, f32)> =
        sqlx::query_as(HYBRID_SEARCH_SQL)
            .bind(query.tenant_ref())
            .bind(query.raw_query())
            .bind(pgvector::Vector::from(leg.vector.clone()))
            .bind(&leg.provider)
            .bind(&leg.model)
            .bind(&leg.prompt_version)
            .bind(&leg.chunking_version)
            .bind(query.read_state().map(ReadStateFilter::as_str))
            .bind(depth)
            .bind(RRF_K)
            .bind(query.limit() + 1)
            .bind(query.offset())
            .fetch_all(executor)
            .await
            .map_err(SearchError::Unavailable)?;
    let has_more = rows.len() > usize::try_from(query.limit()).unwrap_or(usize::MAX);
    rows.truncate(usize::try_from(query.limit()).unwrap_or_default());
    Ok(SearchPage {
        results: rows
            .into_iter()
            .map(
                |(analysis_id, is_read, owner_context, document_id, title, snippet, rank)| {
                    SearchResult {
                        analysis_id,
                        owner_context,
                        document_id,
                        title,
                        snippet: Some(snippet),
                        rank: Some(rank),
                        read_state: if is_read {
                            ReadState::Read
                        } else {
                            ReadState::Unread
                        },
                    }
                },
            )
            .collect(),
        has_more,
    })
}

/// Which retrieval path served one page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankingPath {
    /// Blank query browsed by recency.
    BrowseRecent,
    /// Lexical-only ranking served the page.
    LexicalOnly,
    /// Fused lexical-plus-semantic ranking served the page.
    Hybrid,
}

/// Retrieval selector fusing the embeddings seam with the ranked reader.
///
/// Non-blank queries embed through the controlled provider and serve a
/// hybrid page; any embedding failure, including a dimension mismatch,
/// degrades to plain lexical ranking instead of failing the request.
#[derive(Debug)]
pub struct HybridRetriever<P> {
    provider: P,
}

impl<P: crate::EmbeddingProvider> HybridRetriever<P> {
    /// Wraps one embeddings provider behind the retrieval selector.
    #[must_use]
    pub const fn new(provider: P) -> Self {
        Self { provider }
    }

    /// Serves one page and reports which ranking path served it.
    ///
    /// # Errors
    ///
    /// Returns [`SearchError`] when parameters are invalid or the chosen
    /// database read fails; an embedding failure never surfaces here.
    pub async fn page<'e, E>(
        &self,
        executor: E,
        query: &SearchQuery,
    ) -> Result<(SearchPage, RankingPath), SearchError>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        if query.raw_query().trim().is_empty() {
            return Ok((
                search_page(executor, query).await?,
                RankingPath::BrowseRecent,
            ));
        }
        let identity = self.provider.identity();
        let embedded = self
            .provider
            .embed(vec![query.raw_query().trim().to_owned()])
            .await;
        let vector = match embedded {
            Ok(response)
                if response.vectors.len() == 1
                    && response
                        .vectors
                        .first()
                        .is_some_and(|vector| vector.len() == usize::from(identity.dimensions)) =>
            {
                response.vectors.into_iter().next().unwrap_or_default()
            }
            Ok(response) => {
                tracing::warn!(
                    operation = "hybrid_retrieval",
                    outcome = "dimension_mismatch",
                    returned = response.vectors.len(),
                    expected_dimensions = identity.dimensions,
                    "query embedding shape mismatched; serving lexical results"
                );
                return Ok((
                    search_page(executor, query).await?,
                    RankingPath::LexicalOnly,
                ));
            }
            Err(failure) => {
                tracing::warn!(
                    operation = "hybrid_retrieval",
                    outcome = "embed_failed",
                    failure_class = failure.class.as_str(),
                    http_status = failure.http_status,
                    "query embedding failed; serving lexical results"
                );
                return Ok((
                    search_page(executor, query).await?,
                    RankingPath::LexicalOnly,
                ));
            }
        };
        let leg = SemanticLeg {
            vector,
            provider: identity.provider,
            model: identity.model,
            prompt_version: identity.prompt_version,
            chunking_version: crate::CHUNKING_VERSION.to_owned(),
        };
        Ok((
            hybrid_search_page(executor, query, &leg).await?,
            RankingPath::Hybrid,
        ))
    }
}

/// Recency browse: newest first, no snippet, no score.
async fn browse_recent<'e, E>(executor: E, query: &SearchQuery) -> Result<SearchPage, SearchError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let mut rows: Vec<(uuid::Uuid, bool, String, uuid::Uuid, String)> = sqlx::query_as(
        "select s.latest_output_id,
                coalesce(us.read_state = 'read', false),
                s.owner_context,
                s.document_id,
                s.title
         from knowledge.search_documents s
         left join knowledge.analysis_user_states us
           on us.tenant_ref = s.tenant_ref
          and us.output_id = s.latest_output_id
         where s.tenant_ref = $1
           and ($2::text is null or coalesce(us.read_state, 'unread') = $2)
         order by s.updated_at desc, s.search_document_id desc
         limit $3 offset $4",
    )
    .bind(query.tenant_ref())
    .bind(query.read_state().map(ReadStateFilter::as_str))
    .bind(query.limit() + 1)
    .bind(query.offset())
    .fetch_all(executor)
    .await
    .map_err(SearchError::Unavailable)?;
    let has_more = rows.len() > usize::try_from(query.limit()).unwrap_or(usize::MAX);
    rows.truncate(usize::try_from(query.limit()).unwrap_or_default());
    Ok(SearchPage {
        results: rows
            .into_iter()
            .map(
                |(analysis_id, is_read, owner_context, document_id, title)| SearchResult {
                    analysis_id,
                    owner_context,
                    document_id,
                    title,
                    snippet: None,
                    rank: None,
                    read_state: if is_read {
                        ReadState::Read
                    } else {
                        ReadState::Unread
                    },
                },
            )
            .collect(),
        has_more,
    })
}

/// Cover-density ranking over the weighted projection columns.
async fn rank_matches<'e, E>(executor: E, query: &SearchQuery) -> Result<SearchPage, SearchError>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let mut rows: Vec<(uuid::Uuid, bool, String, uuid::Uuid, String, String, f32)> =
        sqlx::query_as(
            "select s.latest_output_id,
                coalesce(us.read_state = 'read', false),
                s.owner_context,
                s.document_id,
                s.title,
                ts_headline(
                    'english',
                    s.lead || ' ' || s.body,
                    websearch_to_tsquery('english', $2),
                    'StartSel=<b>, StopSel=</b>, MaxWords=16, MinWords=6, MaxFragments=0'
                ),
                ts_rank_cd(s.search_vector, websearch_to_tsquery('english', $2))
         from knowledge.search_documents s
         left join knowledge.analysis_user_states us
           on us.tenant_ref = s.tenant_ref
          and us.output_id = s.latest_output_id
         where s.tenant_ref = $1
           and ($3::text is null or coalesce(us.read_state, 'unread') = $3)
           and s.search_vector @@ websearch_to_tsquery('english', $2)
         order by ts_rank_cd(s.search_vector, websearch_to_tsquery('english', $2)) desc,
                  s.updated_at desc,
                  s.search_document_id desc
         limit $4 offset $5",
        )
        .bind(query.tenant_ref())
        .bind(query.raw_query())
        .bind(query.read_state().map(ReadStateFilter::as_str))
        .bind(query.limit() + 1)
        .bind(query.offset())
        .fetch_all(executor)
        .await
        .map_err(SearchError::Unavailable)?;
    let has_more = rows.len() > usize::try_from(query.limit()).unwrap_or(usize::MAX);
    rows.truncate(usize::try_from(query.limit()).unwrap_or_default());
    Ok(SearchPage {
        results: rows
            .into_iter()
            .map(
                |(analysis_id, is_read, owner_context, document_id, title, snippet, rank)| {
                    SearchResult {
                        analysis_id,
                        owner_context,
                        document_id,
                        title,
                        snippet: Some(snippet),
                        rank: Some(rank),
                        read_state: if is_read {
                            ReadState::Read
                        } else {
                            ReadState::Unread
                        },
                    }
                },
            )
            .collect(),
        has_more,
    })
}

#[cfg(test)]
mod tests {
    use ratatoskr_document_contracts::{Document, DocumentAddress, DocumentBlock};
    use ratatoskr_identifiers::{BlockId, ContentDigest, DigestAlgorithm, DigestHex, DocumentId};

    use super::{SearchText, extract_search_text};

    fn fixture_document(
        title: Option<&str>,
        blocks: Vec<DocumentBlock>,
    ) -> Result<Document, Box<dyn std::error::Error>> {
        Ok(Document {
            document_id: DocumentId::new_v7(),
            source_address: DocumentAddress::parse("document:search-extract")?,
            content_digest: ContentDigest {
                algorithm: DigestAlgorithm::Sha256,
                hex: DigestHex::parse(&"a".repeat(64))?,
            },
            title: title.map(str::to_owned),
            language: None,
            blocks,
            provenance: Vec::new(),
        })
    }

    fn heading(text: &str) -> DocumentBlock {
        DocumentBlock::Heading {
            block_id: BlockId::new_v7(),
            level: 1,
            text: text.to_owned(),
        }
    }

    fn paragraph(text: &str) -> DocumentBlock {
        DocumentBlock::Paragraph {
            block_id: BlockId::new_v7(),
            text: text.to_owned(),
        }
    }

    #[test]
    fn title_comes_from_the_document_title_field() -> Result<(), Box<dyn std::error::Error>> {
        let document = fixture_document(
            Some("Doc Title"),
            vec![
                heading("First Heading"),
                paragraph("Lead."),
                paragraph("Rest."),
            ],
        )?;

        let text = extract_search_text(&document);

        assert_eq!(
            text,
            SearchText {
                title: "Doc Title".to_owned(),
                lead: "Lead.".to_owned(),
                body: "Rest.".to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn title_falls_back_to_the_first_heading_block() -> Result<(), Box<dyn std::error::Error>> {
        let document = fixture_document(
            None,
            vec![
                heading("Fallback Heading"),
                paragraph("Lead."),
                paragraph("Body."),
            ],
        )?;

        let text = extract_search_text(&document);

        assert_eq!(
            text,
            SearchText {
                title: "Fallback Heading".to_owned(),
                lead: "Lead.".to_owned(),
                body: "Body.".to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn title_is_empty_without_a_title_field_or_any_heading()
    -> Result<(), Box<dyn std::error::Error>> {
        let document = fixture_document(None, vec![paragraph("Only paragraph.")])?;

        let text = extract_search_text(&document);

        assert_eq!(
            text,
            SearchText {
                title: String::new(),
                lead: "Only paragraph.".to_owned(),
                body: String::new(),
            }
        );
        Ok(())
    }

    #[test]
    fn lead_is_the_first_paragraph_and_body_keeps_later_blocks()
    -> Result<(), Box<dyn std::error::Error>> {
        let document = fixture_document(
            Some("Doc Title"),
            vec![
                paragraph("First."),
                heading("Section One"),
                paragraph("Second."),
                heading("Section Two"),
                paragraph("Third."),
            ],
        )?;

        let text = extract_search_text(&document);

        assert_eq!(
            text,
            SearchText {
                title: "Doc Title".to_owned(),
                lead: "First.".to_owned(),
                body: "Second.\n\nSection Two\n\nThird.".to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn an_empty_document_yields_an_empty_triple() -> Result<(), Box<dyn std::error::Error>> {
        let document = fixture_document(None, Vec::new())?;

        let text = extract_search_text(&document);

        assert_eq!(
            text,
            SearchText {
                title: String::new(),
                lead: String::new(),
                body: String::new(),
            }
        );
        Ok(())
    }
}
