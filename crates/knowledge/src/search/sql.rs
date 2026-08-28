pub(crate) const HYBRID_SEARCH_SQL: &str = "with lex as (
     select s.search_document_id,
            row_number() over (
                order by ts_rank_cd(s.search_vector, q.tsq) desc,
                         s.updated_at desc,
                         s.search_document_id desc
            ) as rnk
     from knowledge.search_documents s
     left join knowledge.analysis_user_states us
       on us.tenant_ref = s.tenant_ref
      and us.output_id = s.latest_output_id
     cross join websearch_to_tsquery('english', $2) as q(tsq)
     where s.tenant_ref = $1
       and ($8::text is null or coalesce(us.read_state, 'unread') = $8)
       and s.search_vector @@ q.tsq
     limit $9
 ),
 sem as (
     select d.search_document_id,
            row_number() over (
                order by best.dist asc,
                         d.updated_at desc,
                         d.search_document_id desc
            ) as rnk
     from (
         select c.source_ref_id, min(c.embedding <=> $3) as dist
         from knowledge.embedding_chunks c
         where c.provider = $4
           and c.model = $5
           and c.prompt_version = $6
           and c.chunking_version = $7
         group by c.source_ref_id
     ) best
     join knowledge.search_documents d
       on d.source_ref_id = best.source_ref_id
     left join knowledge.analysis_user_states us
       on us.tenant_ref = d.tenant_ref
      and us.output_id = d.latest_output_id
     where d.tenant_ref = $1
       and ($8::text is null or coalesce(us.read_state, 'unread') = $8)
     limit $9
 ),
 fused as (
     select search_document_id, 1.0::double precision / ($10 + rnk) as score
     from lex
     union all
     select search_document_id, 1.0::double precision / ($10 + rnk) as score
     from sem
 )
 select s.latest_output_id,
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
        sum(f.score)::real
 from fused f
 join knowledge.search_documents s
   on s.search_document_id = f.search_document_id
 left join knowledge.analysis_user_states us
   on us.tenant_ref = s.tenant_ref
  and us.output_id = s.latest_output_id
 group by s.search_document_id,
          s.latest_output_id,
          us.read_state,
          s.owner_context,
          s.document_id,
          s.title,
          s.lead,
          s.body,
          s.updated_at
 order by sum(f.score) desc,
          s.updated_at desc,
          s.search_document_id desc
 limit $11 offset $12";
