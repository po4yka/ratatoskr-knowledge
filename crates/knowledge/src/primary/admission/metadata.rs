use super::*;

pub(super) fn social_metadata(
    producer: &str,
    snapshot: &ratatoskr_social_contracts::SocialSourceSnapshot,
    lifecycle: LifecycleFact,
) -> Result<MetadataTuple, &'static str> {
    let expected = match snapshot.platform.as_str() {
        "x" => "ratatoskr-x",
        "instagram" => "ratatoskr-instagram",
        "threads" => "ratatoskr-threads",
        _ => return Err("payload"),
    };
    if producer != expected {
        return Err("producer");
    }
    let key = snapshot.social_source_id.to_string();
    Ok((
        "social",
        snapshot.owner.to_string(),
        key.clone(),
        snapshot.content_digest.hex.to_string(),
        lifecycle,
        true,
        format!("social_source:{key}"),
    ))
}

fn archive_producer(producer: &str, provider: &impl std::fmt::Display) -> Result<(), &'static str> {
    let expected = match provider.to_string().as_str() {
        "chatgpt" => "ratatoskr-chatgpt",
        "claude" => "ratatoskr-claude",
        _ => return Err("payload"),
    };
    (producer == expected).then_some(()).ok_or("producer")
}

pub(super) fn archive_import_metadata(
    producer: &str,
    payload: &AiArchiveImport,
) -> Result<MetadataTuple, &'static str> {
    archive_producer(producer, &payload.provider)?;
    let key = payload.ai_archive_id.to_string();
    Ok((
        "ai_archive",
        payload.owner.to_string(),
        key.clone(),
        payload.source_export.digest.hex.to_string(),
        LifecycleFact::Active,
        false,
        format!("ai_archive:{key}"),
    ))
}

pub(super) fn archive_conversation_metadata(
    producer: &str,
    provenance: &ratatoskr_ai_archive_contracts::AiArchiveProvenance,
    conversation: &ratatoskr_ai_archive_contracts::AiConversation,
) -> Result<MetadataTuple, &'static str> {
    archive_producer(producer, &provenance.provider)?;
    let key = conversation.ai_conversation_id.to_string();
    Ok((
        "ai_archive",
        conversation.owner.to_string(),
        key,
        conversation.content_digest.hex.to_string(),
        LifecycleFact::Active,
        true,
        format!("ai_archive:{}", provenance.ai_archive_id),
    ))
}

pub(super) fn archive_project_metadata(
    producer: &str,
    provenance: &ratatoskr_ai_archive_contracts::AiArchiveProvenance,
    project: &ratatoskr_ai_archive_contracts::AiProject,
    digest: &ratatoskr_identifiers::ContentDigest,
) -> Result<MetadataTuple, &'static str> {
    provenance
        .validate_project(project)
        .map_err(|_| "payload")?;
    archive_producer(producer, &provenance.provider)?;
    Ok((
        "ai_archive",
        provenance.owner.to_string(),
        project.ai_project_id.to_string(),
        digest.hex.to_string(),
        LifecycleFact::Active,
        true,
        format!("ai_archive:{}", provenance.ai_archive_id),
    ))
}

pub(super) fn archive_artifact_metadata(
    producer: &str,
    provenance: &ratatoskr_ai_archive_contracts::AiArchiveProvenance,
    artifact: &ratatoskr_ai_archive_contracts::AiArtifact,
) -> Result<MetadataTuple, &'static str> {
    archive_producer(producer, &provenance.provider)?;
    if artifact.owner != provenance.owner
        || artifact.provider != provenance.provider
        || artifact.parser_name != provenance.parser_name
        || artifact.parser_version != provenance.parser_version
        || artifact.content_blob.owner_service.as_str() != producer
        || provenance.source_export.owner_service.as_str() != producer
        || artifact.content_blob.digest != artifact.content_digest
    {
        return Err("payload");
    }
    Ok((
        "ai_archive",
        artifact.owner.to_string(),
        artifact.external_artifact_id.to_string(),
        digest(
            serde_json::to_vec(artifact)
                .map_err(|_| "payload")?
                .as_slice(),
        ),
        LifecycleFact::Active,
        false,
        format!("ai_archive:{}", provenance.ai_archive_id),
    ))
}

pub(super) fn archive_tombstone_metadata(
    producer: &str,
    tombstone: &AiArchiveTombstone,
) -> Result<MetadataTuple, &'static str> {
    archive_producer(producer, &tombstone.provider)?;
    if tombstone.evidence_ref.owner_service.as_str() != producer {
        return Err("payload");
    }
    let key = match &tombstone.subject {
        AiArchiveTombstoneSubject::Archive => tombstone.ai_archive_id.to_string(),
        AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => {
            ai_conversation_id.to_string()
        }
        AiArchiveTombstoneSubject::Project { ai_project_id } => ai_project_id.to_string(),
        AiArchiveTombstoneSubject::Artifact {
            external_artifact_id,
        } => external_artifact_id.to_string(),
    };
    Ok((
        "ai_archive",
        tombstone.owner.to_string(),
        key,
        tombstone.evidence_ref.digest.hex.to_string(),
        LifecycleFact::Removed,
        false,
        format!("ai_archive:{}", tombstone.ai_archive_id),
    ))
}
