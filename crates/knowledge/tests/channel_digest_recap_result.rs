//! Model-authored versus system-composed fields of the stored channel recap (XR-021
//! CONTRACTS.md S08).

use ratatoskr_knowledge::test_support::recap::{
    fixture_source, model_output, provider_response, seed_recap_run,
};
use ratatoskr_knowledge::test_support::{TemporaryBlobRoot, TestDatabase};
use ratatoskr_knowledge::{
    BlobStore, ChannelRecapContextPolicy, ChannelRecapOutputLanguage, ChannelRecapPipeline,
    ChannelRecapWarning, ScriptedProvider, build_channel_recap_provider_request,
    prepare_channel_recap_context,
};

fn policy(max_characters: usize) -> ChannelRecapContextPolicy {
    ChannelRecapContextPolicy {
        max_sources: 100,
        max_channels: 20,
        max_characters,
    }
}

/// A real model returns only the text it authored. The stored recap still carries the run's
/// manifest digest, the requested language and the exact coverage, and passes the existing
/// validator.
#[tokio::test]
async fn model_output_without_digest_language_or_coverage_composes_and_validates()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let seeded = seed_recap_run(
        &database.database,
        vec![fixture_source(600, 1, "bounded composition fixture")],
        "ru",
    )
    .await?;
    let context = prepare_channel_recap_context(&seeded.verified, policy(100_000))?;
    let provider = ScriptedProvider::new([Ok(provider_response(
        &model_output(&context, &[]),
        "scripted-compose",
    )?)]);
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    let pipeline = ChannelRecapPipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    );

    let recap = pipeline
        .execute(
            seeded.run_id,
            build_channel_recap_provider_request(&context, ChannelRecapOutputLanguage::Ru, 2_048)?,
            &context,
            &seeded.verified.digest_hex,
            ChannelRecapOutputLanguage::Ru,
        )
        .await?;

    assert_eq!(provider.call_count()?, 1);
    assert_eq!(
        recap.manifest_digest.hex.as_str(),
        seeded.verified.digest_hex
    );
    assert_eq!(recap.output_language, ChannelRecapOutputLanguage::Ru);
    assert_eq!(recap.coverage.selected_count, 1);
    assert_eq!(recap.coverage.included_count, 1);
    assert_eq!(recap.coverage.omitted_count, 0);
    assert_eq!(recap.coverage.channel_count, 1);
    assert!(recap.warnings.is_empty());
    let stored: serde_json::Value = sqlx::query_scalar(
        "select result from knowledge.channel_recap_results where recap_run_id = $1",
    )
    .bind(seeded.run_id)
    .fetch_one(database.database.pool())
    .await?;
    assert_eq!(stored, serde_json::to_value(&recap)?);
    assert_eq!(stored["manifest_digest"]["hex"], seeded.verified.digest_hex);
    assert_eq!(stored["output_language"], "ru");
    database.cleanup().await?;
    Ok(())
}

/// Omitted sources are a system fact: the warning is added when the model did not say so and is
/// not duplicated when it did.
#[tokio::test]
async fn omitted_sources_add_the_context_omitted_warning_deterministically()
-> Result<(), Box<dyn std::error::Error>> {
    for model_warnings in [Vec::new(), vec!["context_omitted_sources"]] {
        let database = TestDatabase::create().await?;
        let seeded = seed_recap_run(
            &database.database,
            vec![
                fixture_source(610, 1, "highest priority complete source"),
                fixture_source(611, 1, "lower priority complete source"),
            ],
            "en",
        )
        .await?;
        let full = prepare_channel_recap_context(&seeded.verified, policy(100_000))?;
        let one_source = serde_json::to_string(full.sources.first().ok_or("no prepared source")?)?
            .chars()
            .count();
        let context = prepare_channel_recap_context(&seeded.verified, policy(one_source))?;
        assert_eq!(context.omitted_count, 1);
        let provider = ScriptedProvider::new([Ok(provider_response(
            &model_output(&context, &model_warnings),
            "scripted-omitted",
        )?)]);
        let root = TemporaryBlobRoot::create().await?;
        let blobs = BlobStore::new(root.path(), 65_536);
        let recap = ChannelRecapPipeline::new(
            &database.database,
            &provider,
            &blobs,
            std::time::Duration::from_secs(1),
        )
        .execute(
            seeded.run_id,
            build_channel_recap_provider_request(&context, ChannelRecapOutputLanguage::En, 2_048)?,
            &context,
            &seeded.verified.digest_hex,
            ChannelRecapOutputLanguage::En,
        )
        .await?;

        assert_eq!(
            recap.warnings,
            vec![ChannelRecapWarning::ContextOmittedSources]
        );
        assert_eq!(recap.coverage.selected_count, 2);
        assert_eq!(recap.coverage.included_count, 1);
        assert_eq!(recap.coverage.omitted_count, 1);
        database.cleanup().await?;
    }
    Ok(())
}

/// The provider sees the requested language and a schema that asks only for model-authored
/// fields.
#[tokio::test]
async fn provider_request_carries_the_requested_language_and_a_model_output_schema_without_digest_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let seeded = seed_recap_run(
        &database.database,
        vec![
            fixture_source(620, 1, "first language fixture"),
            fixture_source(621, 2, "second language fixture"),
        ],
        "ru",
    )
    .await?;
    let context = prepare_channel_recap_context(&seeded.verified, policy(100_000))?;
    let request =
        build_channel_recap_provider_request(&context, ChannelRecapOutputLanguage::Ru, 2_048)?;
    assert_eq!(request.output_language, ChannelRecapOutputLanguage::Ru);

    let provider = ScriptedProvider::new([Ok(provider_response(
        &model_output(&context, &[]),
        "scripted-language",
    )?)]);
    let root = TemporaryBlobRoot::create().await?;
    let blobs = BlobStore::new(root.path(), 65_536);
    // The outcome is not the subject here; the request the provider received is.
    let _outcome = ChannelRecapPipeline::new(
        &database.database,
        &provider,
        &blobs,
        std::time::Duration::from_secs(1),
    )
    .execute(
        seeded.run_id,
        request,
        &context,
        &seeded.verified.digest_hex,
        ChannelRecapOutputLanguage::Ru,
    )
    .await;

    let requests = provider.requests()?;
    let sent = requests.first().ok_or("the provider was not called")?;
    let properties = sent.output_schema["properties"]
        .as_object()
        .ok_or("the output schema has no properties")?;
    for authored in [
        "headline",
        "overview",
        "topics",
        "notable_items",
        "warnings",
    ] {
        assert!(properties.contains_key(authored), "missing {authored}");
    }
    for system_owned in [
        "manifest_digest",
        "coverage",
        "output_language",
        "contract_version",
        "prompt_version",
        "context_version",
    ] {
        assert!(
            !properties.contains_key(system_owned),
            "the model must not be asked for {system_owned}"
        );
    }
    let source_json: serde_json::Value = serde_json::from_str(&sent.source_content)?;
    assert_eq!(source_json["output_language"], "ru");
    assert_eq!(source_json["included_source_count"], 2);
    database.cleanup().await?;
    Ok(())
}
