//! Fixtures shared by the channel-recap manifest and admission test binaries.

pub(crate) fn request_json() -> serde_json::Value {
    serde_json::json!({
        "operation_id": "018f0000-0000-7000-8000-000000000201",
        "owner": "user:018f0000-0000-7000-8000-000000000202",
        "digest_run_id": "018f0000-0000-7000-8000-000000000203",
        "window": {
            "start_at": "2026-08-20T10:00:00Z",
            "end_at": "2026-08-21T10:00:00Z"
        },
        "output_language": "ru",
        "source_count": 12,
        "channel_count": 3,
        "manifest_ref": "channel-digest-manifest:018f0000-0000-7000-8000-000000000204",
        "manifest_digest": {
            "algorithm": "sha256",
            "hex": "0000000000000000000000000000000000000000000000000000000000000000"
        },
        "analysis_family": "channel_digest_recap",
        "analysis_contract": "channel_digest_recap.v1"
    })
}

pub(crate) fn command_json() -> serde_json::Value {
    serde_json::json!({
        "command_id": "018f0000-0000-7000-8000-000000000205",
        "command_type": "knowledge.channel_digest_recap.requested.v1",
        "issued_at": "2026-08-21T10:00:01Z",
        "producer": "ratatoskr-channel-digests",
        "aggregate_id": "channel-digest-run:018f0000-0000-7000-8000-000000000203",
        "correlation_id": "operation:018f0000-0000-7000-8000-000000000201",
        "tenant_id": "user:018f0000-0000-7000-8000-000000000202",
        "schema_version": 1,
        "payload": request_json()
    })
}
