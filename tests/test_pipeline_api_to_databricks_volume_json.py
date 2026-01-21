from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx

from metabricks.models.pipeline_config import PipelineConfig


def test_pipeline_api_batch_writes_json_to_filesystem_volume_path(tmp_path):
    """API batch -> filesystem file (json) end-to-end through orchestrator.

    Uses tmp_path as a volume-like root so no Spark is required.
    """

    from metabricks.orchestrator import run_pipeline

    http_client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = [{"id": 1}, {"id": 2}]
    http_client.request.return_value = response

    cfg = PipelineConfig(
        pipeline_name="api_to_uc_volume_json",
        job_name="api_to_uc_volume_json_job",
        environment="dev",
        source_system="some_api",
        source_dataset_id="dataset_123",
        source={
            "system_type": "api",
            "extraction_mode": "batch",
            "format": "json",
            "connection": {
                "base_url": "https://example.test",
                "timeout_seconds": 30.0,
                "headers": {"Accept": "application/json"},
                "auth": {"kind": "none"},
            },
            "endpoint": "/v1/data",
            "method": "GET",
        },
        sink={
            "system_type": "filesystem",
            "mode": "batch",
            "format": "json",
            "root": str(tmp_path),
            "folder_path": "landing/api/dataset_123",
            "object_name": "data.json",
            "compression": "none",
        },
    )

    with patch('httpx.Client', return_value=http_client):
        env = run_pipeline("inv-1", cfg)

    http_client.request.assert_called_once_with("GET", "/v1/data")

    # Orchestrator should attach pipeline metadata and sink audit.
    assert env.context.run_id == "inv-1"
    assert env.context.pipeline_name == "api_to_uc_volume_json"
    assert env.context.environment == "dev"
    assert env.context.source_system == "some_api"
    assert env.context.source_dataset_id == "dataset_123"

    audit = env.context.metadata["sink_audit"]
    assert audit["status"] == "success"
    assert audit["record_count"] == 2

    expected = tmp_path / "landing" / "api" / "dataset_123" / "data.json"
    assert expected.exists()
