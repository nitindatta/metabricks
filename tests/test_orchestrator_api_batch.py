from unittest.mock import MagicMock, patch

import httpx

from metabricks.models.pipeline_config import PipelineConfig


def test_orchestrator_runs_api_batch_and_returns_envelope():
    from metabricks.orchestrator import run_pipeline

    http_client = MagicMock(spec=httpx.Client)

    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"ok": True}
    http_client.request.return_value = response

    cfg = PipelineConfig(
        pipeline_name="p",
        source={
            "system_type": "api",
            "extraction_mode": "batch",
            "connection": {"system_type": "api", "base_url": "https://example.test"},
            "endpoint": "/v1/data",
        },
        sink=None,
    )

    with patch('httpx.Client', return_value=http_client):
        env = run_pipeline("inv-1", cfg)

    http_client.request.assert_called_once_with("GET", "/v1/data")
    assert env.payload_type == "json"
    assert env.data == {"ok": True}


def test_orchestrator_api_batch_writes_json_to_filesystem_volume_path(tmp_path):
    """API batch -> filesystem file (json) through orchestrator."""

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

    assert env.context.environment == "dev"
    assert env.context.source_system == "some_api"
    assert env.context.source_dataset_id == "dataset_123"

    audit = env.context.metadata["sink_audit"]
    assert audit["status"] == "success"
    assert audit["record_count"] == 2

    expected = tmp_path / "landing" / "api" / "dataset_123" / "data.json"
    assert expected.exists()


def test_orchestrator_api_batch_with_secrets_provider_for_api_key():
    """Test that secrets_provider is used to resolve API key when api_key_value is None."""
    from metabricks.orchestrator import run_pipeline
    from metabricks.core.secrets_provider import SecretsProvider

    # Mock secrets provider
    secrets_provider = MagicMock(spec=SecretsProvider)
    secrets_provider.get_secret.return_value = "secret-api-key"

    http_client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"ok": True}
    http_client.request.return_value = response

    cfg = PipelineConfig(
        pipeline_name="p",
        source={
            "system_type": "api",
            "extraction_mode": "batch",
            "connection": {
                "base_url": "https://example.test",
                "auth": {
                    "kind": "api_key",
                    "api_key_name": "my_api_key",
                    # api_key_value is None, so should resolve from secrets
                    "api_key_secret_ref": {"vault_ref": "api_keys", "secret_key": "my_api_key"},
                }
            },
            "endpoint": "/v1/data",
        },
        sink=None,
    )

    with patch('httpx.Client', return_value=http_client):
        env = run_pipeline("inv-1", cfg, secrets_provider=secrets_provider)

    # Verify secrets_provider was called
    secrets_provider.get_secret.assert_called_once_with("api_keys", "my_api_key")

    # Verify the request was made (meaning auth was resolved)
    http_client.request.assert_called_once_with("GET", "/v1/data")
    assert env.payload_type == "json"
