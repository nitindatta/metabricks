from unittest.mock import MagicMock, patch

import httpx
import pytest

from metabricks.connectors.registry import ConnectorRegistry, ConnectorRegistryError
from metabricks.bootstrap import load_builtin_plugins
from metabricks.models.pipeline_config import PipelineConfig
from metabricks.wiring.source_registry import SourceWiringRegistry, SourceWiringRegistryError


def setup_function() -> None:
    # keep tests isolated
    ConnectorRegistry.clear()
    SourceWiringRegistry.clear()

    # Re-import builtins so decorators re-register after the clears.
    load_builtin_plugins(reload=True)


def test_run_pipeline_api_batch_sets_run_id_and_pipeline_hints():
    from metabricks.orchestrator import run_pipeline

    http_client = MagicMock(spec=httpx.Client)

    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"ok": True}
    http_client.request.return_value = response

    cfg = PipelineConfig(
        pipeline_name="p1",
        source={
            "system_type": "api",
            "extraction_mode": "batch",
            "connection": {
                "system_type": "api",
                "base_url": "https://example.test",
            },
            "endpoint": "/v1/data",
        },
        sink=None,
    )

    with patch('httpx.Client', return_value=http_client):
        env = run_pipeline("inv-123", cfg)

    assert env.context.run_id == "inv-123"
    assert env.context.pipeline_name == "p1"


def test_run_pipeline_raises_for_unregistered_connector():
    from metabricks.orchestrator import run_pipeline

    # Ensure we simulate a true "nothing registered" state. run_pipeline() will
    # call load_builtin_plugins(), but the bootstrap module is already marked as
    # loaded, so it won't re-import/auto-register again.
    ConnectorRegistry.clear()
    SourceWiringRegistry.clear()

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

    with pytest.raises(ConnectorRegistryError):
        run_pipeline("inv-1", cfg)


def test_run_pipeline_not_implemented_for_non_api_system():
    # Create a valid non-api source config; orchestrator should fail clearly due to missing wiring.
    from metabricks.orchestrator import run_pipeline

    cfg = PipelineConfig(
        pipeline_name="p",
        source={
            "system_type": "kafka",
            "extraction_mode": "streaming",
            "connection": {"system_type": "kafka", "bootstrap_servers": "localhost:9092"},
            "topic": "t",
        },
        sink=None,
    )

    with pytest.raises((SourceWiringRegistryError, ConnectorRegistryError, RuntimeError)):
        run_pipeline("inv-1", cfg)
