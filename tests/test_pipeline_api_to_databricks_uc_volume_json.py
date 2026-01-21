from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx

from metabricks.bootstrap import load_builtin_plugins
from metabricks.models.pipeline_config import PipelineConfig


def setup_function() -> None:
    from metabricks.connectors.registry import ConnectorRegistry
    from metabricks.sinks.registry import SinkRegistry
    from metabricks.wiring.source_registry import SourceWiringRegistry
    from metabricks.wiring.sink_registry import SinkWiringRegistry

    ConnectorRegistry.clear()
    SourceWiringRegistry.clear()
    SinkRegistry.clear()
    SinkWiringRegistry.clear()

    load_builtin_plugins(reload=True)


def test_pipeline_config_json_api_to_databricks_uc_volume_json(tmp_path):
    from metabricks.orchestrator import run_pipeline

    cfg_path = Path(__file__).parent / "data" / "pipeline_api_to_databricks_uc_volume_json.json"
    cfg = PipelineConfig.model_validate_json(cfg_path.read_text())

    # Keep filesystem writes hermetic.
    assert cfg.sink is not None
    cfg.sink.root = str(tmp_path)  # type: ignore[attr-defined]

    http_client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = [{"id": 1}, {"id": 2}]
    http_client.request.return_value = response

    with patch("httpx.Client", return_value=http_client):
        env = run_pipeline("inv-it-api-1", cfg)

    http_client.request.assert_called_once_with("GET", "/v1/data")

    audit = env.context.metadata["sink_audit"]
    assert audit["status"] == "success"
    assert audit["record_count"] == 2

    expected = tmp_path / "landing" / "api" / "dataset_123" / "data.json"
    assert expected.exists()
