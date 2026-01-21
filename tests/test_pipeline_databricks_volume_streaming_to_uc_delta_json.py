from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from metabricks.bootstrap import load_builtin_plugins
from metabricks.models.pipeline_config import PipelineConfig


def setup_function() -> None:
    # Hermetic integration test: reset registries and reload builtins.
    from metabricks.connectors.registry import ConnectorRegistry
    from metabricks.sinks.registry import SinkRegistry
    from metabricks.wiring.source_registry import SourceWiringRegistry
    from metabricks.wiring.sink_registry import SinkWiringRegistry

    ConnectorRegistry.clear()
    SourceWiringRegistry.clear()
    SinkRegistry.clear()
    SinkWiringRegistry.clear()

    load_builtin_plugins(reload=True)


class _FakeStreamingQuery:
    def __init__(self):
        self.awaited = False
        self._exception = None
        self.lastProgress = {"observedMetrics": {"numRecords": 7}}

    def awaitTermination(self):
        self.awaited = True

    def exception(self):
        return self._exception


class _FakeWriteStream:
    def __init__(self, query: _FakeStreamingQuery):
        self._query = query
        self.calls: list[tuple[str, tuple, dict]] = []
        self.query_name: str | None = None
        self.table_name: str | None = None
        self.output_mode: str | None = None
        self.format_value: str | None = None
        self.options: dict[str, str] = {}
        self.trigger_kwargs: dict | None = None

    def format(self, fmt: str):
        self.format_value = fmt
        self.calls.append(("format", (fmt,), {}))
        return self

    def outputMode(self, mode: str):
        self.output_mode = mode
        self.calls.append(("outputMode", (mode,), {}))
        return self

    def option(self, key: str, value: str):
        self.options[key] = value
        self.calls.append(("option", (key, value), {}))
        return self

    def trigger(self, **kwargs):
        self.trigger_kwargs = kwargs
        self.calls.append(("trigger", (), kwargs))
        return self

    def queryName(self, name: str):
        self.query_name = name
        self.calls.append(("queryName", (name,), {}))
        return self

    def toTable(self, table_name: str):
        self.table_name = table_name
        self.calls.append(("toTable", (table_name,), {}))
        return self._query


class _FakeStreamDF:
    def __init__(self, query: _FakeStreamingQuery):
        self.writeStream = _FakeWriteStream(query)


class _FakeReadStream:
    def __init__(self, df: _FakeStreamDF):
        self._df = df
        self.fmt: str | None = None
        self.opts: dict[str, str] = {}
        self.loaded_path: str | None = None

    def format(self, fmt: str):
        self.fmt = fmt
        return self

    def option(self, key: str, value: str):
        self.opts[key] = value
        return self

    def load(self, path: str):
        self.loaded_path = path
        return self._df


class _FakeSpark:
    def __init__(self, df: _FakeStreamDF):
        self.readStream = _FakeReadStream(df)


def test_pipeline_config_json_databricks_volume_streaming_to_uc_delta(monkeypatch):
    from metabricks.orchestrator import run_pipeline

    cfg_path = Path(__file__).parent / "data" / "pipeline_databricks_volume_streaming_to_uc_delta.json"
    cfg = PipelineConfig.model_validate_json(cfg_path.read_text())

    query = _FakeStreamingQuery()
    df = _FakeStreamDF(query)
    spark = _FakeSpark(df)

    # Patch SparkSession.getActiveSession inside connector & sink modules.
    import metabricks.connectors.databricks.streaming as streaming_connector_mod
    import metabricks.sinks.databricks.streaming as streaming_sink_mod

    monkeypatch.setattr(
        streaming_connector_mod.SparkSession,
        "getActiveSession",
        classmethod(lambda cls: spark),
    )
    monkeypatch.setattr(
        streaming_sink_mod.SparkSession,
        "getActiveSession",
        classmethod(lambda cls: spark),
    )

    # Avoid coupling this test to StreamingQueryProgress parsing.
    with patch(
        "metabricks.sinks.strategies.delta_stream_writer.StreamingQueryProgress.model_validate"
    ) as mock_validate:
        mock_progress = MagicMock()
        mock_progress.observed_metrics.get_record_count.return_value = 7
        mock_validate.return_value = mock_progress

        env = run_pipeline("inv-it-stream-1", cfg)

    # Connector side: cloudFiles autoloader used.
    rs = spark.readStream
    assert rs.fmt == "cloudFiles"
    assert rs.loaded_path == "/Volumes/main/raw/source_volume/landing/events"
    assert rs.opts["cloudFiles.format"] == "json"
    assert rs.opts["cloudFiles.schemaLocation"] == "/Volumes/main/raw/source_volume/_schemas/events"
    assert rs.opts["cloudFiles.inferColumnTypes"] == "true"

    # Sink side: delta stream writer used append-only.
    audit = env.context.metadata["sink_audit"]
    assert audit["status"] == "success"
    assert audit["mode"] == "append"
    assert audit["target_location"] == "main.bronze.events"

    ws = df.writeStream
    assert ws.format_value == "delta"
    assert ws.output_mode == "append"
    assert ws.options["checkpointLocation"] == "/Volumes/main/raw/source_volume/_checkpoints/events_to_bronze"
    assert ws.table_name == "main.bronze.events"

    # availableNow trigger is a strong signal we're using autoloader style.
    assert ws.trigger_kwargs == {"availableNow": True}
    assert query.awaited is True
