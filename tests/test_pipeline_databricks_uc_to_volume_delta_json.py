from __future__ import annotations

from pathlib import Path

from metabricks.bootstrap import load_builtin_plugins
from metabricks.models.pipeline_config import PipelineConfig


def setup_function() -> None:
    # Keep this integration test hermetic: clear registries and re-register builtins.
    from metabricks.connectors.registry import ConnectorRegistry
    from metabricks.sinks.registry import SinkRegistry
    from metabricks.wiring.source_registry import SourceWiringRegistry
    from metabricks.wiring.sink_registry import SinkWiringRegistry

    ConnectorRegistry.clear()
    SourceWiringRegistry.clear()
    SinkRegistry.clear()
    SinkWiringRegistry.clear()

    load_builtin_plugins(reload=True)


class _FakeLimitedDF:
    def __init__(self, snapshot_count: int):
        self._snapshot_count = snapshot_count

    def count(self) -> int:
        return self._snapshot_count


class _FakeWriter:
    def __init__(self):
        self.saved_table: str | None = None
        self.saved_path: str | None = None
        self.options: dict[str, str] = {}
        self.mode_value: str | None = None
        self.format_value: str | None = None

    def format(self, fmt: str):
        self.format_value = fmt
        return self

    def mode(self, mode: str):
        self.mode_value = mode
        return self

    def option(self, key: str, value: str):
        self.options[key] = value
        return self

    def partitionBy(self, *_cols: str):
        return self

    def saveAsTable(self, table_name: str) -> None:
        self.saved_table = table_name

    def save(self, path: str) -> None:
        self.saved_path = path


class _FakeDF:
    def __init__(self, *, row_count: int = 10, snapshot_count: int = 1):
        self._row_count = row_count
        self._snapshot_count = snapshot_count
        self.write = _FakeWriter()

    def limit(self, _n: int):
        return _FakeLimitedDF(self._snapshot_count)

    def count(self) -> int:
        return self._row_count


class _FakeSpark:
    def __init__(self, df: _FakeDF):
        self._df = df
        self.last_query: str | None = None
        self.last_args: dict | None = None

    def sql(self, query: str, args=None):
        self.last_query = query
        self.last_args = args
        return self._df


def test_pipeline_config_json_databricks_uc_to_volume_delta(monkeypatch):
    from metabricks.orchestrator import run_pipeline

    cfg_path = Path(__file__).parent / "data" / "pipeline_databricks_uc_to_volume_delta.json"
    cfg = PipelineConfig.model_validate_json(cfg_path.read_text())

    df = _FakeDF(row_count=42, snapshot_count=1)
    spark = _FakeSpark(df)

    # Patch SparkSession.getActiveSession inside connector & sink modules.
    import metabricks.connectors.databricks.batch as batch_connector_mod
    import metabricks.sinks.databricks.batch as batch_sink_mod

    monkeypatch.setattr(
        batch_connector_mod.SparkSession,
        "getActiveSession",
        classmethod(lambda cls: spark),
    )
    monkeypatch.setattr(
        batch_sink_mod.SparkSession,
        "getActiveSession",
        classmethod(lambda cls: spark),
    )

    # Ensure delta metrics lookup doesn't pull external deps.
    import metabricks.sinks.strategies.delta_writer as delta_writer_mod

    monkeypatch.setattr(delta_writer_mod, "_try_delta_operation_metrics", lambda *_args, **_kwargs: None)

    env = run_pipeline("inv-it-2", cfg)

    assert env.payload_type == "dataframe"
    assert env.data is df

    # Source side ran the UC query with logical_date.
    assert spark.last_query == "SELECT * FROM main.source_schema.source_table WHERE ds = :logical_date"
    assert spark.last_args == {"logical_date": "2025-01-01"}

    # Sink side wrote to a volume path using delta format.
    assert env.context.metadata["sink_audit"]["status"] == "success"
    assert df.write.format_value == "delta"
    assert df.write.mode_value == "overwrite"
    assert df.write.saved_path == "/tmp/metabricks_test_volume/delta_tables/dest_delta_table"
