from unittest.mock import MagicMock

import pytest

from metabricks.bootstrap import load_builtin_plugins
from metabricks.models.pipeline_config import PipelineConfig


def setup_function() -> None:
    from metabricks.connectors.registry import ConnectorRegistry
    from metabricks.wiring.source_registry import SourceWiringRegistry

    ConnectorRegistry.clear()
    SourceWiringRegistry.clear()

    load_builtin_plugins(reload=True)


class FakeDF:
    def __init__(self, count_val: int):
        self._count_val = count_val

    def limit(self, _n: int):
        return self

    def count(self) -> int:
        return self._count_val


class FakeSpark:
    def __init__(self, df: FakeDF):
        self._df = df
        self.last_query = None
        self.last_args = None

    def sql(self, query, args=None):
        self.last_query = query
        self.last_args = args
        return self._df


def test_orchestrator_runs_databricks_batch(monkeypatch):
    from metabricks.orchestrator import run_pipeline

    df = FakeDF(1)
    spark = FakeSpark(df)

    # Patch SparkSession.getActiveSession inside the connector module
    import metabricks.connectors.databricks.batch as batch_mod

    monkeypatch.setattr(batch_mod.SparkSession, "getActiveSession", classmethod(lambda cls: spark))

    cfg = PipelineConfig(
        pipeline_name="p",
        source={
            "system_type": "databricks",
            "extraction_mode": "batch",
            "connection": {"system_type": "databricks"},
            "source_query": "SELECT * FROM t WHERE ds = :logical_date",
            "query_args": {"logical_date": "2025-01-01"},
        },
        sink=None,
    )

    env = run_pipeline("inv-1", cfg)

    assert env.payload_type == "dataframe"
    assert env.data is df
    assert env.context.run_id == "inv-1"
    assert spark.last_args == {"logical_date": "2025-01-01"}


def test_orchestrator_databricks_batch_raises_on_empty_snapshot(monkeypatch):
    from metabricks.orchestrator import run_pipeline

    df = FakeDF(0)
    spark = FakeSpark(df)

    import metabricks.connectors.databricks.batch as batch_mod

    monkeypatch.setattr(batch_mod.SparkSession, "getActiveSession", classmethod(lambda cls: spark))

    cfg = PipelineConfig(
        pipeline_name="p",
        source={
            "system_type": "databricks",
            "extraction_mode": "batch",
            "connection": {"system_type": "databricks"},
            "source_query": "SELECT 1",
            "query_args": {"logical_date": "2025-01-01"},
        },
        sink=None,
    )

    from metabricks.core.exceptions import NoSourceDataError
    
    with pytest.raises(NoSourceDataError, match="No source data found"):
        run_pipeline("inv-1", cfg)
