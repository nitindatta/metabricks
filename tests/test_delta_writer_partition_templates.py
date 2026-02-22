from __future__ import annotations

from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame

from metabricks.core.contracts import DataEnvelope, ExecutionContext
from metabricks.models.delta_write_options import DeltaBatchWriteOptions
from metabricks.sinks.strategies.delta_writer import DeltaWriterStrategy


def test_delta_writer_builds_replace_where_from_overwrite_scope():
    df = MagicMock(spec=DataFrame)
    writer = MagicMock()
    writer.option.return_value = writer
    df.write.format.return_value.mode.return_value = writer

    env = DataEnvelope(
        payload_type="dataframe",
        data=df,
        schema=None,
        context=ExecutionContext(run_id="r1", pipeline_name="p1"),
    )

    strategy = DeltaWriterStrategy()

    with patch("metabricks.sinks.strategies.delta_writer._try_delta_operation_metrics", return_value=None):
        strategy.write(
            env,
            {
                "spark": None,
                "table_name": "catalog.schema.table",
                "overwrite_scope": [{"snapshot_date": "2026-01-19"}],
                "write_options": DeltaBatchWriteOptions(mode="overwrite"),
            },
        )

    (key, value), _ = writer.option.call_args
    assert key == "replaceWhere"
    assert "snapshot_date='2026-01-19'" in value
