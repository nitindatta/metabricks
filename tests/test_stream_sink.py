"""Tests for the forward-only Databricks streaming sink."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pyspark.sql import SparkSession

from metabricks.core.contracts import DataEnvelope, ExecutionContext
from metabricks.sinks.types import DatabricksSinkRuntimeConfig
from metabricks.systems.databricks.types import DatabricksCatalogTableTarget, DatabricksConnection
from metabricks.sinks.databricks.streaming import DatabricksStreamingSink


@pytest.fixture
def mock_spark() -> object:
    return MagicMock(spec=SparkSession)


@pytest.fixture
def stream_sink(mock_spark: object) -> DatabricksStreamingSink:
    cfg = DatabricksSinkRuntimeConfig(
        system_type="databricks",
        mode="streaming",
        format="delta",
        connection=DatabricksConnection(catalog="default_catalog", schema_name="default_schema"),
        target=DatabricksCatalogTableTarget(object_name="raw_stream_table"),
        checkpoint_path="/mnt/checkpoints/raw_stream",
    )
    return DatabricksStreamingSink(cfg, spark=mock_spark)


def _mock_streaming_df(*, query: object):
    df = MagicMock()
    ws = MagicMock()
    df.writeStream = ws

    ws.format.return_value = ws
    ws.outputMode.return_value = ws
    ws.option.return_value = ws
    ws.trigger.return_value = ws
    ws.queryName.return_value = ws
    ws.toTable.return_value = query
    return df, ws


def test_stream_sink_rejects_non_stream_payload(stream_sink: DatabricksStreamingSink):
    env = DataEnvelope(payload_type="json", data={"k": "v"}, schema=None, context=None)
    with pytest.raises(TypeError, match="stream payload"):
        stream_sink.write(env)


@pytest.mark.parametrize("payload_type", ["stream"])
def test_stream_sink_accepts_stream_payloads(stream_sink: DatabricksStreamingSink, payload_type: str):
    query = MagicMock()
    query.exception.return_value = None
    query.lastProgress = {"observedMetrics": {"numRecords": 123}}
    df, ws = _mock_streaming_df(query=query)

    context = ExecutionContext(run_id="inv-1", pipeline_name="test")
    env = DataEnvelope(payload_type=payload_type, data=df, schema=None, context=context)

    with patch(
        "metabricks.sinks.strategies.delta_stream_writer.StreamingQueryProgress.model_validate"
    ) as mock_validate:
        mock_progress = MagicMock()
        mock_progress.observed_metrics.get_record_count.return_value = 123
        mock_validate.return_value = mock_progress

        result = stream_sink.write(env)

    assert result["status"] == "success"
    assert result["record_count"] == 123

    # Verify core streaming writer chain.
    ws.format.assert_called_once_with("delta")
    ws.outputMode.assert_called_once_with("append")
    assert any(call.args[0] == "checkpointLocation" for call in ws.option.call_args_list)
    assert any(call.args[0] == "mergeSchema" for call in ws.option.call_args_list)
    ws.trigger.assert_called_once_with(availableNow=True)


def test_stream_sink_query_name_includes_table_and_run_id(stream_sink: DatabricksStreamingSink):
    query = MagicMock()
    query.exception.return_value = None
    query.lastProgress = {"observedMetrics": {"numRecords": 1}}
    df, ws = _mock_streaming_df(query=query)

    context = ExecutionContext(run_id="inv-xyz", pipeline_name="test")
    env = DataEnvelope(payload_type="stream", data=df, schema=None, context=context)

    with patch(
        "metabricks.sinks.strategies.delta_stream_writer.StreamingQueryProgress.model_validate"
    ) as mock_validate:
        mock_progress = MagicMock()
        mock_progress.observed_metrics.get_record_count.return_value = 1
        mock_validate.return_value = mock_progress

        stream_sink.write(env)

    qn = ws.queryName.call_args.args[0]
    assert "default_catalog.default_schema.raw_stream_table" in qn
    assert "inv-xyz" in qn


def test_stream_sink_adds_logical_date_when_audit_enabled(stream_sink: DatabricksStreamingSink):
    query = MagicMock()
    query.exception.return_value = None
    query.lastProgress = {"observedMetrics": {"numRecords": 1}}
    df, _ws = _mock_streaming_df(query=query)
    df.withColumn.return_value = df

    context = ExecutionContext(
        run_id="inv-ld-1",
        pipeline_name="test",
        attach_audit_meta=True,
        logical_date="2025-01-01",
    )
    env = DataEnvelope(payload_type="stream", data=df, schema=None, context=context)

    class _DummyCol:
        def cast(self, _typ):
            return self

    with patch("metabricks.sinks.databricks.streaming.F.lit", return_value=_DummyCol()), patch(
        "metabricks.sinks.strategies.delta_stream_writer.StreamingQueryProgress.model_validate"
    ) as mock_validate:
        mock_progress = MagicMock()
        mock_progress.observed_metrics.get_record_count.return_value = 1
        mock_validate.return_value = mock_progress

        stream_sink.write(env)

    assert any(call.args[0] == "_logical_date" for call in df.withColumn.call_args_list)


def test_stream_sink_raises_when_query_exception(stream_sink: DatabricksStreamingSink):
    query = MagicMock()
    query.exception.return_value = RuntimeError("boom")
    query.lastProgress = {"observedMetrics": {"numRecords": 1}}
    df, _ws = _mock_streaming_df(query=query)

    env = DataEnvelope(payload_type="stream", data=df, schema=None, context=None)
    with pytest.raises(RuntimeError, match="Streaming query failed"):
        stream_sink.write(env)


def test_stream_sink_raises_when_no_last_progress(stream_sink: DatabricksStreamingSink):
    query = MagicMock()
    query.exception.return_value = None
    query.lastProgress = None
    df, _ws = _mock_streaming_df(query=query)

    env = DataEnvelope(payload_type="stream", data=df, schema=None, context=None)
    with pytest.raises(RuntimeError, match="No lastProgress"):
        stream_sink.write(env)
