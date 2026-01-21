"""Tests for the forward-only Databricks sink."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pyspark.sql import DataFrame, SparkSession

from metabricks.core.contracts import DataEnvelope, ExecutionContext
from metabricks.sinks.types import DatabricksSinkRuntimeConfig
from metabricks.systems.databricks.types import DatabricksCatalogTableTarget, DatabricksConnection, DatabricksExternalLocationTarget, DatabricksVolumeTarget
from metabricks.sinks.databricks.batch import DatabricksSink


@pytest.fixture
def mock_spark() -> object:
    return MagicMock(spec=SparkSession)


def _df_and_writer(*, count_val: int = 1):
    df = MagicMock(spec=DataFrame)
    df.count.return_value = count_val

    writer = MagicMock()
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer

    df.write.format.return_value.mode.return_value = writer
    return df, writer


@pytest.fixture
def databricks_sink(mock_spark: object) -> DatabricksSink:
    cfg = DatabricksSinkRuntimeConfig(
        system_type="databricks",
        mode="batch",
        format="delta",
        connection=DatabricksConnection(catalog="default_catalog", schema_name="default_schema"),
        target=DatabricksCatalogTableTarget(object_name="test_table"),
        write_mode="overwrite",
    )
    return DatabricksSink(cfg, spark=mock_spark)


def test_sink_initializes_with_config(databricks_sink: DatabricksSink):
    assert databricks_sink.config.system_type == "databricks"
    assert isinstance(databricks_sink.config.target, DatabricksCatalogTableTarget)


def test_catalog_table_write_requires_dataframe_payload(databricks_sink: DatabricksSink):
    env = DataEnvelope(payload_type="json", data={"k": "v"}, schema=None, context=None)
    with pytest.raises(TypeError, match="dataframe payload"):
        databricks_sink.write(env)


def test_catalog_table_delta_write_success_uses_save_as_table(databricks_sink: DatabricksSink):
    df, writer = _df_and_writer(count_val=200)
    env = DataEnvelope(payload_type="dataframe", data=df, schema=None, context=None)

    with patch("metabricks.sinks.strategies.delta_writer._try_delta_operation_metrics", return_value=None):
        result = databricks_sink.write(env)

    assert result["kind"] == "delta"
    assert result["status"] == "success"
    assert result["record_count"] == 200
    assert result["target_location"] == "default_catalog.default_schema.test_table"

    writer.saveAsTable.assert_called_once_with("default_catalog.default_schema.test_table")


def test_catalog_table_overwrite_with_overwrite_scope_sets_replace_where(databricks_sink: DatabricksSink):
    databricks_sink.config.overwrite_scope = [
        {"year": "2025", "month": "01"},
        {"year": "2025", "month": "02"},
    ]
    databricks_sink.config.write_mode = "overwrite"

    df, writer = _df_and_writer(count_val=10)
    env = DataEnvelope(payload_type="dataframe", data=df, schema=None, context=None)

    with patch("metabricks.sinks.strategies.delta_writer._try_delta_operation_metrics", return_value=None):
        databricks_sink.write(env)

    writer.option.assert_called_once()
    (key, value), _ = writer.option.call_args
    assert key == "replaceWhere"
    assert "year='2025'" in value
    assert "month='01'" in value
    assert " OR " in value


def test_volume_json_envelope_writes_file(tmp_path):
    cfg = DatabricksSinkRuntimeConfig(
        system_type="databricks",
        mode="batch",
        format="json",
        connection=DatabricksConnection(),
        target=DatabricksVolumeTarget(volume_path=str(tmp_path), folder_path="raw", object_name="data.json"),
        format_options={"compression": "none"},
    )

    sink = DatabricksSink(cfg, spark=None)
    env = DataEnvelope(payload_type="json", data=[{"id": 1}], schema=None, context=None)

    with pytest.raises(TypeError, match="dataframe payload"):
        sink.write(env)


@pytest.mark.parametrize("fmt", ["parquet", "csv", "json"])
@pytest.mark.parametrize("write_mode", ["overwrite", "append"])
def test_volume_dataframe_writes_use_spark_file_writers(tmp_path, mock_spark: object, fmt: str, write_mode: str):
    cfg = DatabricksSinkRuntimeConfig(
        system_type="databricks",
        mode="batch",
        format=fmt,  # type: ignore[arg-type]
        connection=DatabricksConnection(),
        target=DatabricksVolumeTarget(volume_path=str(tmp_path), folder_path="raw", object_name="dataset"),
        write_mode=write_mode,
    )

    sink = DatabricksSink(cfg, spark=mock_spark)
    df, writer = _df_and_writer(count_val=5)
    env = DataEnvelope(payload_type="dataframe", data=df, schema=None, context=None)

    result = sink.write(env)

    assert result["kind"] == fmt
    assert result["status"] == "success"
    assert result["record_count"] == 5
    assert result["target_location"].endswith("/raw/dataset")

    df.write.format.assert_called_once_with(fmt)
    df.write.format.return_value.mode.assert_called_once_with(write_mode)
    writer.save.assert_called_once()


def test_external_location_delta_writes_to_path(mock_spark: object):
    cfg = DatabricksSinkRuntimeConfig(
        system_type="databricks",
        mode="batch",
        format="delta",
        connection=DatabricksConnection(),
        target=DatabricksExternalLocationTarget(path="s3://bucket/prefix/table"),
        write_mode="overwrite",
    )
    sink = DatabricksSink(cfg, spark=mock_spark)
    df, writer = _df_and_writer(count_val=7)
    env = DataEnvelope(payload_type="dataframe", data=df, schema=None, context=None)

    with patch("metabricks.sinks.strategies.delta_writer._try_delta_operation_metrics", return_value=None):
        result = sink.write(env)

    assert result["kind"] == "delta"
    assert result["target_location"] == "s3://bucket/prefix/table"
    writer.save.assert_called_once_with("s3://bucket/prefix/table")


def test_attach_meta_struct_when_hint_enabled(databricks_sink: DatabricksSink):
    """Test that _meta struct attachment is triggered when attach_audit_meta is True."""
    df = MagicMock(spec=DataFrame)
    
    # Create a mock for the withColumn call that returns a modified df
    df_with_meta = MagicMock(spec=DataFrame)
    df_with_meta.count.return_value = 3
    df.withColumn.return_value = df_with_meta
    
    writer = MagicMock()
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer
    df_with_meta.write.format.return_value.mode.return_value = writer
    
    # Create execution context with audit enabled
    context = ExecutionContext(
        run_id="inv-123-abc",
        pipeline_name="test_pipeline",
        batch_id="1",
        pipeline_version="v1.0.0",
        is_replay=False,
        record_hash_keys=["id", "timestamp"],
        attach_audit_meta=True,
    )
    env = DataEnvelope(payload_type="dataframe", data=df, schema=None, context=context)
    
    # Mock _attach_meta_struct to verify it's called
    with patch.object(databricks_sink, "_attach_meta_struct", return_value=df_with_meta) as mock_attach, \
         patch("metabricks.sinks.strategies.delta_writer._try_delta_operation_metrics", return_value=None):
        
        result = databricks_sink.write(env)
    
    # Verify _attach_meta_struct was called with the df and context
    mock_attach.assert_called_once()
    call_args = mock_attach.call_args
    assert call_args[0][1] == context  # Second argument is context
    
    # Verify write succeeded with the modified df
    assert result["kind"] == "delta"
    assert result["status"] == "success"
    writer.saveAsTable.assert_called_once()


def test_attach_meta_struct_skip_when_hint_disabled(databricks_sink: DatabricksSink):
    """Test that _meta struct is NOT attached when attach_audit_meta is False."""
    df = MagicMock(spec=DataFrame)
    df.count.return_value = 5
    
    writer = MagicMock()
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer
    df.write.format.return_value.mode.return_value = writer
    
    # Envelope with attach_audit_meta disabled
    context = ExecutionContext(
        run_id="inv-456-def",
        pipeline_name="test_pipeline",
        attach_audit_meta=False,
    )
    env = DataEnvelope(payload_type="dataframe", data=df, schema=None, context=context)
    
    with patch("metabricks.sinks.strategies.delta_writer._try_delta_operation_metrics", return_value=None):
        result = databricks_sink.write(env)
    
    # Verify withColumn was NOT called
    df.withColumn.assert_not_called()
    
    # Verify write proceeded normally without _meta
    assert result["kind"] == "delta"
    assert result["status"] == "success"
    writer.saveAsTable.assert_called_once()
