"""Unit tests for sink configuration with write options.

Tests cover:
- DatabricksSinkConfig write_options defaults
- DatabricksSinkRuntimeConfig smart factory
- Batch vs streaming mode routing
- Configuration validation
"""

import pytest
from pydantic import ValidationError

from metabricks.models.sink_config import DatabricksSinkConfig
from metabricks.models.connection_config import DatabricksConnectionConfig
from metabricks.models.delta_write_options import DeltaBatchWriteOptions, DeltaStreamWriteOptions
from metabricks.sinks.types import DatabricksSinkRuntimeConfig
from metabricks.systems.databricks.types import (
    DatabricksCatalogTableTarget,
    DatabricksVolumeTarget,
    DatabricksExternalLocationTarget,
    DatabricksConnection,
)


class TestDatabricksSinkConfigDefaults:
    """Test write_options defaults in sink configuration."""

    def test_batch_config_gets_default_batch_write_options(self):
        """Test batch sink config creates default batch write options."""
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="customers",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="silver"
            ),
        )
        assert config.delta_write_options is not None
        assert isinstance(config.delta_write_options, DeltaBatchWriteOptions)
        assert config.delta_write_options.mode == "overwrite"
        assert config.delta_write_options.merge_schema is False

    def test_batch_config_append_mode_sets_correct_default(self):
        """Test batch config with append mode."""
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="customers",
            write_mode="append",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="silver"
            ),
        )
        assert config.delta_write_options.mode == "append"

    def test_explicit_batch_write_options_override_defaults(self):
        """Test explicit write options override defaults."""
        custom_opts = DeltaBatchWriteOptions(
            mode="append",
            merge_schema=True,
        )
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="customers",
            write_mode="overwrite",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="silver"
            ),
            delta_write_options=custom_opts,
        )
        assert config.delta_write_options.mode == "append"
        assert config.delta_write_options.merge_schema is True

    def test_batch_write_options_with_replace_where(self):
        """Test sink-level replace_where override for selective overwrites."""
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="facts",
            write_mode="overwrite",
            replace_where="year='2025' AND month='01'",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="warehouse"
            ),
        )
        assert config.replace_where == "year='2025' AND month='01'"

    def test_default_table_properties(self):
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="facts",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="warehouse"
            ),
        )
        assert config.table_properties["delta.columnMapping.mode"] == "name"


class TestDatabricksSinkRuntimeConfigBatchMode:
    """Test runtime config smart factory for batch mode."""

    def test_batch_runtime_config_creates_batch_write_options(self):
        """Test batch mode creates DeltaBatchWriteOptions."""
        runtime_config = DatabricksSinkRuntimeConfig(
            mode="batch",
            write_mode="overwrite",
        )
        assert isinstance(runtime_config.delta_write_options, DeltaBatchWriteOptions)
        assert runtime_config.delta_write_options.mode == "overwrite"

    def test_batch_runtime_config_append_mode(self):
        """Test batch mode with append."""
        runtime_config = DatabricksSinkRuntimeConfig(
            mode="batch",
            write_mode="append",
        )
        assert runtime_config.delta_write_options.mode == "append"

    def test_batch_runtime_config_preserves_explicit_options(self):
        """Test batch mode preserves explicitly set options."""
        opts = DeltaBatchWriteOptions(
            mode="append",
            merge_schema=True,
        )
        runtime_config = DatabricksSinkRuntimeConfig(
            mode="batch",
            write_mode="overwrite",
            delta_write_options=opts,
        )
        # Explicit options should be preserved, not overridden by write_mode
        assert runtime_config.delta_write_options.mode == "append"
        assert runtime_config.delta_write_options.merge_schema is True

    def test_batch_runtime_config_default_table_properties(self):
        runtime_config = DatabricksSinkRuntimeConfig(mode="batch")
        assert runtime_config.table_properties["delta.columnMapping.mode"] == "name"


class TestDatabricksSinkRuntimeConfigStreamingMode:
    """Test runtime config smart factory for streaming mode."""

    def test_streaming_runtime_config_creates_stream_write_options(self):
        """Test streaming mode creates DeltaStreamWriteOptions."""
        runtime_config = DatabricksSinkRuntimeConfig(
            mode="streaming",
            write_mode="append",
            checkpoint_path="/mnt/checkpoints/test",
        )
        assert isinstance(runtime_config.delta_write_options, DeltaStreamWriteOptions)
        assert runtime_config.delta_write_options.checkpoint_location == "/mnt/checkpoints/test"

    def test_streaming_runtime_config_default_trigger(self):
        """Test streaming config has correct default trigger."""
        runtime_config = DatabricksSinkRuntimeConfig(
            mode="streaming",
            write_mode="append",
            checkpoint_path="/mnt/checkpoints/test",
        )
        assert runtime_config.delta_write_options.trigger_mode == "availableNow"

    def test_streaming_runtime_config_preserves_explicit_stream_options(self):
        """Test streaming mode preserves explicitly set stream options."""
        opts = DeltaStreamWriteOptions(
            output_mode="complete",
            merge_schema=False,
            checkpoint_location="/custom/cp",
            trigger_mode="processingTime",
            trigger_interval="30 seconds",
        )
        runtime_config = DatabricksSinkRuntimeConfig(
            mode="streaming",
            write_mode="append",
            checkpoint_path="/mnt/checkpoints/test",
            delta_write_options=opts,
        )
        # Explicit stream options should be preserved
        assert runtime_config.delta_write_options.output_mode == "complete"
        assert runtime_config.delta_write_options.checkpoint_location == "/custom/cp"
        assert runtime_config.delta_write_options.trigger_interval == "30 seconds"


class TestWriteOptionsRouting:
    """Test write options are correctly routed to sinks."""

    def test_batch_write_options_format(self):
        """Test batch write options produce correct format."""
        opts = DeltaBatchWriteOptions(
            mode="append",
            merge_schema=True,
        )
        write_dict = opts.to_write_options()
        assert "mergeSchema" in write_dict
        assert write_dict["mergeSchema"] == "true"

    def test_stream_write_options_format(self):
        """Test stream write options produce correct format."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/mnt/cp",
        )
        write_dict = opts.to_write_options()
        assert "checkpointLocation" in write_dict
        assert write_dict["checkpointLocation"] == "/mnt/cp"


class TestWriteOptionsValidation:
    """Test validation of write options configurations."""

    def test_batch_mode_enum_values(self):
        """Test batch mode accepts valid values."""
        # Valid modes
        DeltaBatchWriteOptions(mode="overwrite")
        DeltaBatchWriteOptions(mode="append")

    def test_stream_output_mode_enum_values(self):
        """Test streaming output modes are valid."""
        # Valid output modes
        DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/tmp",
        )
        DeltaStreamWriteOptions(
            output_mode="complete",
            merge_schema=False,
            checkpoint_location="/tmp",
        )
        DeltaStreamWriteOptions(
            output_mode="update",
            merge_schema=False,
            checkpoint_location="/tmp",
        )

    def test_merge_schema_bool_validation(self):
        """Test merge_schema accepts boolean."""
        opts = DeltaBatchWriteOptions(mode="append", merge_schema=True)
        assert opts.merge_schema is True
        opts = DeltaBatchWriteOptions(mode="append", merge_schema=False)
        assert opts.merge_schema is False


class TestConfigurationScenarios:
    """Test real-world configuration scenarios."""

    def test_simple_batch_overwrite(self):
        """Test simple batch overwrite scenario."""
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="dim_customers",
            write_mode="overwrite",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="silver"
            ),
        )
        assert config.delta_write_options.mode == "overwrite"
        assert config.delta_write_options.merge_schema is False

    def test_cdc_append_with_merge_schema(self):
        """Test CDC append scenario with schema evolution."""
        opts = DeltaBatchWriteOptions(
            mode="append",
            merge_schema=True,
        )
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="fact_events",
            write_mode="append",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="silver"
            ),
            delta_write_options=opts,
        )
        assert config.delta_write_options.mode == "append"
        assert config.delta_write_options.merge_schema is True

    def test_incremental_partition_replace(self):
        """Test incremental partition replace scenario."""
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="facts",
            write_mode="overwrite",
            replace_where="partition_date='2025-01-15'",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="warehouse"
            ),
        )
        assert config.replace_where == "partition_date='2025-01-15'"

    def test_replace_where_requires_overwrite_mode(self):
        """replace_where is only valid when write_mode='overwrite'."""
        with pytest.raises(ValidationError, match="replace_where is only valid"):
            DatabricksSinkConfig(
                location_type="catalog_table",
                object_name="facts",
                write_mode="append",
                replace_where="partition_date='2025-01-15'",
                connection=DatabricksConnectionConfig(
                    catalog="main",
                    schema_name="warehouse",
                ),
            )

    def test_real_time_streaming(self):
        """Test real-time streaming scenario."""
        runtime_config = DatabricksSinkRuntimeConfig(
            mode="streaming",
            write_mode="append",
            checkpoint_path="/mnt/checkpoints/kafka_stream",
        )
        assert isinstance(runtime_config.delta_write_options, DeltaStreamWriteOptions)
        assert runtime_config.delta_write_options.output_mode == "append"

    def test_scheduled_micro_batch(self):
        """Test scheduled micro-batch processing."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/mnt/checkpoints/scheduled",
            trigger_mode="processingTime",
            trigger_interval="5 minutes",
        )
        runtime_config = DatabricksSinkRuntimeConfig(
            mode="streaming",
            write_mode="append",
            checkpoint_path="/mnt/checkpoints/scheduled",
            delta_write_options=opts,
        )
        assert runtime_config.delta_write_options.trigger_interval == "5 minutes"
