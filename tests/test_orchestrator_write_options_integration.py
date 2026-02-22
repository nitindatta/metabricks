"""Integration tests showing end-to-end usage with JSON configurations.

These tests demonstrate how clients will use the metadata-driven orchestrator
by passing JSON pipeline configurations that include the new write options.
"""

import json
import pytest

from metabricks.models.sink_config import DatabricksSinkConfig
from metabricks.models.connection_config import DatabricksConnectionConfig
from metabricks.models.delta_write_options import DeltaBatchWriteOptions, DeltaStreamWriteOptions
from metabricks.sinks.types import DatabricksSinkRuntimeConfig
from metabricks.systems.databricks.types import DatabricksCatalogTableTarget


class TestJsonConfigParsing:
    """Test parsing JSON configurations to typed models."""

    def test_batch_catalog_table_with_overwrite_scope(self):
        """Test batch catalog table config with selective overwrite via overwrite_scope."""
        json_config = {
            "location_type": "catalog_table",
            "object_name": "facts",
            "connection": {
                "catalog": "main",
                "schema_name": "warehouse",
            },
            "overwrite_scope": [
                {"year": "2025", "month": "01"},
                {"year": "2025", "month": "02"},
            ],
            "delta_write_options": {
                "mode": "overwrite",
                "merge_schema": False,
            },
        }

        config = DatabricksSinkConfig(**json_config)
        assert config.delta_write_options.mode == "overwrite"
        assert config.overwrite_scope is not None
        assert len(config.overwrite_scope) == 2

    def test_batch_catalog_table_with_replace_where_override(self):
        """Test sink-level replace_where advanced override."""
        json_config = {
            "location_type": "catalog_table",
            "object_name": "facts",
            "connection": {
                "catalog": "main",
                "schema_name": "warehouse",
            },
            "write_mode": "overwrite",
            "replace_where": "partition_date = CAST(current_date() AS STRING)",
            "delta_write_options": {
                "mode": "overwrite",
                "merge_schema": False,
            },
        }

        config = DatabricksSinkConfig(**json_config)
        assert config.replace_where is not None
        assert "partition_date" in config.replace_where

    def test_cdc_append_with_merge_schema(self):
        """Test CDC append with schema evolution."""
        json_config = {
            "location_type": "catalog_table",
            "object_name": "fact_events",
            "connection": {
                "catalog": "main",
                "schema_name": "silver",
            },
            "delta_write_options": {
                "mode": "append",
                "merge_schema": True,
            },
        }

        config = DatabricksSinkConfig(**json_config)
        assert config.delta_write_options.mode == "append"
        assert config.delta_write_options.merge_schema is True

    def test_streaming_with_processing_time_trigger(self):
        """Test streaming config with processingTime trigger."""
        json_config = {
            "output_mode": "append",
            "merge_schema": True,
            "checkpoint_location": "/mnt/checkpoints/scheduled",
            "trigger_mode": "processingTime",
            "trigger_interval": "5 minutes",
        }

        write_opts = DeltaStreamWriteOptions(**json_config)
        assert write_opts.trigger_interval == "5 minutes"
        trigger = write_opts.to_trigger_dict()
        assert trigger == {"processingTime": "5 minutes"}

    def test_write_options_conversion_to_spark_format(self):
        """Test conversion of write options to Spark format."""
        json_config = {
            "mode": "append",
            "merge_schema": True,
        }

        batch_opts = DeltaBatchWriteOptions(**json_config)
        write_dict = batch_opts.to_write_options()
        
        # mergeSchema should be included when True
        assert write_dict.get("mergeSchema") == "true"
        assert "replaceWhere" not in write_dict


class TestRealWorldClientUsagePatterns:
    """Real-world usage patterns demonstrating client integration."""

    def test_batch_pipeline_full_config(self):
        """Test complete batch pipeline configuration."""
        # Client defines pipeline as JSON
        pipeline_json = """
        {
            "location_type": "catalog_table",
            "object_name": "customers",
            "write_mode": "overwrite",
            "connection": {
                "catalog": "main",
                "schema_name": "silver"
            },
            "delta_write_options": {
                "mode": "overwrite",
                "merge_schema": false
            }
        }
        """
        
        config_dict = json.loads(pipeline_json)
        sink_config = DatabricksSinkConfig(**config_dict)
        
        # Verify configuration
        assert sink_config.object_name == "customers"
        assert sink_config.delta_write_options.mode == "overwrite"
        assert sink_config.delta_write_options.merge_schema is False

    def test_cdc_pipeline_full_config(self):
        """Test complete CDC pipeline configuration."""
        pipeline_json = """
        {
            "location_type": "catalog_table",
            "object_name": "customer_changes",
            "connection": {
                "catalog": "main",
                "schema_name": "cdc"
            },
            "delta_write_options": {
                "mode": "append",
                "merge_schema": true
            }
        }
        """
        
        config_dict = json.loads(pipeline_json)
        sink_config = DatabricksSinkConfig(**config_dict)
        
        assert sink_config.delta_write_options.mode == "append"
        assert sink_config.delta_write_options.merge_schema is True

    def test_streaming_kafka_pipeline(self):
        """Test streaming pipeline configuration."""
        # Client creates streaming sink config
        sink_config = DatabricksSinkRuntimeConfig(
            mode="streaming",
            checkpoint_path="/mnt/checkpoints/kafka",
            delta_write_options=DeltaStreamWriteOptions(
                output_mode="append",
                merge_schema=True,
                checkpoint_location="/mnt/checkpoints/kafka",
                trigger_mode="availableNow",
            ),
        )
        
        assert isinstance(sink_config.delta_write_options, DeltaStreamWriteOptions)
        assert sink_config.delta_write_options.output_mode == "append"

    def test_incremental_daily_load_pipeline(self):
        """Test daily incremental load with partition replacement."""
        pipeline_json = """
        {
            "location_type": "catalog_table",
            "object_name": "facts",
            "write_mode": "overwrite",
            "connection": {
                "catalog": "main",
                "schema_name": "warehouse"
            },
            "overwrite_scope": [
                {"load_date": "2025-01-15"}
            ],
            "delta_write_options": {
                "mode": "overwrite",
                "merge_schema": false
            }
        }
        """
        
        config_dict = json.loads(pipeline_json)
        sink_config = DatabricksSinkConfig(**config_dict)
        
        # Verify partition replacement is configured
        assert sink_config.overwrite_scope is not None
        assert sink_config.overwrite_scope[0]["load_date"] == "2025-01-15"

    def test_volume_archive_pipeline(self):
        """Test volume-based archival pipeline."""
        pipeline_json = """
        {
            "location_type": "volume",
            "volume_path": "/Volumes/main/archives/events",
            "write_mode": "append",
            "delta_write_options": {
                "mode": "append",
                "merge_schema": true
            }
        }
        """
        
        config_dict = json.loads(pipeline_json)
        sink_config = DatabricksSinkConfig(**config_dict)
        
        assert sink_config.delta_write_options.mode == "append"
        assert sink_config.volume_path == "/Volumes/main/archives/events"


class TestConfigurationDefaults:
    """Test default configuration behavior."""

    def test_batch_config_defaults_to_overwrite(self):
        """Test batch config defaults write_mode to overwrite."""
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="test",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="test",
            ),
        )
        assert config.delta_write_options.mode == "overwrite"

    def test_streaming_config_defaults_to_append(self):
        """Test streaming config creates append mode by default."""
        runtime_config = DatabricksSinkRuntimeConfig(
            mode="streaming",
            checkpoint_path="/tmp/cp",
        )
        assert runtime_config.delta_write_options.output_mode == "append"

    def test_explicit_options_override_defaults(self):
        """Test explicit write options override mode-based defaults."""
        custom_opts = DeltaBatchWriteOptions(
            mode="append",
            merge_schema=True,
        )
        config = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="test",
            write_mode="overwrite",  # Default would be overwrite
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="test",
            ),
            delta_write_options=custom_opts,
        )
        # Explicit options should be preserved
        assert config.delta_write_options.mode == "append"
        assert config.delta_write_options.merge_schema is True
