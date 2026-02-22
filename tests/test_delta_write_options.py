"""Unit tests for Delta write options models.

Tests cover:
- DeltaBatchWriteOptions validation and behavior
- DeltaStreamWriteOptions validation and behavior
- Conversion to Spark write options
- Default values
"""

import pytest
from pydantic import ValidationError

from metabricks.models.delta_write_options import DeltaBatchWriteOptions, DeltaStreamWriteOptions


class TestDeltaBatchWriteOptions:
    """Tests for batch write options model."""

    def test_defaults(self):
        """Test default values."""
        opts = DeltaBatchWriteOptions(mode="overwrite")
        assert opts.mode == "overwrite"
        assert opts.merge_schema is False

    def test_merge_schema_enables_schema_evolution(self):
        """Test merge_schema for dynamic schema evolution."""
        opts = DeltaBatchWriteOptions(
            mode="append",
            merge_schema=True,
        )
        write_opts = opts.to_write_options()
        assert write_opts["mergeSchema"] == "true"

    def test_merge_schema_disabled_by_default(self):
        """Test merge_schema is disabled by default (not included in write options)."""
        opts = DeltaBatchWriteOptions(mode="overwrite")
        write_opts = opts.to_write_options()
        # mergeSchema is only included when True; False is the implicit default
        assert "mergeSchema" not in write_opts

    def test_replace_where_not_exposed(self):
        """replaceWhere is intentionally not exposed via DeltaBatchWriteOptions.

        Selective overwrite is configured at the sink level using:
        - overwrite_scope (preferred), or
        - replace_where (advanced override).
        """
        opts = DeltaBatchWriteOptions(mode="overwrite")
        write_opts = opts.to_write_options()
        assert "replaceWhere" not in write_opts

    def test_mode_validation_overwrite(self):
        """Test valid overwrite mode."""
        opts = DeltaBatchWriteOptions(mode="overwrite")
        assert opts.mode == "overwrite"

    def test_mode_validation_append(self):
        """Test valid append mode."""
        opts = DeltaBatchWriteOptions(mode="append")
        assert opts.mode == "append"

    def test_to_write_options_dict(self):
        """Test conversion to write options dict."""
        opts = DeltaBatchWriteOptions(
            mode="append",
            merge_schema=True,
        )
        write_opts = opts.to_write_options()
        assert isinstance(write_opts, dict)
        assert write_opts["mergeSchema"] == "true"


class TestDeltaStreamWriteOptions:
    """Tests for streaming write options model."""

    def test_defaults_append_mode(self):
        """Test default streaming config."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/tmp/checkpoint",
        )
        assert opts.output_mode == "append"
        assert opts.merge_schema is True
        assert opts.trigger_mode == "availableNow"
        assert opts.trigger_interval is None

    def test_complete_mode(self):
        """Test complete output mode."""
        opts = DeltaStreamWriteOptions(
            output_mode="complete",
            merge_schema=False,
            checkpoint_location="/tmp/cp",
        )
        assert opts.output_mode == "complete"

    def test_update_mode(self):
        """Test update output mode."""
        opts = DeltaStreamWriteOptions(
            output_mode="update",
            merge_schema=False,
            checkpoint_location="/tmp/cp",
        )
        assert opts.output_mode == "update"

    def test_continuous_trigger(self):
        """Test continuous trigger mode."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/tmp/cp",
            trigger_mode="continuous",
        )
        trigger = opts.to_trigger_dict()
        assert trigger == {"continuous": "0"}

    def test_processing_time_trigger_with_interval(self):
        """Test processingTime trigger with interval."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/tmp/cp",
            trigger_mode="processingTime",
            trigger_interval="10 seconds",
        )
        trigger = opts.to_trigger_dict()
        assert trigger == {"processingTime": "10 seconds"}

    def test_processing_time_trigger_default_interval(self):
        """Test processingTime trigger can be created without interval (defaults to None)."""
        # The model allows trigger_interval to be None, even for processingTime
        # This is a permissive design allowing flexible configurations
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/tmp/cp",
            trigger_mode="processingTime",
        )
        # When trigger_interval is None, it should still be queryable
        assert opts.trigger_interval is None

    def test_available_now_trigger(self):
        """Test availableNow trigger (default)."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/tmp/cp",
            trigger_mode="availableNow",
        )
        trigger = opts.to_trigger_dict()
        assert trigger == {"availableNow": True}

    def test_to_write_options_dict(self):
        """Test conversion to write options dict."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/mnt/checkpoints/my_stream",
        )
        write_opts = opts.to_write_options()
        assert isinstance(write_opts, dict)
        assert write_opts["checkpointLocation"] == "/mnt/checkpoints/my_stream"
        assert write_opts["mergeSchema"] == "true"

    def test_trigger_interval_only_valid_for_processing_time(self):
        """Test trigger_interval is only valid for processingTime."""
        with pytest.raises(ValidationError, match="trigger_interval only valid for processingTime"):
            DeltaStreamWriteOptions(
                output_mode="append",
                merge_schema=True,
                checkpoint_location="/tmp/cp",
                trigger_mode="availableNow",
                trigger_interval="10 seconds",
            )


class TestBatchOptionsIntegration:
    """Integration tests for batch write options."""

    def test_merge_schema_with_partitions(self):
        """Test merge_schema works with partitioned writes."""
        opts = DeltaBatchWriteOptions(
            mode="append",
            merge_schema=True,
        )
        write_opts = opts.to_write_options()
        assert write_opts["mergeSchema"] == "true"

    def test_replace_where_not_present_in_write_options(self):
        """Batch write options should never include replaceWhere; it's a sink-level concern."""
        opts = DeltaBatchWriteOptions(mode="overwrite", merge_schema=False)
        write_opts = opts.to_write_options()
        assert "replaceWhere" not in write_opts

    def test_append_mode_with_merge_schema(self):
        """Test typical append with schema evolution scenario."""
        opts = DeltaBatchWriteOptions(
            mode="append",
            merge_schema=True,
        )
        assert opts.mode == "append"
        assert opts.merge_schema is True


class TestStreamOptionsIntegration:
    """Integration tests for streaming write options."""

    def test_cdc_enabled_streaming_config(self):
        """Test typical CDC streaming setup."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/mnt/checkpoints/cdc_stream",
            trigger_mode="availableNow",
        )
        assert opts.output_mode == "append"
        assert opts.merge_schema is True
        write_opts = opts.to_write_options()
        assert write_opts["checkpointLocation"] == "/mnt/checkpoints/cdc_stream"

    def test_micro_batch_continuous_streaming(self):
        """Test micro-batch streaming with continuous trigger."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=False,
            checkpoint_location="/mnt/cp",
            trigger_mode="continuous",
        )
        trigger = opts.to_trigger_dict()
        assert trigger == {"continuous": "0"}

    def test_scheduled_processing_streaming(self):
        """Test scheduled processing time streaming."""
        opts = DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location="/mnt/cp",
            trigger_mode="processingTime",
            trigger_interval="5 minutes",
        )
        trigger = opts.to_trigger_dict()
        assert trigger == {"processingTime": "5 minutes"}
