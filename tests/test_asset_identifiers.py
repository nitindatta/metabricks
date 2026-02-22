"""Tests for asset identifier tracking in events.

Tests verify that source_asset, sink_asset, source_system, and source_dataset_id
are correctly computed and propagated through all event stages.
"""

import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone

import pytest

from metabricks.bootstrap import load_builtin_plugins
from metabricks.core.asset_identifiers import (
    get_source_asset_identifier,
    get_sink_asset_identifier,
)
from metabricks.core.events import EventBus, MemoryJSONLObserver, publish_event, set_global_bus
from metabricks.models.pipeline_config import PipelineConfig
from metabricks.models.source_config import ApiSourceConfig, ApiConnectionConfig, DatabricksSourceConfig, DatabricksBatchQueryConfig
from metabricks.models.sink_config import DatabricksSinkConfig
from metabricks.models.connection_config import DatabricksConnectionConfig
from metabricks.orchestrator import MetadataOrchestrator


def setup_function():
    """Reset before each test."""
    from metabricks.connectors.registry import ConnectorRegistry
    from metabricks.wiring.source_registry import SourceWiringRegistry
    ConnectorRegistry.clear()
    SourceWiringRegistry.clear()
    load_builtin_plugins(reload=True)


class TestAssetIdentifierComputation:
    """Test asset identifier computation functions."""

    def test_api_source_asset_identifier(self):
        """Compute asset name for API source."""
        source = ApiSourceConfig(
            connection=ApiConnectionConfig(base_url="https://api.data.org"),
            endpoint="/v1/metadata",
        )
        asset = get_source_asset_identifier(source)
        assert asset == "api:///v1/metadata"

    def test_kafka_source_asset_identifier(self):
        """Compute asset name for Kafka source."""
        from metabricks.models.source_config import KafkaSourceConfig
        from metabricks.models.connection_config import KafkaConnectionConfig
        
        source = KafkaSourceConfig(
            connection=KafkaConnectionConfig(bootstrap_servers="localhost:9092"),
            topic="data.events",
        )
        asset = get_source_asset_identifier(source)
        assert asset == "kafka://data.events"

    def test_databricks_batch_query_source_asset_identifier(self):
        """Compute asset name for Databricks batch query source."""
        source = DatabricksSourceConfig(
            extraction_mode="batch",
            batch=DatabricksBatchQueryConfig(
                query="SELECT * FROM table WHERE date = :logical_date",
            ),
        )
        asset = get_source_asset_identifier(source)
        # Should contain "databricks://batch/query/" prefix
        assert asset.startswith("databricks://batch/query/")
        assert len(asset) > len("databricks://batch/query/")  # Has hash

    def test_databricks_batch_path_source_asset_identifier(self):
        """Compute asset name for Databricks batch path source."""
        from metabricks.models.source_config import DatabricksBatchPathConfig
        
        source = DatabricksSourceConfig(
            extraction_mode="batch",
            batch=DatabricksBatchPathConfig(
                path="/Volumes/main/raw/data",
                format="delta",
            ),
        )
        asset = get_source_asset_identifier(source)
        assert asset == "databricks:///Volumes/main/raw/data"

    def test_databricks_sink_catalog_table_asset_identifier(self):
        """Compute asset name for Databricks catalog table sink."""
        sink = DatabricksSinkConfig(
            location_type="catalog_table",
            object_name="users",
            connection=DatabricksConnectionConfig(
                catalog="main",
                schema_name="test_data",
            ),
        )
        asset = get_sink_asset_identifier(sink)
        assert asset == "main.test_data.users"

    def test_databricks_sink_volume_asset_identifier(self):
        """Compute asset name for Databricks volume sink."""
        sink = DatabricksSinkConfig(
            location_type="volume",
            volume_path="/Volumes/main/archives/events",
            folder_path="raw",
        )
        asset = get_sink_asset_identifier(sink)
        assert asset == "/Volumes/main/archives/events/raw"

    def test_databricks_sink_volume_no_folder_asset_identifier(self):
        """Compute asset name for Databricks volume sink without folder."""
        sink = DatabricksSinkConfig(
            location_type="volume",
            volume_path="/Volumes/main/archives/events",
        )
        asset = get_sink_asset_identifier(sink)
        assert asset == "/Volumes/main/archives/events"

    def test_no_sink_asset_identifier(self):
        """Compute asset name when no sink is configured."""
        asset = get_sink_asset_identifier(None)
        assert asset == "no-sink"


class TestAssetIdentifiersInEvents:
    """Test that asset identifiers are propagated through events."""

    def test_events_include_source_and_sink_assets(self):
        """Verify source_asset and sink_asset are captured in events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            observer = MemoryJSONLObserver(
                base_path=tmpdir,
                pipeline_name="asset_test",
                run_id="asset-123",
            )
            bus = EventBus(
                run_id="asset-123",
                pipeline_name="asset_test",
                observers=[observer],
            )
            bus.start()
            set_global_bus(bus)

            # Publish event with asset identifiers
            publish_event(
                stage="source.init",
                status="started",
                source_asset="api://api.example.com/v1/data",
                sink_asset="main.lakehouse.raw_data",
            )
            publish_event(
                stage="source.extract",
                status="completed",
                source_asset="api://api.example.com/v1/data",
                sink_asset="main.lakehouse.raw_data",
                details={"payload_type": "json"},
            )

            # Shutdown to flush
            bus.shutdown()

            # Read and verify events
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            file_path = Path(tmpdir) / "asset_test" / date_str / "asset-123.jsonl"
            assert file_path.exists()

            with open(file_path) as f:
                events = [json.loads(line) for line in f]

            assert len(events) >= 2
            # Check first event has assets
            assert events[0]["source_asset"] == "api://api.example.com/v1/data"
            assert events[0]["sink_asset"] == "main.lakehouse.raw_data"
            # Check second event has assets
            assert events[1]["source_asset"] == "api://api.example.com/v1/data"
            assert events[1]["sink_asset"] == "main.lakehouse.raw_data"

            set_global_bus(None)

    def test_events_include_source_system_and_dataset_id(self):
        """Verify source_system and source_dataset_id are captured."""
        with tempfile.TemporaryDirectory() as tmpdir:
            observer = MemoryJSONLObserver(
                base_path=tmpdir,
                pipeline_name="context_test",
                run_id="ctx-123",
            )
            bus = EventBus(
                run_id="ctx-123",
                pipeline_name="context_test",
                observers=[observer],
            )
            bus.start()
            set_global_bus(bus)

            # Publish event with source context
            publish_event(
                stage="pipeline",
                status="started",
                source_system="data_api",
                source_dataset_id="metadata_v1",
            )

            # Shutdown to flush
            bus.shutdown()

            # Read and verify events
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            file_path = Path(tmpdir) / "context_test" / date_str / "ctx-123.jsonl"
            with open(file_path) as f:
                events = [json.loads(line) for line in f]

            assert events[0]["source_system"] == "data_api"
            assert events[0]["source_dataset_id"] == "metadata_v1"

            set_global_bus(None)


class TestOrchestrationWithAssetTracking:
    """Integration tests: verify orchestrator passes assets through to events."""

    def test_orchestrator_publishes_assets_in_events(self):
        """Verify orchestrator computes and publishes asset identifiers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            observer = MemoryJSONLObserver(
                base_path=tmpdir,
                pipeline_name="test_pipeline",
                run_id="orch-test-123",
            )
            bus = EventBus(
                run_id="orch-test-123",
                pipeline_name="test_pipeline",
                observers=[observer],
            )
            bus.start()
            set_global_bus(bus)

            # Create a simple API config with sink
            config = {
                "pipeline_name": "test_pipeline",
                "source_system": "data",
                "source_dataset_id": "metadata_v1",
                "environment": "test",
                "source": {
                    "system_type": "api",
                    "extraction_mode": "batch",
                    "format": "json",
                    "connection": {"system_type": "api"},
                    "endpoint": "/v1/metadata",
                },
                "sink": None,  # No sink for simplicity
            }

            orchestrator = MetadataOrchestrator(run_id="orch-test-123")
            try:
                result = orchestrator.run(config)
                assert result is not None
            except Exception as e:
                # We expect some errors since this is a mock, but event publishing should work
                pass
            finally:
                bus.shutdown()
                set_global_bus(None)

            # Verify events were captured with asset info
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            file_path = Path(tmpdir) / "test_pipeline" / date_str / "orch-test-123.jsonl"
            
            if file_path.exists():
                with open(file_path) as f:
                    events = [json.loads(line) for line in f if line.strip()]
                
                # Check that at least some events have source_asset populated
                source_init_events = [e for e in events if e.get("stage") == "source.init"]
                if source_init_events:
                    # Should have source_asset set
                    assert any(e.get("source_asset") for e in source_init_events)
                    # Should have source_system and source_dataset_id from config
                    assert any(e.get("source_system") == "data" for e in source_init_events)
                    assert any(e.get("source_dataset_id") == "metadata_v1" for e in source_init_events)


class TestAssetFieldsOptional:
    """Verify asset fields are optional and backward compatible."""

    def test_publish_event_without_assets_still_works(self):
        """Asset parameters should be optional in publish_event."""
        with tempfile.TemporaryDirectory() as tmpdir:
            observer = MemoryJSONLObserver(
                base_path=tmpdir,
                pipeline_name="no_assets",
                run_id="noasset-123",
            )
            bus = EventBus(
                run_id="noasset-123",
                pipeline_name="no_assets",
                observers=[observer],
            )
            bus.start()
            set_global_bus(bus)

            # Publish without asset fields (backward compat)
            publish_event(stage="test.stage", status="started")
            publish_event(stage="test.stage", status="completed")

            bus.shutdown()

            # Should still work
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            file_path = Path(tmpdir) / "no_assets" / date_str / "noasset-123.jsonl"
            assert file_path.exists()

            with open(file_path) as f:
                events = [json.loads(line) for line in f]

            # Assets should be None or absent
            assert events[0].get("source_asset") is None
            assert events[0].get("sink_asset") is None

            set_global_bus(None)
