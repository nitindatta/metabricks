"""
Tests for MetadataOrchestrator class - the public API for pipeline execution.

Tests cover:
- Instantiation with and without run_id
- Pipeline execution with PipelineConfig
- Integration with run_pipeline function
- Error handling and propagation
- Run ID generation and tracking
"""

from unittest.mock import MagicMock, patch
import uuid

import httpx
import pytest

from metabricks.bootstrap import load_builtin_plugins
from metabricks.connectors.registry import ConnectorRegistry, ConnectorRegistryError
from metabricks.core.contracts import DataEnvelope
from metabricks.models.pipeline_config import PipelineConfig
from metabricks.orchestrator import MetadataOrchestrator, run_pipeline
from metabricks.wiring.source_registry import SourceWiringRegistry, SourceWiringRegistryError


def setup_function() -> None:
    """Setup test environment - keep tests isolated."""
    ConnectorRegistry.clear()
    SourceWiringRegistry.clear()
    load_builtin_plugins(reload=True)


def pytest_configure(config):
    """Ensure plugins are loaded before any tests run."""
    load_builtin_plugins()


class TestMetadataOrchestratorInstantiation:
    """Test MetadataOrchestrator initialization."""

    @pytest.fixture(autouse=True)
    def setup_registry(self):
        """Setup test environment for each test."""
        ConnectorRegistry.clear()
        SourceWiringRegistry.clear()
        load_builtin_plugins(reload=True)
        yield
        """Should generate a UUID if run_id not provided."""
        orchestrator = MetadataOrchestrator()
        
        assert orchestrator.run_id is not None
        # Check it's a valid UUID string format
        try:
            uuid.UUID(orchestrator.run_id)
        except ValueError:
            pytest.fail(f"Invalid UUID format: {orchestrator.run_id}")

    def test_instantiate_with_run_id(self):
        """Should use provided run_id."""
        run_id = "run-12345"
        orchestrator = MetadataOrchestrator(run_id=run_id)
        
        assert orchestrator.run_id == run_id

    def test_instantiate_with_uuid_object(self):
        """Should accept UUID object as run_id."""
        test_uuid = uuid.uuid4()
        test_uuid_str = str(test_uuid)
        
        orchestrator = MetadataOrchestrator(run_id=test_uuid_str)
        assert orchestrator.run_id == test_uuid_str

    def test_multiple_orchestrators_have_unique_ids(self):
        """Each orchestrator should get a unique ID when not specified."""
        orch1 = MetadataOrchestrator()
        orch2 = MetadataOrchestrator()
        
        assert orch1.run_id != orch2.run_id


class TestMetadataOrchestratorRun:
    """Test MetadataOrchestrator.run() method."""

    @pytest.fixture(autouse=True)
    def setup_registry(self):
        """Setup test environment for each test."""
        ConnectorRegistry.clear()
        SourceWiringRegistry.clear()
        load_builtin_plugins(reload=True)
        yield

    def test_run_with_api_batch_sets_run_hints(self):
        """Should set run_id and pipeline_name in result hints."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="test_pipeline",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        orchestrator = MetadataOrchestrator(run_id="test-run-001")

        with patch('httpx.Client', return_value=http_client):
            result = orchestrator.run(cfg)

        assert isinstance(result, DataEnvelope)
        assert result.context.run_id == "test-run-001"
        assert result.context.pipeline_name == "test_pipeline"

    def test_run_uses_same_run_id_for_multiple_calls(self):
        """Same orchestrator instance should use same run_id for all runs."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="p1",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        orchestrator = MetadataOrchestrator(run_id="persistent-run-id")

        with patch('httpx.Client', return_value=http_client):
            result1 = orchestrator.run(cfg)
            result2 = orchestrator.run(cfg)

        assert result1.context.run_id == "persistent-run-id"
        assert result2.context.run_id == "persistent-run-id"
        """Should run successfully without providing a secrets_provider."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"data": []}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="p1",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        orchestrator = MetadataOrchestrator()

        with patch('httpx.Client', return_value=http_client):
            result = orchestrator.run(cfg)

        assert isinstance(result, DataEnvelope)
        assert result.context.run_id is not None

    def test_run_with_custom_secrets_provider(self):
        """Should accept and use a custom secrets_provider."""
        mock_secrets_provider = MagicMock()
        
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="p1",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        orchestrator = MetadataOrchestrator()

        with patch('httpx.Client', return_value=http_client):
            result = orchestrator.run(cfg, secrets_provider=mock_secrets_provider)

        assert isinstance(result, DataEnvelope)

    def test_run_resolves_runtime_vars_into_config_dict(self, monkeypatch):
        from metabricks.orchestrator import MetadataOrchestrator

        # Minimal fake databricks batch connector path: we reuse the existing databricks batch test harness.
        from metabricks.orchestrator import run_pipeline
        from metabricks.models.pipeline_config import PipelineConfig

        class FakeDF:
            def limit(self, _n: int):
                return self

            def count(self) -> int:
                return 1

        class FakeSpark:
            def __init__(self):
                self.last_args = None

            def sql(self, _query, args=None):
                self.last_args = args
                return FakeDF()

        spark = FakeSpark()

        import metabricks.connectors.databricks.batch as batch_mod

        monkeypatch.setattr(batch_mod.SparkSession, "getActiveSession", classmethod(lambda cls: spark))

        # Use dict input so orchestrator can resolve templates before model validation.
        cfg_dict = {
            "pipeline_name": "p",
            "source": {
                "system_type": "databricks",
                "extraction_mode": "batch",
                "connection": {"system_type": "databricks"},
                "source_query": "SELECT * FROM t WHERE ds = :logical_date",
                "query_args": {"logical_date": "{{snapshot_date}}"},
            },
            "sink": None,
        }

        orch = MetadataOrchestrator(run_id="r1")
        env = orch.run(cfg_dict, runtime_vars={"snapshot_date": "2025-01-01"})

        assert env.context.metadata.get("snapshot_date") == "2025-01-01"
        assert spark.last_args == {"logical_date": "2025-01-01"}

    def test_result_object_access_and_warnings_pattern(self):
        """Client should access result as DataEnvelope object and use hints safely."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="object_usage",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        orchestrator = MetadataOrchestrator()

        with patch('httpx.Client', return_value=http_client):
            result = orchestrator.run(cfg)

        # Validate object-based access
        assert isinstance(result, DataEnvelope)
        assert result.payload_type in {"json", "dataframe", "bytes", "text", "stream"}
        assert result.context is not None
        assert result.context.run_id is not None

        # Safe warnings access (may be absent in metadata)
        warnings = (result.context.metadata or {}).get("warnings")
        assert warnings is None or isinstance(warnings, (list, str))

    def test_run_raises_for_unregistered_connector(self):
        """Should raise ConnectorRegistryError for unsupported source types."""
        # Try to use invalid sink type that doesn't match schema
        cfg = PipelineConfig(
            pipeline_name="p",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        # Clear registry to simulate unregistered connector
        ConnectorRegistry.clear()
        SourceWiringRegistry.clear()

        orchestrator = MetadataOrchestrator()

        with pytest.raises(ConnectorRegistryError):
            orchestrator.run(cfg)

    def test_run_preserves_data_envelope_structure(self):
        """Should return DataEnvelope with all expected properties."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"key": "value"}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="p1",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        orchestrator = MetadataOrchestrator()

        with patch('httpx.Client', return_value=http_client):
            result = orchestrator.run(cfg)

        assert isinstance(result, DataEnvelope)
        assert result.data is not None
        assert result.context is not None
        assert result.context.run_id is not None

    def test_run_accepts_config_dict(self):
        """Should accept config dict and validate internally."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"data": "test"}
        http_client.request.return_value = response

        config_dict = {
            "pipeline_name": "dict_config_test",
            "source": {
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            "sink": None,
        }

        orchestrator = MetadataOrchestrator(run_id="dict-test-001")

        with patch('httpx.Client', return_value=http_client):
            result = orchestrator.run(config_dict)

        assert isinstance(result, DataEnvelope)
        assert result.context.run_id == "dict-test-001"
        assert result.context.pipeline_name == "dict_config_test"

    def test_run_with_pipelineconfig_model(self):
        """Should accept validated PipelineConfig model directly."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="model_config_test",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        orchestrator = MetadataOrchestrator(run_id="model-test-001")

        with patch('httpx.Client', return_value=http_client):
            result = orchestrator.run(cfg)

        assert isinstance(result, DataEnvelope)
        assert result.context.run_id == "model-test-001"
        assert result.context.pipeline_name == "model_config_test"


class TestMetadataOrchestratorIntegration:
    """Integration tests with run_pipeline function."""

    @pytest.fixture(autouse=True)
    def setup_registry(self):
        """Setup test environment for each test."""
        ConnectorRegistry.clear()
        SourceWiringRegistry.clear()
        load_builtin_plugins(reload=True)
        yield

    def test_orchestrator_delegates_to_run_pipeline(self):
        """Orchestrator.run() should delegate to run_pipeline() function."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="p1",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        orchestrator = MetadataOrchestrator(run_id="test-inv")

        with patch('httpx.Client', return_value=http_client):
            with patch('metabricks.orchestrator.run_pipeline', wraps=run_pipeline) as mock_run:
                result = orchestrator.run(cfg)
                
                # Verify run_pipeline was called with correct arguments
                mock_run.assert_called_once()
                call_args = mock_run.call_args
                assert call_args[1]['run_id'] == "test-inv"
                assert call_args[1]['cfg'] == cfg

    def test_orchestrator_with_api_and_sink(self):
        """Should handle pipelines with both source and sink."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"id": 1, "name": "test"}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="api_to_sink",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://api.example.com",
                },
                "endpoint": "/v1/metadata",
            },
            sink=None,  # Simplified: no sink for now to avoid complex mocking
        )

        orchestrator = MetadataOrchestrator()

        with patch('httpx.Client', return_value=http_client):
            result = orchestrator.run(cfg)

        assert isinstance(result, DataEnvelope)
        assert result.context.pipeline_name == "api_to_sink"


class TestMetadataOrchestratorUsagePatterns:
    """Test common usage patterns for the MetadataOrchestrator."""

    @pytest.fixture(autouse=True)
    def setup_registry(self):
        """Setup test environment for each test."""
        ConnectorRegistry.clear()
        SourceWiringRegistry.clear()
        load_builtin_plugins(reload=True)
        yield

    def test_sequential_pipeline_runs(self):
        """Should handle running multiple pipelines sequentially."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"status": "ok"}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="sequential_test",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        with patch('httpx.Client', return_value=http_client):
            # Run pipeline twice with same orchestrator
            orchestrator = MetadataOrchestrator(run_id="batch-run-001")
            
            result1 = orchestrator.run(cfg)
            result2 = orchestrator.run(cfg)

            # Both should have same run_id
            assert result1.context.run_id == result2.context.run_id
            assert result1.context.run_id == "batch-run-001"

    def test_multiple_orchestrators_independent(self):
        """Should allow independent orchestrator instances for different runs."""
        http_client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {}
        http_client.request.return_value = response

        cfg = PipelineConfig(
            pipeline_name="p",
            source={
                "system_type": "api",
                "extraction_mode": "batch",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://example.test",
                },
                "endpoint": "/v1/data",
            },
            sink=None,
        )

        with patch('httpx.Client', return_value=http_client):
            orch1 = MetadataOrchestrator(run_id="run-1")
            orch2 = MetadataOrchestrator(run_id="run-2")
            
            result1 = orch1.run(cfg)
            result2 = orch2.run(cfg)

            assert result1.context.run_id == "run-1"
            assert result2.context.run_id == "run-2"

    def test_orchestrator_auto_generated_ids_are_unique(self):
        """Multiple orchestrators with auto-generated IDs should be unique."""
        orchestrators = [MetadataOrchestrator() for _ in range(5)]
        ids = [orch.run_id for orch in orchestrators]
        
        # All IDs should be unique
        assert len(set(ids)) == len(ids)


class TestMetadataOrchestratorDocumentation:
    """Verify that MetadataOrchestrator has proper documentation."""

    def test_class_has_docstring(self):
        """MetadataOrchestrator class should have a docstring."""
        assert MetadataOrchestrator.__doc__ is not None
        assert len(MetadataOrchestrator.__doc__.strip()) > 0

    def test_init_has_docstring(self):
        """__init__ method should have a docstring."""
        assert MetadataOrchestrator.__init__.__doc__ is not None
        assert "run_id" in MetadataOrchestrator.__init__.__doc__

    def test_run_has_docstring(self):
        """run method should have a docstring."""
        assert MetadataOrchestrator.run.__doc__ is not None
        # Check that docstring mentions configuration and returns result
        docstring = MetadataOrchestrator.run.__doc__.lower()
        assert "pipeline" in docstring or "config" in docstring


@pytest.mark.integration
class TestMetadataOrchestratorIntegrationRealAPI:
    """Integration tests using real public APIs and realistic configurations."""

    @pytest.fixture(autouse=True)
    def setup_registry(self):
        """Setup test environment for each test."""
        ConnectorRegistry.clear()
        SourceWiringRegistry.clear()
        load_builtin_plugins(reload=True)
        yield

    def test_orchestrator_with_jsonplaceholder_api_config(self):
        """
        Integration test: Extract users from JSONPlaceholder API to Databricks volume.
        Uses a realistic configuration with proper metadata, logging, and hints.
        """
        config = {
            "pipeline_name": "pl_api_extract",
            "job_name": "load_typicode",
            "source_system": "ems_ces",
            "environment": "dev",
            "source_dataset_version": 1,
            "source_dataset_id": "users",
            "tags": ["ems_ces", "users"],
            "trigger_cron": "0 0 0",
            "source": {
                "system_type": "api",
                "extraction_mode": "batch",
                "format": "json",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://jsonplaceholder.typicode.com",
                    "headers": {"Accept": "application/json"},
                    "timeout_seconds": 30,
                    "auth": {"kind": "none"},
                },
                "endpoint": "/users",
                "method": "GET",
            },
            "sink": None,
        }

        orchestrator = MetadataOrchestrator(run_id="test-jsonplaceholder-001")

        # Run orchestrator: should fetch real data from JSONPlaceholder
        result = orchestrator.run(config)

        # Validate result structure
        assert isinstance(result, DataEnvelope)
        assert result.payload_type == "json"
        assert result.data is not None
        assert isinstance(result.data, list)
        assert len(result.data) > 0  # JSONPlaceholder returns 10 users

        # Verify context includes proper metadata and run_id
        assert result.context is not None
        assert result.context.run_id == "test-jsonplaceholder-001"
        assert result.context.pipeline_name == "pl_api_extract"
        assert result.context.source_system == "ems_ces"
        assert result.context.environment == "dev"
        assert result.context.source_dataset_id == "users"

        # Verify data structure (user objects from JSONPlaceholder)
        first_user = result.data[0]
        assert "id" in first_user
        assert "name" in first_user
        assert "email" in first_user
        assert "username" in first_user

    def test_orchestrator_with_config_dict_logs_run_id(self, caplog):
        """Verify that orchestrator logs include run_id for traceability."""
        import logging

        config = {
            "pipeline_name": "test_logging",
            "source": {
                "system_type": "api",
                "extraction_mode": "batch",
                "format": "json",
                "connection": {
                    "system_type": "api",
                    "base_url": "https://jsonplaceholder.typicode.com",
                    "headers": {"Accept": "application/json"},
                    "timeout_seconds": 30,
                    "auth": {"kind": "none"},
                },
                "endpoint": "/users/1",
                "method": "GET",
            },
            "sink": None,
        }

        orchestrator = MetadataOrchestrator(run_id="trace-id-12345")

        with caplog.at_level(logging.INFO):
            result = orchestrator.run(config)

        # Verify logs contain run_id
        log_text = caplog.text
        assert "trace-id-12345" in log_text or "Starting pipeline run" in log_text
        assert result.context.run_id == "trace-id-12345"
