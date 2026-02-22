import pytest
from pydantic import TypeAdapter, ValidationError

from metabricks.models.source_config import SourceConfig


def test_source_config_discriminates_api():
    adapter = TypeAdapter(SourceConfig)
    cfg = adapter.validate_python(
        {
            "system_type": "api",
            "extraction_mode": "batch",
            "connection": {"system_type": "api", "base_url": "https://example.test"},
            "endpoint": "/v1/data",
        }
    )

    assert cfg.system_type == "api"
    assert cfg.extraction_mode == "batch"
    assert cfg.format == "json"


def test_api_streaming_requires_poll_interval_seconds():
    adapter = TypeAdapter(SourceConfig)

    with pytest.raises(ValidationError) as exc:
        adapter.validate_python(
            {
                "system_type": "api",
                "extraction_mode": "streaming",
                "connection": {"system_type": "api", "base_url": "https://example.test"},
                "endpoint": "/v1/events",
            }
        )

    assert "poll_interval_seconds" in str(exc.value)


def test_api_paginated_requires_pagination_block():
    adapter = TypeAdapter(SourceConfig)

    with pytest.raises(ValidationError) as exc:
        adapter.validate_python(
            {
                "system_type": "api",
                "extraction_mode": "paginated",
                "connection": {"system_type": "api", "base_url": "https://example.test"},
                "endpoint": "/v1/data",
            }
        )

    assert "pagination" in str(exc.value)


def test_api_paginated_accepts_offset_pagination():
    adapter = TypeAdapter(SourceConfig)

    cfg = adapter.validate_python(
        {
            "system_type": "api",
            "extraction_mode": "paginated",
            "connection": {"system_type": "api", "base_url": "https://example.test"},
            "endpoint": "/v1/data",
            "pagination": {"type": "offset", "page_size": 50},
        }
    )

    assert cfg.extraction_mode == "paginated"
    assert cfg.pagination is not None
    assert cfg.pagination.page_size == 50


def test_kafka_requires_streaming_mode():
    adapter = TypeAdapter(SourceConfig)

    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "system_type": "kafka",
                "extraction_mode": "batch",
                "connection": {"system_type": "kafka", "bootstrap_servers": "localhost:9092"},
                "topic": "t",
            }
        )


def test_source_config_discriminates_databricks():
    adapter = TypeAdapter(SourceConfig)
    cfg = adapter.validate_python(
        {
            "system_type": "databricks",
            "extraction_mode": "batch",
            "connection": {"system_type": "databricks"},
            "batch": {
                "kind": "query",
                "query": "SELECT 1 WHERE ds = :logical_date",
                "args": {"logical_date": "2025-01-01"},
            },
        }
    )

    assert cfg.system_type == "databricks"
    assert cfg.extraction_mode == "batch"
    assert cfg.batch is not None


def test_databricks_batch_accepts_logical_date_via_query_args():
    adapter = TypeAdapter(SourceConfig)
    cfg = adapter.validate_python(
        {
            "system_type": "databricks",
            "extraction_mode": "batch",
            "connection": {"system_type": "databricks"},
            "batch": {
                "kind": "query",
                "query": "SELECT 1 WHERE ds = :logical_date AND country = :country",
                "args": {"logical_date": "2025-01-01", "country": "FR"},
            },
        }
    )

    assert cfg.system_type == "databricks"
    assert cfg.extraction_mode == "batch"


def test_databricks_batch_rejects_unknown_format():
    adapter = TypeAdapter(SourceConfig)

    with pytest.raises(ValidationError) as exc:
        adapter.validate_python(
            {
                "system_type": "databricks",
                "extraction_mode": "batch",
                "connection": {"system_type": "databricks"},
                "batch": {
                    "kind": "path",
                    "path": "/Volumes/cat/schema/vol/folder",
                    "format": "txt",
                },
            }
        )

    assert "format" in str(exc.value)


def test_databricks_streaming_requires_path_and_schema_location():
    adapter = TypeAdapter(SourceConfig)

    with pytest.raises(ValidationError) as exc:
        adapter.validate_python(
            {
                "system_type": "databricks",
                "extraction_mode": "streaming",
                "connection": {"system_type": "databricks"},
                "streaming": {"kind": "autoloader"},
            }
        )

    s = str(exc.value)
    assert "path" in s or "schema_location" in s


def test_databricks_streaming_accepts_json_autoloader_shape():
    adapter = TypeAdapter(SourceConfig)

    cfg = adapter.validate_python(
        {
            "system_type": "databricks",
            "extraction_mode": "streaming",
            "connection": {"system_type": "databricks"},
            "streaming": {
                "kind": "autoloader",
                "path": "/Volumes/cat/schema/vol/folder",
                "schema_location": "/Volumes/cat/schema/vol/_schemas/foo",
                "format": "json",
                "options": {"cloudFiles.inferColumnTypes": "true"},
            },
        }
    )

    assert cfg.system_type == "databricks"
    assert cfg.extraction_mode == "streaming"
    assert cfg.streaming is not None
