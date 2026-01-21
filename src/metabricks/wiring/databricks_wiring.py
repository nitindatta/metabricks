from __future__ import annotations

from typing import Optional

from metabricks.connectors.databricks.types import DatabricksAutoloaderSource, DatabricksConnection, DatabricksQuery
from metabricks.connectors.types import KafkaConnection
from metabricks.core.secrets_provider import SecretsProvider
from metabricks.models.source_config import DatabricksSourceConfig
from metabricks.wiring.source_registry import BuiltConnectorArgs, register_source_wiring


def build_databricks_connection(cfg: DatabricksSourceConfig) -> DatabricksConnection:
    return DatabricksConnection(
        catalog=cfg.connection.catalog,
        schema_name=cfg.connection.schema_name,
    )


def build_databricks_query(cfg: DatabricksSourceConfig) -> DatabricksQuery:
    assert cfg.source_query is not None
    assert cfg.query_args is not None
    return DatabricksQuery(source_query=cfg.source_query, query_args=cfg.query_args)


def build_databricks_autoloader_source(cfg: DatabricksSourceConfig) -> DatabricksAutoloaderSource:
    assert cfg.source_path is not None
    assert cfg.schema_location is not None
    return DatabricksAutoloaderSource(
        path=cfg.source_path,
        schema_location=cfg.schema_location,
        format="json",
        autoloader_options=dict(cfg.autoloader_options or {}),
    )


@register_source_wiring(system_type="databricks", extraction_mode="batch")
def build_databricks_batch_connector_args(
    *,
    source: DatabricksSourceConfig,
    secrets_provider: Optional[SecretsProvider] = None,
) -> BuiltConnectorArgs:
    # secrets_provider unused for Spark-based connector.
    _ = secrets_provider

    connection = build_databricks_connection(source)
    query = build_databricks_query(source)
    return BuiltConnectorArgs(args=(connection, query), kwargs={})


@register_source_wiring(system_type="databricks", extraction_mode="streaming")
def build_databricks_streaming_connector_args(
    *,
    source: DatabricksSourceConfig,
    secrets_provider: Optional[SecretsProvider] = None,
) -> BuiltConnectorArgs:
    # secrets_provider unused for Spark-based connector.
    _ = secrets_provider

    connection = build_databricks_connection(source)
    autoloader_source = build_databricks_autoloader_source(source)
    return BuiltConnectorArgs(args=(connection, autoloader_source), kwargs={})
