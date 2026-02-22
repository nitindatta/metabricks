from __future__ import annotations

from typing import Optional

from metabricks.connectors.databricks.types import (
    DatabricksAutoloaderSource,
    DatabricksBatchPathSource,
    DatabricksBatchQuerySource,
    DatabricksBatchSource,
    DatabricksConnection,
)
from metabricks.core.secrets_provider import SecretsProvider
from metabricks.models.source_config import (
    DatabricksBatchPathConfig,
    DatabricksBatchQueryConfig,
    DatabricksSourceConfig,
)
from metabricks.wiring.source_registry import BuiltConnectorArgs, register_source_wiring


def build_databricks_connection(cfg: DatabricksSourceConfig) -> DatabricksConnection:
    return DatabricksConnection(
        catalog=cfg.connection.catalog,
        schema_name=cfg.connection.schema_name,
    )


def build_databricks_batch_source(cfg: DatabricksSourceConfig) -> DatabricksBatchSource:
    if cfg.batch is None:
        raise ValueError("batch configuration is required for Databricks batch sources")
    if isinstance(cfg.batch, DatabricksBatchQueryConfig):
        return DatabricksBatchQuerySource(kind="query", query=cfg.batch.query, args=dict(cfg.batch.args))
    if isinstance(cfg.batch, DatabricksBatchPathConfig):
        return DatabricksBatchPathSource(
            kind="path",
            path=cfg.batch.path,
            format=cfg.batch.format,
            options=dict(cfg.batch.options or {}),
        )
    raise TypeError(f"Unsupported Databricks batch config: {type(cfg.batch)!r}")


def build_databricks_autoloader_source(cfg: DatabricksSourceConfig) -> DatabricksAutoloaderSource:
    if cfg.streaming is None:
        raise ValueError("streaming configuration is required for Databricks streaming sources")
    return DatabricksAutoloaderSource(
        path=cfg.streaming.path,
        schema_location=cfg.streaming.schema_location,
        format=cfg.streaming.format,
        autoloader_options=dict(cfg.streaming.options or {}),
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
    batch_source = build_databricks_batch_source(source)
    return BuiltConnectorArgs(args=(connection, batch_source), kwargs={})


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
