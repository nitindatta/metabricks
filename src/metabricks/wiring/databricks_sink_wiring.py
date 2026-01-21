from __future__ import annotations

from typing import Optional

from metabricks.core.secrets_provider import SecretsProvider
from metabricks.models.sink_config import DatabricksSinkConfig
from metabricks.sinks.types import DatabricksSinkRuntimeConfig
from metabricks.systems.databricks.types import (
    DatabricksCatalogTableTarget,
    DatabricksConnection,
    DatabricksExternalLocationTarget,
    DatabricksVolumeTarget,
)
from metabricks.wiring.sink_registry import BuiltSinkArgs, register_sink_wiring


def build_databricks_sink_runtime_config(cfg: DatabricksSinkConfig) -> DatabricksSinkRuntimeConfig:
    if cfg.location_type == "catalog_table":
        if not cfg.object_name:
            raise ValueError("object_name is required for databricks catalog_table sinks")
        target = DatabricksCatalogTableTarget(object_name=cfg.object_name)
    elif cfg.location_type == "volume":
        if not cfg.volume_path:
            raise ValueError("volume_path is required for databricks volume sinks")
        target = DatabricksVolumeTarget(
            volume_path=cfg.volume_path,
            folder_path=cfg.folder_path,
            object_name=cfg.object_name,
        )
    elif cfg.location_type == "external_location":
        if not cfg.path:
            raise ValueError("path is required for databricks external_location sinks")
        target = DatabricksExternalLocationTarget(path=cfg.path)
    else:
        raise ValueError(f"Unknown location_type: {cfg.location_type}")

    return DatabricksSinkRuntimeConfig(
        system_type=cfg.system_type,
        mode=cfg.mode,
        format=cfg.format,
        connection=DatabricksConnection(catalog=cfg.connection.catalog, schema_name=cfg.connection.schema_name),
        target=target,
        write_mode=cfg.write_mode,
        overwrite_scope=cfg.overwrite_scope,
        replace_where=cfg.replace_where,
        partition_by=cfg.partition_by,
        checkpoint_path=cfg.checkpoint_path,
        format_options=dict(cfg.format_options or {}),
    )


@register_sink_wiring(system_type="databricks", mode="batch")
def build_databricks_batch_sink_args(
    *,
    sink: DatabricksSinkConfig,
    secrets_provider: Optional[SecretsProvider] = None,
) -> BuiltSinkArgs:
    _ = secrets_provider
    runtime_cfg = build_databricks_sink_runtime_config(sink)
    return BuiltSinkArgs(args=(runtime_cfg,), kwargs={})


@register_sink_wiring(system_type="databricks", mode="streaming")
def build_databricks_streaming_sink_args(
    *,
    sink: DatabricksSinkConfig,
    secrets_provider: Optional[SecretsProvider] = None,
) -> BuiltSinkArgs:
    _ = secrets_provider
    runtime_cfg = build_databricks_sink_runtime_config(sink)
    return BuiltSinkArgs(args=(runtime_cfg,), kwargs={})
