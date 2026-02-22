from __future__ import annotations

from typing import Optional

from metabricks.core.secrets_provider import SecretsProvider
from metabricks.models.sink_config import FilesystemSinkConfig
from metabricks.sinks.types import FilesystemSinkRuntimeConfig
from metabricks.wiring.sink_registry import BuiltSinkArgs, register_sink_wiring


@register_sink_wiring(system_type="filesystem", mode="batch")
def build_filesystem_batch_sink_args(
    *,
    sink: FilesystemSinkConfig,
    secrets_provider: Optional[SecretsProvider] = None,
) -> BuiltSinkArgs:
    _ = secrets_provider
    runtime_cfg = FilesystemSinkRuntimeConfig(
        system_type=sink.system_type,
        mode=sink.mode,
        format=sink.format,
        path=sink.path,
        root=sink.root,
        folder_path=sink.folder_path,
        object_name=sink.object_name,
        compression=sink.compression,
        format_options=dict(sink.format_options or {}),
    )
    return BuiltSinkArgs(args=(runtime_cfg,), kwargs={})
