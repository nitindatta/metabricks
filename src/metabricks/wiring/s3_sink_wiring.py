from __future__ import annotations

from typing import Optional

from metabricks.core.secrets_provider import SecretsProvider
from metabricks.models.sink_config import S3SinkConfig
from metabricks.sinks.types import S3SinkRuntimeConfig
from metabricks.wiring.sink_registry import BuiltSinkArgs, register_sink_wiring


@register_sink_wiring(system_type="s3", mode="batch")
def build_s3_batch_sink_args(
    *,
    sink: S3SinkConfig,
    secrets_provider: Optional[SecretsProvider] = None,
) -> BuiltSinkArgs:
    _ = secrets_provider
    runtime_cfg = S3SinkRuntimeConfig(
        system_type=sink.system_type,
        mode=sink.mode,
        format=sink.format,
        path=sink.path,
        bucket=sink.bucket,
        prefix=sink.prefix,
        object_name=sink.object_name,
        write_mode=sink.write_mode,
        partition_by=sink.partition_by,
        format_options=dict(sink.format_options or {}),
    )
    return BuiltSinkArgs(args=(runtime_cfg,), kwargs={})
