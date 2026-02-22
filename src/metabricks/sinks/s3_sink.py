from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from metabricks.core.base_sink import BaseSink
from metabricks.core.contracts import DataEnvelope
from metabricks.models.delta_write_options import DeltaBatchWriteOptions
from metabricks.sinks.types import S3SinkRuntimeConfig
from metabricks.sinks.registry import register_sink
from metabricks.sinks.strategies.delta_writer import DeltaWriterStrategy
from metabricks.sinks.strategies.spark_file_writer import SparkFileWriterStrategy


def _join_s3_path(*parts: str) -> str:
    cleaned = [p.strip("/") for p in parts if p and p.strip("/")]
    return "/".join(cleaned)


@register_sink(system_type="s3", mode="batch")
class S3Sink(BaseSink):
    """S3 sink implemented as Spark writes to an s3:// path.

    This assumes the runtime environment has Hadoop/S3 credentials configured.
    """

    def __init__(self, config: S3SinkRuntimeConfig, *, spark: Optional[object] = None):
        super().__init__(config)
        self.spark = spark

        self._writers: Dict[str, Any] = {
            "delta": DeltaWriterStrategy(),
            "parquet": SparkFileWriterStrategy(format="parquet"),
            "csv": SparkFileWriterStrategy(format="csv"),
            "json": SparkFileWriterStrategy(format="json"),
        }

    def _ensure_spark(self) -> object:
        if self.spark is None:
            self.spark = SparkSession.getActiveSession()
        if self.spark is None:
            raise RuntimeError("No active Spark session available")
        return self.spark

    def _target_path(self) -> str:
        cfg = self.config
        if cfg.path:
            return cfg.path

        # bucket/prefix/object_name validated in model
        prefix = cfg.prefix or ""
        obj = cfg.object_name or ""
        key = _join_s3_path(prefix, obj)
        return f"s3://{cfg.bucket}/{key}" if key else f"s3://{cfg.bucket}"

    def write(self, envelope: DataEnvelope) -> Dict[str, Any]:
        cfg = self.config
        if cfg.mode != "batch":
            raise ValueError(f"S3Sink only supports mode='batch' (got {cfg.mode!r})")

        spark = self._ensure_spark()
        fmt = cfg.format.lower()
        writer = self._writers.get(fmt)
        if writer is None:
            raise ValueError(f"Unsupported sink format: {fmt}")

        write_options = DeltaBatchWriteOptions(mode=cfg.write_mode)

        return writer.write(
            envelope,
            {
                "spark": spark,
                "path": self._target_path(),
                "write_options": write_options,
                "partition_by": cfg.partition_by or [],
                "options": cfg.format_options,
            },
        )
