from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, BooleanType

from metabricks.core.base_sink import BaseSink
from metabricks.core.contracts import DataEnvelope, ExecutionContext
from metabricks.sinks.types import DatabricksSinkRuntimeConfig
from metabricks.systems.databricks.types import (
    DatabricksCatalogTableTarget,
    DatabricksExternalLocationTarget,
    DatabricksVolumeTarget,
)
from metabricks.sinks.registry import register_sink
from metabricks.sinks.strategies.delta_writer import DeltaWriterStrategy
from metabricks.sinks.strategies.spark_file_writer import SparkFileWriterStrategy



@register_sink(system_type="databricks", mode="batch")
class DatabricksSink(BaseSink):
    """Databricks batch sink that routes to format-specific writer strategies."""

    def __init__(self, config: DatabricksSinkRuntimeConfig, *, spark: Optional[object] = None):
        super().__init__(config)
        self.spark = spark
        self.log_info(f"Initializing DatabricksSink: mode={config.mode}, format={config.format}")

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

    def _table_name(self) -> str:
        cfg = self.config
        if not isinstance(cfg.target, DatabricksCatalogTableTarget):
            raise ValueError("catalog_table target required for table_name")
        return f"{cfg.connection.catalog}.{cfg.connection.schema_name}.{cfg.target.object_name}"

    def _volume_dir(self) -> Path:
        cfg = self.config
        if not isinstance(cfg.target, DatabricksVolumeTarget):
            raise ValueError("volume target required for volume_dir")
        base = Path(cfg.target.volume_path)
        folder = Path(cfg.target.folder_path or "")
        return base / folder

    def write(self, envelope: DataEnvelope) -> Dict[str, Any]:
        cfg = self.config
        self.log_info(f"Starting write operation: payload_type={envelope.payload_type}, format={cfg.format}")
        
        if cfg.mode != "batch":
            raise ValueError(f"DatabricksSink only supports mode='batch' (got {cfg.mode!r})")

        if envelope.payload_type != "dataframe":
            raise TypeError(
                "DatabricksSink requires a dataframe payload. "
                "For writing Python objects (json/text/bytes) to files, use system_type='filesystem'."
            )

        fmt = cfg.format.lower()

        # Optionally attach enterprise audit metadata struct at sink-time.
        # Guarded by context.attach_audit_meta so existing pipelines remain unchanged.
        if envelope.context and envelope.context.attach_audit_meta:
            df = envelope.data
            envelope.data = self._attach_meta_struct(df, envelope.context)

        if isinstance(cfg.target, DatabricksCatalogTableTarget):
            self.log_info(f"Target type: catalog_table, table={self._table_name()}")
            if fmt != "delta":
                raise ValueError("catalog_table currently supports only format='delta'")

            spark = self._ensure_spark()
            writer = self._writers["delta"]
            self.log_info(f"Using DeltaWriterStrategy for catalog_table write with mode={cfg.delta_write_options.mode}")
            
            return writer.write(
                envelope,
                {
                    "spark": spark,
                    "table_name": self._table_name(),
                    "overwrite_scope": cfg.overwrite_scope or None,
                    "replace_where": cfg.replace_where,
                    "partition_by": cfg.partition_by or [],
                    "write_options": cfg.delta_write_options,
                    "options": cfg.format_options,
                },
            )

        if isinstance(cfg.target, DatabricksVolumeTarget):
            volume_dir = self._volume_dir()
            self.log_info(f"Target type: volume, path={volume_dir}, object_name={cfg.target.object_name}")

            spark = self._ensure_spark()
            writer = self._writers.get(fmt)
            if writer is None:
                raise ValueError(f"Unsupported sink format: {fmt}")

            if cfg.target.object_name:
                target_path = str(volume_dir / cfg.target.object_name)
            else:
                target_path = str(volume_dir)
            
            self.log_info(f"Using {writer.__class__.__name__} for volume write: target_path={target_path}, mode={cfg.delta_write_options.mode}")
            
            cfg_dict: Dict[str, Any] = {
                "spark": spark,
                "path": target_path,
                "overwrite_scope": cfg.overwrite_scope or None,
                "replace_where": cfg.replace_where,
                "partition_by": cfg.partition_by or [],
                "write_options": cfg.delta_write_options,
                "options": cfg.format_options,
            }
            # SparkFileWriterStrategy supports physical partitioning via partition_by.
            return writer.write(envelope, cfg_dict)

        if isinstance(cfg.target, DatabricksExternalLocationTarget):
            self.log_info(f"Target type: external_location, path={cfg.target.path}")

            spark = self._ensure_spark()
            writer = self._writers.get(fmt)
            if writer is None:
                raise ValueError(f"Unsupported sink format: {fmt}")
            
            self.log_info(f"Using {writer.__class__.__name__} for external location write: path={cfg.target.path}, mode={cfg.delta_write_options.mode}")
            
            cfg_dict: Dict[str, Any] = {
                "spark": spark,
                "path": cfg.target.path,
                "overwrite_scope": cfg.overwrite_scope or None,
                "replace_where": cfg.replace_where,
                "partition_by": cfg.partition_by or [],
                "write_options": cfg.delta_write_options,
                "options": cfg.format_options,
            }
            return writer.write(envelope, cfg_dict)

        raise ValueError(f"Unknown databricks target: {type(cfg.target)!r}")

    def _attach_meta_struct(self, df, context: ExecutionContext):
        """Attach audit metadata columns for bronze layer tables.
        
        Top-level columns (for query performance):
        - _ingest_ts: Timestamp for time-based filtering, partitioning, watermarks
        - run_id: Run identifier for debugging/tracing
        
        Nested _meta struct (rarely queried audit fields):
        - batch_id, pipeline_name, pipeline_version, is_replay
        - write_principal, record_hash

        Execution context fields used:
        - run_id: Unique run identifier
        - batch_id: Optional batch sequence identifier
        - pipeline_name: Pipeline identifier
        - pipeline_version: Code version (git SHA or semver)
        - is_replay: Backfill/reprocess flag
        - record_hash_keys: Column names used to compute dedup hash
        """

        run_id = context.run_id
        batch_id = context.batch_id
        pipeline_name = context.pipeline_name
        pipeline_version = context.pipeline_version
        is_replay = context.is_replay or False
        hash_keys = context.record_hash_keys or []

        # Compute record_hash from configured keys if present; else NULL.
        if hash_keys:
            record_hash = F.sha2(F.concat_ws("||", *[F.col(k) for k in hash_keys]), 256)
        else:
            # Explicitly cast NULL to STRING type to avoid void type
            record_hash = F.coalesce(F.lit(None), F.lit(""))

        # Add top-level columns for performance (used in WHERE clauses, partitioning)
        df = df.withColumn("_ingest_ts", F.current_timestamp().cast(TimestampType()))
        df = df.withColumn("_run_id", F.lit(run_id).cast(StringType()))

        # Build nested _meta struct with remaining audit fields
        meta_struct = F.struct(
            F.lit(batch_id).cast(StringType()).alias("batch_id"),
            F.lit(pipeline_name).cast(StringType()).alias("pipeline_name"),
            F.lit(pipeline_version).cast(StringType()).alias("pipeline_version"),
            F.lit(is_replay).cast(BooleanType()).alias("is_replay"),
            F.expr("current_user()").cast(StringType()).alias("write_principal"),
            record_hash.cast(StringType()).alias("record_hash"),
        )

        return df.withColumn("_meta", meta_struct)
