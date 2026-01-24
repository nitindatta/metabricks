from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from metabricks.core.base_sink import BaseSink
from metabricks.core.contracts import DataEnvelope
from metabricks.sinks.types import DatabricksSinkRuntimeConfig
from metabricks.sinks.registry import register_sink
from metabricks.sinks.strategies.delta_stream_writer import DeltaStreamWriterStrategy
from metabricks.sinks.utils.column_normalizer import normalize_dataframe_columns, store_column_mapping
from metabricks.systems.databricks.types import DatabricksCatalogTableTarget


@register_sink(system_type="databricks", mode="streaming")
class DatabricksStreamingSink(BaseSink):
    """Databricks streaming sink writing to a Delta table."""

    def __init__(self, config: DatabricksSinkRuntimeConfig, *, spark: Optional[object] = None):
        super().__init__(config)
        self.spark = spark or SparkSession.getActiveSession()
        self._writer = DeltaStreamWriterStrategy()
        self.log_info(f"Initialized DatabricksStreamingSink: mode={config.mode}, format={config.format}")

    def write(self, env: DataEnvelope) -> Dict[str, Any]:
        cfg = self.config
        self.log_info(f"Starting streaming write: payload_type={env.payload_type}")
        
        if cfg.mode != "streaming":
            raise ValueError(
                f"DatabricksStreamingSink only supports mode='streaming' (got {cfg.mode!r})"
            )

        if env.payload_type != "stream":
            raise TypeError("DatabricksStreamingSink requires stream payload")

        if not isinstance(cfg.target, DatabricksCatalogTableTarget):
            raise ValueError("Streaming sink currently supports only catalog_table targets")

        if cfg.format != "delta":
            raise ValueError("Streaming sink currently supports only format='delta'")

        if cfg.normalize_columns:
            df, mapping, _changed = normalize_dataframe_columns(env.data, cfg.normalization_strategy)
            env.data = df
            store_column_mapping(env.context, mapping, cfg.normalization_mapping_key)
        else:
            if env.context:
                identity_mapping = {col: col for col in env.data.columns}
                store_column_mapping(env.context, identity_mapping, cfg.normalization_mapping_key)

        if env.context and env.context.attach_audit_meta:
            env.data = env.data.withColumn("_logical_date", F.lit(env.context.logical_date).cast(StringType()))

        table_name = f"{cfg.connection.catalog}.{cfg.connection.schema_name}.{cfg.target.object_name}"
        run_id = env.context.run_id if env.context else "run"
        
        self.log_info(f"Writing stream to table: {table_name}, checkpoint={cfg.delta_write_options.checkpoint_location}, run_id={run_id}")
        result = self._writer.write(
            env,
            {
                "spark": self.spark,
                "table_name": table_name,
                "run_id": run_id,
                "write_options": cfg.delta_write_options,
            },
        )
        self.log_info(f"Streaming write completed: record_count={result.get('record_count')}, status={result.get('status')}")
        return result
