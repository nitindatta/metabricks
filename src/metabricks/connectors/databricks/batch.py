from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from metabricks.connectors.databricks.types import (
    DatabricksBatchPathSource,
    DatabricksBatchQuerySource,
    DatabricksBatchSource,
    DatabricksConnection,
)
from metabricks.connectors.registry import register_connector
from metabricks.core.contracts import DataEnvelope
from metabricks.core.exceptions import NoSourceDataError


@register_connector(system_type="databricks", extraction_mode="batch")
class DatabricksBatchConnector:
    def __init__(
        self,
        connection: DatabricksConnection,
        source: DatabricksBatchSource,
        *,
        spark: Optional[object] = None,
    ):
        self.connection = connection
        self.source = source
        self.spark = spark or SparkSession.getActiveSession()

    def extract(self) -> DataEnvelope:
        if self.spark is None:
            raise RuntimeError("No active Spark session available")

        if isinstance(self.source, DatabricksBatchPathSource):
            fmt = self.source.format
            path = self.source.path
            options = dict(self.source.options or {})

            reader = self.spark.read.format(fmt)
            for k, v in options.items():
                reader = reader.option(k, v)
            df = reader.load(path)

            if df.limit(1).count() == 0:
                raise NoSourceDataError(
                    reason="No source data found",
                    details={"path": path, "format": fmt, "options": options},
                )

            return DataEnvelope(
                payload_type="dataframe",
                data=df,
                metadata={"source": "databricks", "path": path, "format": fmt},
            )

        if isinstance(self.source, DatabricksBatchQuerySource):
            df = self.spark.sql(self.source.query, args=self.source.args)

            if df.limit(1).count() == 0:
                raise NoSourceDataError(
                    reason="No source data found",
                    details={"query_args": self.source.args},
                )

            return DataEnvelope(
                payload_type="dataframe",
                data=df,
                metadata={"source": "databricks"},
            )

        raise TypeError(f"Unsupported Databricks batch source: {type(self.source)!r}")
