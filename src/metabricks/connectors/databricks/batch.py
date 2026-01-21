from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from metabricks.connectors.databricks.types import DatabricksConnection, DatabricksQuery
from metabricks.connectors.registry import register_connector
from metabricks.core.contracts import DataEnvelope
from metabricks.core.exceptions import NoSourceDataError


@register_connector(system_type="databricks", extraction_mode="batch")
class DatabricksBatchConnector:
    def __init__(
        self,
        connection: DatabricksConnection,
        query: DatabricksQuery,
        *,
        spark: Optional[object] = None,
    ):
        self.connection = connection
        self.query = query
        self.spark = spark or SparkSession.getActiveSession()

    def extract(self) -> DataEnvelope:
        df = self.spark.sql(self.query.source_query, args=self.query.query_args)

        if df.limit(1).count() == 0:
            raise NoSourceDataError(
                reason="No source data found",
                details={"query_args": self.query.query_args}
            )

        return DataEnvelope(
            payload_type="dataframe",
            data=df,
            metadata={"source": "databricks"},
        )
