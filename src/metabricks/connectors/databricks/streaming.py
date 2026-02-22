from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from metabricks.connectors.databricks.types import DatabricksAutoloaderSource, DatabricksConnection
from metabricks.connectors.registry import register_connector
from metabricks.core.contracts import DataEnvelope


@register_connector(system_type="databricks", extraction_mode="streaming")
class DatabricksFileStreamingConnector:
    """Databricks streaming connector.

    Currently supports Databricks cloudFiles (Auto Loader) reading from a path; format is provided via the source config.

    This returns a streaming DataFrame envelope with payload_type='stream' so it
    can be written by streaming sinks.
    """

    def __init__(
        self,
        connection: DatabricksConnection,
        source: DatabricksAutoloaderSource,
        *,
        spark: Optional[object] = None,
    ):
        self.connection = connection
        self.source = source
        self.spark = spark or SparkSession.getActiveSession()

    def extract(self) -> DataEnvelope:
        if self.spark is None:
            raise RuntimeError("No active Spark session available")

        # cloudFiles.* options are passed via .option(); some callers may supply
        # keys without the prefix, so keep this permissive.
        reader = self.spark.readStream.format("cloudFiles").option("cloudFiles.format", self.source.format)

        reader = reader.option("cloudFiles.schemaLocation", self.source.schema_location)

        for k, v in (self.source.autoloader_options or {}).items():
            reader = reader.option(k, v)

        df = reader.load(self.source.path)

        return DataEnvelope(
            payload_type="stream",
            data=df,
            metadata={
                "source": "databricks",
                "extraction_mode": "streaming",
                "format": self.source.format,
                "path": self.source.path,
            },
        )
