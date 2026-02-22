from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from metabricks.connectors.registry import register_connector
from metabricks.core.contracts import DataEnvelope
from metabricks.connectors.types import KafkaConnection


@register_connector(system_type="kafka", extraction_mode="streaming")
class KafkaStreamingConnector:
    """Kafka streaming connector using Spark Structured Streaming."""

    def __init__(
        self,
        connection: KafkaConnection,
        *,
        spark: Optional[object] = None,
    ):
        self.connection = connection
        self.spark = spark or SparkSession.getActiveSession()

    def extract(self) -> DataEnvelope:
        if self.spark is None:
            raise RuntimeError("No active Spark session available")

        reader = self.spark.readStream.format("kafka")

        # Required options
        reader = reader.option("kafka.bootstrap.servers", self.connection.bootstrap_servers)
        reader = reader.option("subscribe", self.connection.topic)
        reader = reader.option("startingOffsets", self.connection.starting_offsets)

        for k, v in (self.connection.kafka_options or {}).items():
            reader = reader.option(k, v)

        df = reader.load()

        return DataEnvelope(
            payload_type="stream",
            data=df,
            metadata={
                "source": "kafka",
                "extraction_mode": "streaming",
                "topic": self.connection.topic,
                "starting_offsets": self.connection.starting_offsets,
            },
        )
