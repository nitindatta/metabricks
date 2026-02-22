from metabricks.connectors.kafka_stream import KafkaStreamingConnector
from metabricks.connectors.types import KafkaConnection


class FakeReadStream:
    def __init__(self):
        self.fmt = None
        self.opts = {}

    def format(self, fmt: str):
        self.fmt = fmt
        return self

    def option(self, key: str, value: str):
        self.opts[key] = value
        return self

    def load(self):
        return "FAKE_KAFKA_STREAM_DF"


class FakeSpark:
    def __init__(self):
        self.readStream = FakeReadStream()


def test_kafka_streaming_connector_builds_reader():
    spark = FakeSpark()

    connector = KafkaStreamingConnector(
        KafkaConnection(
            bootstrap_servers="broker1:9092,broker2:9092",
            topic="events",
            starting_offsets="earliest",
            kafka_options={"kafka.security.protocol": "SASL_SSL"},
        ),
        spark=spark,
    )

    env = connector.extract()

    assert env.payload_type == "stream"
    assert env.data == "FAKE_KAFKA_STREAM_DF"

    rs = spark.readStream
    assert rs.fmt == "kafka"
    assert rs.opts["kafka.bootstrap.servers"] == "broker1:9092,broker2:9092"
    assert rs.opts["subscribe"] == "events"
    assert rs.opts["startingOffsets"] == "earliest"
    assert rs.opts["kafka.security.protocol"] == "SASL_SSL"
