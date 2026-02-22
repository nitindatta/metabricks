from metabricks.connectors.databricks.streaming import DatabricksFileStreamingConnector
from metabricks.connectors.databricks.types import DatabricksAutoloaderSource, DatabricksConnection


class FakeReadStream:
    def __init__(self):
        self.fmt = None
        self.opts = {}
        self.loaded_path = None

    def format(self, fmt: str):
        self.fmt = fmt
        return self

    def option(self, key: str, value: str):
        self.opts[key] = value
        return self

    def load(self, path: str):
        self.loaded_path = path
        return "FAKE_STREAM_DF"


class FakeSpark:
    def __init__(self):
        self.readStream = FakeReadStream()


def test_databricks_streaming_connector_builds_cloudfiles_reader():
    spark = FakeSpark()

    connector = DatabricksFileStreamingConnector(
        DatabricksConnection(),
        DatabricksAutoloaderSource(
            path="/Volumes/cat/schema/vol/folder",
            schema_location="/Volumes/cat/schema/vol/_schemas/foo",
            format="json",
            autoloader_options={"cloudFiles.inferColumnTypes": "true"},
        ),
        spark=spark,
    )

    env = connector.extract()

    assert env.payload_type == "stream"
    assert env.data == "FAKE_STREAM_DF"

    rs = spark.readStream
    assert rs.fmt == "cloudFiles"
    assert rs.loaded_path == "/Volumes/cat/schema/vol/folder"
    assert rs.opts["cloudFiles.format"] == "json"
    assert rs.opts["cloudFiles.schemaLocation"] == "/Volumes/cat/schema/vol/_schemas/foo"
    assert rs.opts["cloudFiles.inferColumnTypes"] == "true"
