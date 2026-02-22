from metabricks.connectors.databricks.batch import DatabricksBatchConnector
from metabricks.connectors.databricks.types import DatabricksBatchPathSource, DatabricksConnection


class FakeDF:
    def __init__(self, count_val: int):
        self._count_val = count_val

    def limit(self, _n: int):
        return self

    def count(self) -> int:
        return self._count_val


class FakeReader:
    def __init__(self, df: FakeDF):
        self._df = df
        self.format_value = None
        self.options = {}
        self.loaded_path = None

    def format(self, fmt: str):
        self.format_value = fmt
        return self

    def option(self, key: str, value):
        self.options[key] = value
        return self

    def load(self, path: str):
        self.loaded_path = path
        return self._df


class FakeSpark:
    def __init__(self, df: FakeDF):
        self._reader = FakeReader(df)

    @property
    def read(self):
        return self._reader


def test_databricks_batch_connector_reads_path_source():
    df = FakeDF(1)
    spark = FakeSpark(df)

    source = DatabricksBatchPathSource(
        kind="path",
        path="/Volumes/cat/schema/vol/folder",
        format="json",
        options={"header": True},
    )
    connector = DatabricksBatchConnector(DatabricksConnection(), source, spark=spark)

    env = connector.extract()

    assert env.payload_type == "dataframe"
    assert env.data is df
    assert env.metadata["path"] == "/Volumes/cat/schema/vol/folder"

    reader = spark.read
    assert reader.format_value == "json"
    assert reader.loaded_path == "/Volumes/cat/schema/vol/folder"
    assert reader.options["header"] is True
