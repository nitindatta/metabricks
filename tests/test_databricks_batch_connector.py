from unittest.mock import MagicMock

import pytest

from metabricks.connectors.databricks.batch import DatabricksBatchConnector
from metabricks.connectors.databricks.types import DatabricksBatchQuerySource, DatabricksConnection


class FakeDF:
    def __init__(self, count_val: int):
        self._count_val = count_val

    def limit(self, _n: int):
        return self

    def count(self) -> int:
        return self._count_val


class FakeSpark:
    def __init__(self, df: FakeDF):
        self._df = df
        self.last_query = None
        self.last_args = None

    def sql(self, query, args=None):
        self.last_query = query
        self.last_args = args
        return self._df


def test_databricks_batch_connector_extract_success():
    df = FakeDF(1)
    spark = FakeSpark(df)

    connector = DatabricksBatchConnector(
        DatabricksConnection(),
        DatabricksBatchQuerySource(
            kind="query",
            query="SELECT * WHERE ds = :logical_date",
            args={"logical_date": "2025-01-01"},
        ),
        spark=spark,
    )

    env = connector.extract()

    assert env.payload_type == "dataframe"
    assert env.data is df
    assert env.metadata == {"source": "databricks"}
    assert spark.last_args == {"logical_date": "2025-01-01"}


def test_databricks_batch_connector_raises_when_no_snapshot():
    df = FakeDF(0)
    spark = FakeSpark(df)

    connector = DatabricksBatchConnector(
        DatabricksConnection(),
        DatabricksBatchQuerySource(
            kind="query",
            query="SELECT 1",
            args={"logical_date": "2025-01-01"},
        ),
        spark=spark,
    )

    from metabricks.core.exceptions import NoSourceDataError
    
    with pytest.raises(NoSourceDataError, match="No source data found"):
        connector.extract()


def test_databricks_batch_connector_supports_multiple_named_parameters():
    df = FakeDF(1)
    spark = FakeSpark(df)

    connector = DatabricksBatchConnector(
        DatabricksConnection(),
        DatabricksBatchQuerySource(
            kind="query",
            query="SELECT * FROM t WHERE ds = :logical_date AND country = :country",
            args={"logical_date": "2025-01-01", "country": "FR"},
        ),
        spark=spark,
    )

    env = connector.extract()

    assert env.data is df
    assert spark.last_args == {"logical_date": "2025-01-01", "country": "FR"}
