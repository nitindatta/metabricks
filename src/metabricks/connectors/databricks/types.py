from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Union

from metabricks.systems.databricks.types import DatabricksConnection


@dataclass(frozen=True)
class DatabricksBatchQuerySource:
    kind: Literal["query"]
    query: str
    args: Dict[str, Any]


@dataclass(frozen=True)
class DatabricksBatchPathSource:
    kind: Literal["path"]
    path: str
    format: Literal["delta", "csv", "json", "parquet", "avro"] = "delta"
    options: Dict[str, Any] | None = None


DatabricksBatchSource = Union[DatabricksBatchQuerySource, DatabricksBatchPathSource]


@dataclass(frozen=True)
class DatabricksAutoloaderSource:
    path: str
    schema_location: str

    format: Literal["json", "csv", "parquet", "delta"] = "json"
    autoloader_options: Dict[str, str] | None = None
