from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

from metabricks.systems.databricks.types import DatabricksConnection


@dataclass(frozen=True)
class DatabricksQuery:
    source_query: str
    query_args: Dict[str, Any]


@dataclass(frozen=True)
class DatabricksAutoloaderSource:
    path: str
    schema_location: str

    # Keep this narrow for now; expand as we add other formats.
    format: Literal["json"] = "json"
    autoloader_options: Dict[str, str] | None = None
