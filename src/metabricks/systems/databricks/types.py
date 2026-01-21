from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Union


DatabricksLocationType = Literal["catalog_table", "volume", "external_location"]


@dataclass(frozen=True)
class DatabricksCatalogTableTarget:
    object_name: str


@dataclass(frozen=True)
class DatabricksVolumeTarget:
    volume_path: str
    folder_path: Optional[str] = None
    object_name: Optional[str] = None


@dataclass(frozen=True)
class DatabricksExternalLocationTarget:
    path: str


DatabricksTarget = Union[
    DatabricksCatalogTableTarget,
    DatabricksVolumeTarget,
    DatabricksExternalLocationTarget,
]


@dataclass(frozen=True)
class DatabricksConnection:
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
