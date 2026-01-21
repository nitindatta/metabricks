from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional

from metabricks.models.delta_write_options import DeltaBatchWriteOptions, DeltaStreamWriteOptions
from metabricks.systems.databricks.types import DatabricksConnection, DatabricksTarget


SinkMode = Literal["batch", "streaming"]
SinkFormat = Literal["delta", "parquet", "csv", "json"]


def _default_delta_write_options(instance):
    """Factory that creates appropriate write options based on sink mode."""
    if instance.mode == "streaming":
        return DeltaStreamWriteOptions(
            output_mode="append",
            merge_schema=True,
            checkpoint_location=instance.checkpoint_path or "/tmp/checkpoint",
        )
    else:
        return DeltaBatchWriteOptions(mode=instance.write_mode)


@dataclass
class DatabricksSinkRuntimeConfig:
    system_type: Literal["databricks"] = "databricks"
    mode: SinkMode = "batch"
    format: SinkFormat = "delta"

    connection: DatabricksConnection = field(default_factory=DatabricksConnection)

    target: DatabricksTarget | None = None

    write_mode: Literal["append", "overwrite"] = "overwrite"
    overwrite_scope: Optional[List[Dict[str, str]]] = None
    replace_where: Optional[str] = None
    partition_by: Optional[List[str]] = None
    checkpoint_path: Optional[str] = None

    # Delta write-time options (typed models)
    delta_write_options: Optional[DeltaBatchWriteOptions | DeltaStreamWriteOptions] = None

    format_options: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        """Populate delta_write_options if not provided."""
        if self.delta_write_options is None:
            self.delta_write_options = _default_delta_write_options(self)


@dataclass
class FilesystemSinkRuntimeConfig:
    """Configuration for filesystem sink (Python objects to files).
    
    No location_type needed - works with any filesystem path:
    - Local: /tmp/data
    - Unity Catalog Volume: /Volumes/catalog/schema/volume/data
    - DBFS: /dbfs/mnt/data
    - Cloud: abfss://container@account.dfs.core.windows.net/path
    """
    system_type: Literal["filesystem"] = "filesystem"
    mode: Literal["batch"] = "batch"
    format: Literal["json", "csv", "text", "bytes"] = "json"
    
    # Path can be either absolute path or built from root + folder + object_name
    path: Optional[str] = None
    root: Optional[str] = None
    folder_path: str = ""
    object_name: str = ""
    
    compression: Literal["none", "gzip", "gz"] = "none"
    format_options: Dict[str, str] = field(default_factory=dict)


@dataclass
class S3SinkRuntimeConfig:
    system_type: Literal["s3"] = "s3"
    mode: Literal["batch"] = "batch"
    format: SinkFormat = "delta"

    path: Optional[str] = None
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    object_name: Optional[str] = None

    write_mode: Literal["append", "overwrite"] = "overwrite"
    partition_by: Optional[List[str]] = None
    format_options: Dict[str, str] = field(default_factory=dict)
