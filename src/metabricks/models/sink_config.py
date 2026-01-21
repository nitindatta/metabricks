from __future__ import annotations

from typing import Annotated, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, model_validator

from metabricks.models.connection_config import DatabricksConnectionConfig, FileConnectionConfig
from metabricks.models.delta_write_options import DeltaBatchWriteOptions
from metabricks.systems.databricks.types import DatabricksLocationType


SinkMode = Literal["batch", "streaming"]
SinkFormat = Literal["delta", "parquet", "csv", "json"]


class DatabricksSinkConfig(BaseModel):
    """Forward-only sink config for Databricks destinations.

    This follows the proposal docs: destination (`system_type`) is separate from
    write mode and format.
    """

    system_type: Literal["databricks"] = "databricks"

    # Where specifically in Databricks the data lands.
    location_type: DatabricksLocationType = "catalog_table"

    mode: SinkMode = "batch"
    format: SinkFormat = "delta"

    connection: DatabricksConnectionConfig = Field(default_factory=DatabricksConnectionConfig)

    # --- catalog_table targets ---
    object_name: Optional[str] = None

    # --- volume targets ---
    # Volumes are mounted under /Volumes/... at runtime.
    volume_path: Optional[str] = None
    folder_path: Optional[str] = None

    # --- external_location targets ---
    # Full path (e.g. s3://..., abfss://..., /mnt/...)
    path: Optional[str] = None

    write_mode: Literal["append", "overwrite"] = "overwrite"

    # Overwrite scope (used to build Delta replaceWhere).
    # This is NOT physical partitioning.
    # Example:
    #   overwrite_scope=[{"snapshot_date": "2026-01-19", "country": "FR"}]
    overwrite_scope: Optional[List[Dict[str, str]]] = None

    # Advanced: explicit Delta replaceWhere predicate.
    # Prefer overwrite_scope for common partition-scoped overwrites.
    # If set, this takes precedence over overwrite_scope.
    replace_where: Optional[str] = None

    # Physical partitioning for writes (Spark partitionBy). This controls layout on disk.
    partition_by: Optional[List[str]] = None

    checkpoint_path: Optional[str] = None

    # Delta write-time options (typed model)
    delta_write_options: Optional[DeltaBatchWriteOptions] = None
    
    format_options: Dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_and_setup_write_options(self) -> "DatabricksSinkConfig":
        # Set default write options if not provided
        if self.delta_write_options is None:
            self.delta_write_options = DeltaBatchWriteOptions(mode=self.write_mode)
        return self

    @model_validator(mode="after")
    def _validate_overwrite_scope(self) -> "DatabricksSinkConfig":
        if self.overwrite_scope:
            keys = list(self.overwrite_scope[0].keys())
            if not keys:
                raise ValueError("overwrite_scope entries must have at least one key")

            for entry in self.overwrite_scope:
                if set(entry.keys()) != set(keys):
                    raise ValueError(
                        "All overwrite_scope entries must have the same keys"
                    )

        return self

    @model_validator(mode="after")
    def _validate_replace_where(self) -> "DatabricksSinkConfig":
        if self.replace_where:
            if self.write_mode != "overwrite":
                raise ValueError("replace_where is only valid when write_mode='overwrite'")

        return self
    
    @model_validator(mode="after")
    def _validate_targets(self) -> "DatabricksSinkConfig":
        if self.location_type == "catalog_table":
            if not self.object_name:
                raise ValueError("object_name is required when location_type is 'catalog_table'")
            if not self.connection.catalog or not self.connection.schema_name:
                raise ValueError(
                    "connection.catalog and connection.schema_name are required when location_type is 'catalog_table'"
                )
            if self.path or self.volume_path or self.folder_path:
                raise ValueError("Do not set path/volume_path/folder_path for catalog_table sinks")

            # Today we only support table writes in Delta.
            if self.format != "delta":
                raise ValueError("catalog_table currently supports only format='delta'")

            if self.mode == "streaming" and not self.checkpoint_path:
                raise ValueError("checkpoint_path is required when mode is 'streaming'")

        elif self.location_type == "volume":
            if not self.volume_path:
                raise ValueError("volume_path is required when location_type is 'volume'")
            if self.path:
                raise ValueError("Do not set path when location_type is 'volume' (use volume_path + folder_path)")

            # For file-based sinks, the object_name is strongly recommended; for
            # directory-based formats (delta/parquet), object_name can be treated
            # as the folder name.
            if self.mode == "streaming" and not self.checkpoint_path:
                raise ValueError("checkpoint_path is required when mode is 'streaming'")

        elif self.location_type == "external_location":
            if not self.path:
                raise ValueError("path is required when location_type is 'external_location'")
            if self.volume_path:
                raise ValueError("Do not set volume_path when location_type is 'external_location'")
            if self.mode == "streaming" and not self.checkpoint_path:
                raise ValueError("checkpoint_path is required when mode is 'streaming'")

        else:
            raise ValueError(f"Unknown location_type: {self.location_type}")

        return self


class FilesystemSinkConfig(BaseModel):
    """Config for filesystem sink - write Python objects to files.
    
    No location_type needed. Works with any filesystem path:
    - Local: /tmp/data/file.json
    - Unity Catalog Volume: /Volumes/catalog/schema/volume/file.json
    - DBFS: /dbfs/mnt/data/file.json
    - Cloud: abfss://container@account/path/file.json
    """
    
    system_type: Literal["filesystem"] = "filesystem"
    mode: Literal["batch"] = "batch"
    format: Literal["json", "csv", "text", "bytes"] = "json"
    
    # Either provide full path or build from root + folder_path + object_name
    path: Optional[str] = None
    root: Optional[str] = None
    folder_path: str = ""
    object_name: str = ""
    
    compression: Literal["none", "gzip", "gz"] = "none"
    format_options: Dict[str, str] = Field(default_factory=dict)
    
    @model_validator(mode="after")
    def _validate_path_config(self) -> "FilesystemSinkConfig":
        if not self.path and not self.root:
            raise ValueError("Either 'path' or 'root' must be provided")
        if self.path and (self.root or self.folder_path or self.object_name):
            raise ValueError("When 'path' is provided, do not set 'root', 'folder_path', or 'object_name'")
        if not self.path and not self.object_name:
            raise ValueError("'object_name' is required when using 'root' instead of 'path'")
        return self


class S3SinkConfig(BaseModel):
    system_type: Literal["s3"] = "s3"
    mode: Literal["batch"] = "batch"

    format: SinkFormat = "delta"

    # Either set `path` directly (s3://bucket/prefix/...) or provide bucket+prefix.
    path: Optional[str] = None
    bucket: Optional[str] = None
    prefix: Optional[str] = None

    # Optional explicit final component. For delta/parquet this is typically a folder.
    object_name: Optional[str] = None

    write_mode: Literal["append", "overwrite"] = "overwrite"
    partition_by: Optional[List[str]] = None
    format_options: Dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_path(self) -> "S3SinkConfig":
        if self.path:
            if not self.path.startswith("s3://"):
                raise ValueError("s3 sink path must start with 's3://'")
            if self.bucket or self.prefix or self.object_name:
                raise ValueError("Do not set bucket/prefix/object_name when path is provided")
            return self

        if not self.bucket:
            raise ValueError("bucket is required when path is not provided")

        # prefix can be empty; object_name is recommended to avoid writing to bucket root
        if (self.prefix is None or self.prefix == "") and not self.object_name:
            raise ValueError("Provide prefix and/or object_name when path is not provided")

        return self


SinkConfig = Annotated[
    Union[DatabricksSinkConfig, FilesystemSinkConfig, S3SinkConfig],
    Field(discriminator="system_type"),
]
