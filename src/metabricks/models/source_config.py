from __future__ import annotations

from typing import Annotated, Dict, Literal, Optional, Union

from pydantic import BaseModel, Field, PositiveFloat, PositiveInt, model_validator

from metabricks.models.connection_config import (
    ApiConnectionConfig,
    DatabricksConnectionConfig,
    FileConnectionConfig,
    JdbcConnectionConfig,
    KafkaConnectionConfig,
)

ExtractionMode = Literal["batch", "streaming", "paginated", "incremental", "cdc"]
SourceFormat = Literal["json", "delta"]


class ApiPaginationConfig(BaseModel):
    type: Literal["offset", "cursor"] = "offset"
    page_size: PositiveInt = 100

    offset_param: str = "offset"
    limit_param: str = "limit"

    cursor_param: Optional[str] = None

    @model_validator(mode="after")
    def _validate_cursor(self) -> "ApiPaginationConfig":
        if self.type == "cursor" and not self.cursor_param:
            raise ValueError("cursor_param is required when pagination type is 'cursor'")
        return self


class ApiSourceConfig(BaseModel):
    system_type: Literal["api"] = "api"
    extraction_mode: ExtractionMode = "batch"

    # External representation. API connectors currently always emit JSON.
    format: Literal["json"] = "json"

    connection: ApiConnectionConfig
    endpoint: str = "/"
    method: Literal["GET", "POST"] = "GET"

    poll_interval_seconds: Optional[PositiveFloat] = None
    pagination: Optional[ApiPaginationConfig] = None

    checkpoint_key: Optional[str] = None

    @model_validator(mode="after")
    def _validate_mode_requirements(self) -> "ApiSourceConfig":
        if self.extraction_mode == "streaming" and self.poll_interval_seconds is None:
            raise ValueError("poll_interval_seconds is required when extraction_mode is 'streaming'")
        if self.extraction_mode == "paginated" and self.pagination is None:
            raise ValueError("pagination is required when extraction_mode is 'paginated'")
        if self.extraction_mode == "incremental" and not self.checkpoint_key:
            raise ValueError("checkpoint_key is required when extraction_mode is 'incremental'")
        return self


class JdbcSourceConfig(BaseModel):
    system_type: Literal["jdbc"] = "jdbc"
    extraction_mode: ExtractionMode = "batch"

    # Placeholder: JDBC extraction is not yet wired through the new registry.
    # Keep this default to avoid breaking config parsing.
    format: SourceFormat = "delta"

    connection: JdbcConnectionConfig
    query: str

    checkpoint_column: Optional[str] = None
    checkpoint_key: Optional[str] = None

    @model_validator(mode="after")
    def _validate_mode_requirements(self) -> "JdbcSourceConfig":
        if self.extraction_mode == "incremental":
            if not self.checkpoint_column:
                raise ValueError("checkpoint_column is required when extraction_mode is 'incremental'")
            if not self.checkpoint_key:
                raise ValueError("checkpoint_key is required when extraction_mode is 'incremental'")
        return self


class FileSourceConfig(BaseModel):
    system_type: Literal["file"] = "file"
    extraction_mode: ExtractionMode = "batch"

    # Placeholder: file connectors are not yet wired through the new registry.
    format: SourceFormat = "json"

    connection: FileConnectionConfig = Field(default_factory=FileConnectionConfig)
    path: str
    pattern: Optional[str] = None

    poll_interval_seconds: Optional[PositiveFloat] = None

    @model_validator(mode="after")
    def _validate_mode_requirements(self) -> "FileSourceConfig":
        if self.extraction_mode == "streaming" and self.poll_interval_seconds is None:
            raise ValueError("poll_interval_seconds is required when extraction_mode is 'streaming'")
        return self


class KafkaSourceConfig(BaseModel):
    system_type: Literal["kafka"] = "kafka"
    extraction_mode: ExtractionMode = "streaming"

    # Placeholder: kafka connectors are not yet wired through the new registry.
    format: Literal["json"] = "json"

    connection: KafkaConnectionConfig
    topic: str
    starting_offsets: Literal["earliest", "latest"] = "latest"
    kafka_options: Dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_mode_requirements(self) -> "KafkaSourceConfig":
        if self.extraction_mode != "streaming":
            raise ValueError("Kafka sources only support extraction_mode='streaming'")
        return self


class DatabricksSourceConfig(BaseModel):
    system_type: Literal["databricks"] = "databricks"

    # Align with sink UX: `extraction_mode` + `format` are explicit dimensions.
    extraction_mode: Literal["batch", "streaming"] = "batch"
    format: SourceFormat = "delta"

    connection: DatabricksConnectionConfig = Field(default_factory=DatabricksConnectionConfig)

    # --- batch delta targets ---
    source_query: Optional[str] = None
    query_args: Optional[Dict[str, str]] = None

    # --- streaming json targets (autoloader) ---
    source_path: Optional[str] = None
    schema_location: Optional[str] = None
    autoloader_options: Dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_mode_and_format(self) -> "DatabricksSourceConfig":
        if self.extraction_mode == "batch":
            if self.format != "delta":
                raise ValueError("Databricks batch currently supports only format='delta'")
            if not self.source_query:
                raise ValueError("source_query is required when extraction_mode is 'batch'")
            if not self.query_args:
                raise ValueError("query_args is required when extraction_mode is 'batch'")
            if self.source_path or self.schema_location or self.autoloader_options:
                raise ValueError(
                    "Do not set source_path/schema_location/autoloader_options when extraction_mode is 'batch'"
                )
            return self

        if self.extraction_mode == "streaming":
            if self.format != "json":
                raise ValueError("Databricks streaming currently supports only format='json'")
            if not self.source_path:
                raise ValueError("source_path is required when extraction_mode is 'streaming'")
            if not self.schema_location:
                raise ValueError("schema_location is required when extraction_mode is 'streaming'")
            if self.source_query or self.query_args:
                raise ValueError("Do not set source_query/query_args when extraction_mode is 'streaming'")
            return self

        raise ValueError(f"Unsupported extraction_mode for databricks: {self.extraction_mode!r}")


SourceConfig = Annotated[
    Union[ApiSourceConfig, JdbcSourceConfig, FileSourceConfig, KafkaSourceConfig, DatabricksSourceConfig],
    Field(discriminator="system_type"),
]
