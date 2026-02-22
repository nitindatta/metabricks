"""Utilities for computing asset identifiers for event tracking.

Asset identifiers provide a consistent way to identify source/sink assets
across all event stages, enabling lineage tracking and correlation.
"""

from typing import Optional, Any, Dict
from metabricks.models.source_config import SourceConfig
from metabricks.models.sink_config import DatabricksSinkConfig


def get_source_asset_identifier(source: SourceConfig) -> str:
    """Compute a canonical asset identifier for a source.
    
    Returns a string like:
    - "api://api.data.org/v1/metadata" for API sources
    - "kafka://data.metadata.events" for Kafka topics
    - "databricks://query" for Databricks queries
    - "databricks:///Volumes/main/raw/data" for Databricks paths
    - "jdbc://db.table" for JDBC sources
    
    Args:
        source: SourceConfig model instance
        
    Returns:
        Asset identifier string
    """
    system_type = source.system_type
    
    if system_type == "api":
        # API source: api://host/endpoint
        endpoint = getattr(source, "endpoint", "/")
        return f"api://{endpoint}"
    
    elif system_type == "kafka":
        # Kafka source: kafka://topic
        topic = getattr(source, "topic", "unknown")
        return f"kafka://{topic}"
    
    elif system_type == "databricks":
        # Databricks source: check batch vs streaming
        extraction_mode = getattr(source, "extraction_mode", "batch")
        
        if extraction_mode == "batch":
            batch_cfg = getattr(source, "batch", None)
            if batch_cfg:
                kind = getattr(batch_cfg, "kind", "unknown")
                if kind == "query":
                    # For queries, use a shortened hash of the query
                    query = getattr(batch_cfg, "query", "")
                    query_hash = hash(query) & 0x7FFFFFFF  # Ensure positive
                    return f"databricks://batch/query/{query_hash:x}"
                elif kind == "path":
                    path = getattr(batch_cfg, "path", "unknown")
                    return f"databricks://{path}"
        elif extraction_mode == "streaming":
            streaming_cfg = getattr(source, "streaming", None)
            if streaming_cfg:
                path = getattr(streaming_cfg, "path", "unknown")
                return f"databricks://{path}"
        
        return "databricks://unknown"
    
    elif system_type == "jdbc":
        # JDBC source: jdbc://db/query
        query = getattr(source, "query", "select")
        # Use a hash to keep it reasonable length
        query_hash = hash(query) & 0x7FFFFFFF
        return f"jdbc://query/{query_hash:x}"
    
    elif system_type == "file":
        # File source: file://path
        path = getattr(source, "path", "unknown")
        return f"file://{path}"
    
    else:
        # Generic fallback
        return f"{system_type}://unknown"


def get_sink_asset_identifier(sink: Any) -> str:
    """Compute a canonical asset identifier for a sink.
    
    Returns a string like:
    - "main.test_data.metadata_raw" for Databricks catalog tables
    - "/Volumes/main/archives/events" for Databricks volumes
    - "s3://bucket/path" for S3 sinks
    
    Args:
        sink: Sink configuration model instance (typically DatabricksSinkConfig)
        
    Returns:
        Asset identifier string
    """
    if not sink:
        return "no-sink"
    
    system_type = getattr(sink, "system_type", "unknown")
    
    if system_type == "databricks":
        location_type = getattr(sink, "location_type", "catalog_table")
        
        if location_type == "catalog_table":
            # catalog.schema.table
            connection = getattr(sink, "connection", None)
            object_name = getattr(sink, "object_name", "unknown")
            
            if connection:
                catalog = getattr(connection, "catalog", "main")
                schema_name = getattr(connection, "schema_name", "default")
                return f"{catalog}.{schema_name}.{object_name}"
            else:
                return f"main.default.{object_name}"
        
        elif location_type == "volume":
            # volume_path/folder_path
            volume_path = getattr(sink, "volume_path", "unknown")
            folder_path = getattr(sink, "folder_path", "")
            
            if folder_path:
                return f"{volume_path}/{folder_path}".rstrip("/")
            else:
                return volume_path
        
        elif location_type == "external_location":
            # path
            path = getattr(sink, "path", "unknown")
            return path
        
        else:
            return f"databricks://{location_type}/unknown"
    
    elif system_type == "s3":
        # S3 sink: s3://bucket/path
        path = getattr(sink, "path", "bucket/unknown")
        return f"s3://{path}"
    
    elif system_type == "filesystem":
        # Filesystem sink: file://path
        path = getattr(sink, "path", "unknown")
        return f"file://{path}"
    
    else:
        # Generic fallback
        return f"{system_type}://unknown"
