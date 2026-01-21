from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from metabricks.core.base_sink import BaseSink
from metabricks.core.contracts import DataEnvelope
from metabricks.sinks.types import FilesystemSinkRuntimeConfig
from metabricks.sinks.registry import register_sink
from metabricks.sinks.strategies.file_writer import FileWriterStrategy


@register_sink(system_type="filesystem", mode="batch")
class FilesystemSink(BaseSink):
    """
    Generic filesystem sink for writing Python objects to files.
    
    Writes Python objects (JSON, CSV, text, bytes) to any filesystem path including:
    - Local filesystem
    - Unity Catalog volumes (/Volumes/catalog/schema/volume/...)
    - DBFS (/dbfs/...)
    - Mounted cloud storage (abfss://, s3://, etc.)
    
    This sink uses FileWriterStrategy which does not require Spark, making it ideal
    for writing API responses, configuration files, or other Python objects to storage.
    
    For Spark DataFrame writes with advanced features (partitioning, delta, etc.),
    use DatabricksSink instead.
    """

    def __init__(self, config: FilesystemSinkRuntimeConfig):
        super().__init__(config)
        self.log_info(f"Initializing FilesystemSink: format={config.format}")
        self._file_writer = FileWriterStrategy()

    def _target_path(self) -> str:
        """Build the full target path from configuration."""
        cfg = self.config
        if cfg.path:
            return cfg.path
        if not cfg.root:
            raise ValueError("Either 'path' or 'root' must be specified")
        base = Path(cfg.root)
        folder = Path(cfg.folder_path or "")
        return str(base / folder / cfg.object_name)

    def write(self, env: DataEnvelope) -> Dict[str, Any]:
        """Write Python object to filesystem."""
        cfg = self.config
        self.log_info(f"Starting filesystem write: payload_type={env.payload_type}, format={cfg.format}")
        
        if cfg.mode != "batch":
            raise ValueError("filesystem sink only supports mode='batch'")

        # Validate payload type
        if env.payload_type not in {"json", "text", "bytes"}:
            raise TypeError(
                f"FilesystemSink requires payload_type in {{json, text, bytes}}, "
                f"got '{env.payload_type}'. For DataFrames, use DatabricksSink."
            )

        fmt = cfg.format.lower()
        target_path = self._target_path()
        self.log_info(f"Target path: {target_path}")

        # Use FileWriterStrategy for all writes
        self.log_info(f"Writing {env.payload_type} payload to {fmt} format")
        
        # If path is specified directly, extract components for writer
        if cfg.path:
            path_obj = Path(cfg.path)
            root = str(path_obj.parent)
            object_name = path_obj.name
            folder_path = ""
        else:
            root = cfg.root
            object_name = cfg.object_name
            folder_path = cfg.folder_path
        
        audit = self._file_writer.write(
            env,
            {
                "root": root,
                "folder_path": folder_path,
                "object_name": object_name,
                "compression": cfg.compression,
                "format": fmt,
                "csv_delimiter": cfg.format_options.get("delimiter", ",") if cfg.format_options else ",",
                "csv_include_header": cfg.format_options.get("header", True) if cfg.format_options else True,
            },
        )
        
        self.log_info(f"Filesystem write completed: status={audit.get('status')}, record_count={audit.get('record_count')}")
        return self.post_write(audit)
