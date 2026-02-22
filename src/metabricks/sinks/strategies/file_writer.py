from __future__ import annotations

import csv
import gzip
import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict

from metabricks.core.contracts import DataEnvelope

from metabricks.sinks.strategies.base import WriterStrategy


class FileWriterStrategy(WriterStrategy):
    """
    Generic file writer for Python objects to filesystem paths.
    
    Supports writing various payload types (json, text, bytes, csv) to any filesystem
    location including local paths, Unity Catalog volumes, DBFS, or cloud storage.
    
    This is a sink for Python objects (API responses, dicts, lists, etc.) that need
    to be persisted to files, regardless of the source system.
    
    Supported payload types:
    - json: Python dicts/lists serialized to JSON
    - text: String data written as-is
    - bytes: Binary data written directly
    - dict/list with format="csv": Converts to CSV format
    
    Supports optional gzip compression for all formats.
    """

    def supports_payload_type(self, payload_type: str) -> bool:
        """Supports json, text, bytes payload types."""
        return payload_type in {"json", "text", "bytes"}

    def write(self, envelope: DataEnvelope, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Write envelope data to a file.
        
        Config parameters:
        - root: Base directory path
        - folder_path: Optional subdirectory
        - object_name: Target filename
        - compression: 'gzip', 'gz', or 'none' (default: 'none')
        - format: Override format - 'json', 'csv', 'text' (default: inferred from payload_type)
        - csv_delimiter: Delimiter for CSV format (default: ',')
        - csv_include_header: Include header row for CSV (default: True)
        """
        root = Path(config["root"])
        folder_path = Path(config.get("folder_path") or "")
        object_name = config["object_name"]
        compression = (config.get("compression") or "none").lower()
        output_format = config.get("format", envelope.payload_type).lower()

        full_dir = root / folder_path
        full_dir.mkdir(parents=True, exist_ok=True)
        output_file = full_dir / object_name

        # Write based on format
        if output_format == "json":
            self._write_json(envelope, output_file, compression)
            record_count = self._count_records_json(envelope.data)
        elif output_format == "csv":
            self._write_csv(envelope, output_file, compression, config)
            record_count = self._count_records_csv(envelope.data)
        elif output_format == "text":
            self._write_text(envelope, output_file, compression)
            record_count = 1
        elif output_format == "bytes" or envelope.payload_type == "bytes":
            self._write_bytes(envelope, output_file, compression)
            record_count = 1
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        return {
            "extract_time_utc": datetime.now(timezone.utc).isoformat(),
            "target_location": str(output_file),
            "status": "success",
            "record_count": record_count,
            "format": output_format,
            "compression": compression,
        }

    def _write_json(self, envelope: DataEnvelope, path: Path, compression: str) -> None:
        """Write JSON payload to file."""
        if envelope.payload_type not in {"json", "text"}:
            raise TypeError(f"Cannot write payload_type='{envelope.payload_type}' as JSON")
        
        data = envelope.data
        # If it's a string, try to parse it as JSON first
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                raise TypeError("Text payload is not valid JSON")

        if compression in {"gzip", "gz"}:
            with gzip.open(path, mode="wt", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        else:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

    def _write_csv(self, envelope: DataEnvelope, path: Path, compression: str, config: Dict[str, Any]) -> None:
        """Write data as CSV."""
        data = envelope.data
        
        # Data must be list of dicts or list of lists
        if not isinstance(data, list) or len(data) == 0:
            raise TypeError("CSV format requires non-empty list of dicts or lists")
        
        delimiter = config.get("csv_delimiter", ",")
        include_header = config.get("csv_include_header", True)
        
        # Build CSV in memory first
        output = StringIO()
        
        if isinstance(data[0], dict):
            # List of dicts - use DictWriter
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=delimiter)
            if include_header:
                writer.writeheader()
            writer.writerows(data)
        elif isinstance(data[0], (list, tuple)):
            # List of lists - use regular writer
            writer = csv.writer(output, delimiter=delimiter)
            writer.writerows(data)
        else:
            raise TypeError("CSV format requires list of dicts or list of lists/tuples")
        
        csv_content = output.getvalue()
        
        # Write to file with optional compression
        if compression in {"gzip", "gz"}:
            with gzip.open(path, mode="wt", encoding="utf-8") as f:
                f.write(csv_content)
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(csv_content)

    def _write_text(self, envelope: DataEnvelope, path: Path, compression: str) -> None:
        """Write text payload to file."""
        if envelope.payload_type != "text":
            # Try to convert to text
            if isinstance(envelope.data, str):
                text = envelope.data
            else:
                text = str(envelope.data)
        else:
            text = envelope.data
            
        if not isinstance(text, str):
            raise TypeError(f"Text format requires string data, got {type(text)}")

        if compression in {"gzip", "gz"}:
            with gzip.open(path, mode="wt", encoding="utf-8") as f:
                f.write(text)
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)

    def _write_bytes(self, envelope: DataEnvelope, path: Path, compression: str) -> None:
        """Write binary payload to file."""
        data = envelope.data
        if not isinstance(data, bytes):
            raise TypeError(f"Bytes format requires bytes data, got {type(data)}")

        if compression in {"gzip", "gz"}:
            with gzip.open(path, mode="wb") as f:
                f.write(data)
        else:
            with open(path, "wb") as f:
                f.write(data)

    def _count_records_json(self, data: Any) -> int:
        """Count records in JSON data."""
        if isinstance(data, list):
            return len(data)
        return 1

    def _count_records_csv(self, data: Any) -> int:
        """Count records in CSV data."""
        if isinstance(data, list):
            return len(data)
        return 0
