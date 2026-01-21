from __future__ import annotations

from typing import Any, Dict, Optional

from metabricks.core.contracts import DataEnvelope
from metabricks.core.logger import get_logger
from metabricks.models.delta_write_options import DeltaBatchWriteOptions

from metabricks.sinks.strategies.base import WriterStrategy


class SparkFileWriterStrategy(WriterStrategy):
    """Write a batch dataframe to a filesystem path via Spark."""

    def __init__(self, *, format: str):
        self._format = format
        self.log = get_logger(self.__class__.__name__)

    def supports_payload_type(self, payload_type: str) -> bool:
        return payload_type == "dataframe"

    def write(self, envelope: DataEnvelope, config: Dict[str, Any]) -> Dict[str, Any]:
        if envelope.payload_type != "dataframe":
            raise TypeError(f"{self.__class__.__name__} requires dataframe payload")

        df = envelope.data
        path: Optional[str] = config.get("path")
        if not path:
            raise ValueError(f"{self._format} writer requires path")

        write_options: DeltaBatchWriteOptions = config.get("write_options")
        mode = write_options.mode
        partition_by = config.get("partition_by") or []
        options = config.get("options") or {}

        self.log.info(f"Writing {self._format} file: path={path}, mode={mode}, partition_by={partition_by}")
        
        writer = df.write.format(self._format).mode(mode)
        for k, v in options.items():
            writer = writer.option(k, v)
        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.save(path)
        rows = df.count()
        
        self.log.info(f"{self._format.capitalize()} write completed: record_count={rows}, location={path}")

        return {
            "kind": self._format,
            "target_location": path,
            "mode": mode,
            "status": "success",
            "record_count": rows,
        }
