from __future__ import annotations

from typing import Any, Dict

from metabricks.core.contracts import DataEnvelope
from metabricks.core.logger import get_logger
from metabricks.models.metric_model import StreamingQueryProgress
from metabricks.models.delta_write_options import DeltaStreamWriteOptions

from metabricks.sinks.strategies.base import WriterStrategy


class DeltaStreamWriterStrategy(WriterStrategy):
    def __init__(self):
        self.log = get_logger(self.__class__.__name__)
    
    def supports_payload_type(self, payload_type: str) -> bool:
        return payload_type == "stream"

    def write(self, envelope: DataEnvelope, config: Dict[str, Any]) -> Dict[str, Any]:
        if envelope.payload_type != "stream":
            raise TypeError("Delta stream writer requires stream payload")

        df = envelope.data
        table_name = config["table_name"]
        run_id = config.get("run_id") or "run"
        write_options: DeltaStreamWriteOptions = config.get("write_options")
        
        # Extract from typed model
        output_mode = write_options.output_mode
        merge_schema = write_options.merge_schema
        checkpoint_location = write_options.checkpoint_location
        trigger_mode = write_options.trigger_mode
        trigger_interval = write_options.trigger_interval
        
        query_name = f"autoloader_{table_name}_{run_id}"
        self.log.info(f"Starting Delta stream write: table={table_name}, query_name={query_name}")
        self.log.info(f"Stream configuration: checkpoint={checkpoint_location}, trigger={trigger_mode}, output_mode={output_mode}")

        # Build trigger dict
        trigger_dict = self._build_trigger(trigger_mode, trigger_interval)

        query = (
            df.writeStream.format("delta")
            .outputMode(output_mode)
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true" if merge_schema else "false")
            .trigger(**trigger_dict)
            .queryName(query_name)
            .toTable(table_name)
        )
        
        self.log.info(f"Awaiting stream termination for query: {query_name}")
        query.awaitTermination()

        ex = query.exception()
        if ex is not None:
            self.log.error(f"Streaming query failed with exception: {ex}")
            raise RuntimeError(f"Streaming query failed: {ex}")

        lp = query.lastProgress
        if lp is None:
            raise RuntimeError("No lastProgress was produced (unexpected for availableNow).")

        last_progress = StreamingQueryProgress.model_validate(lp)
        record_count = last_progress.observed_metrics.get_record_count()
        
        self.log.info(f"Delta stream write completed: record_count={record_count}, table={table_name}")

        return {
            "kind": "delta",
            "target_location": table_name,
            "mode": "append",
            "status": "success",
            "record_count": record_count,
        }
    
    def _build_trigger(self, trigger_mode: str, trigger_interval: str | None) -> dict:
        """Build trigger dict for writeStream.trigger()."""
        if trigger_mode == "availableNow":
            return {"availableNow": True}
        elif trigger_mode == "continuous":
            return {"continuous": "0"}
        elif trigger_mode == "processingTime":
            if not trigger_interval:
                raise ValueError("trigger_interval required for processingTime mode")
            return {"processingTime": trigger_interval}
        else:
            self.log.warning(f"Unknown trigger mode {trigger_mode}, using availableNow")
            return {"availableNow": True}
