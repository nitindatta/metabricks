"""Test MemoryJSONLObserver writes at end only."""
import json
import os
import tempfile
import time
from pathlib import Path

from datetime import datetime, timezone

from metabricks.core.events import MemoryJSONLObserver, EventBus, set_global_bus, publish_event


def test_memory_jsonl_observer_end_flush():
    with tempfile.TemporaryDirectory() as tmpdir:
        observer = MemoryJSONLObserver(
            base_path=tmpdir,
            pipeline_name="end_mode",
            run_id="end-123",
        )
        bus = EventBus(
            run_id="end-123",
            pipeline_name="end_mode",
            observers=[observer],
        )
        bus.start()
        set_global_bus(bus)

        publish_event(stage="pipeline.start", status="started")
        publish_event(stage="source.init", status="started")
        publish_event(stage="source.init", status="completed")
        publish_event(stage="source.extract", status="started")
        publish_event(stage="source.extract", status="completed")
        publish_event(stage="pipeline.complete", status="completed")

        # No intermediate file writes, only on shutdown
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        file_path = Path(tmpdir) / "end_mode" / date_str / "end-123.jsonl"
        assert not file_path.exists(), "File should not exist before flush"

        # Shutdown triggers flush
        bus.shutdown()
        assert file_path.exists(), "File should exist after flush"
        with open(file_path) as f:
            events = [json.loads(line) for line in f]
        assert len(events) == 6, f"Expected 6 events, got {len(events)}"
        set_global_bus(None)


if __name__ == "__main__":
    test_memory_jsonl_observer_end_flush()
    print("✓ MemoryJSONLObserver end flush passed")
