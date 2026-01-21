"""Test functional events bus and observers."""
import json
import os
import tempfile
import time
from pathlib import Path

from datetime import datetime, timezone

import pytest

from metabricks.core.events import (
    EventBus,
    StdoutObserver,
    VolumeJSONLObserver,
    publish_event,
    set_global_bus,
)


def test_volume_jsonl_observer_basic():
    """Test that VolumeJSONLObserver writes events to file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        observer = VolumeJSONLObserver(
            base_path=tmpdir,
            pipeline_name="test_pipeline",
            run_id="test-inv-123",
            batch_size=2,  # Small batch for testing
            flush_seconds=1,
        )
        
        bus = EventBus(
            run_id="test-inv-123",
            pipeline_name="test_pipeline",
            observers=[observer],
        )
        bus.start()
        set_global_bus(bus)
        
        # Publish some events
        publish_event(stage="test.stage1", status="started")
        publish_event(stage="test.stage1", status="completed")
        publish_event(stage="test.stage2", status="started")
        
        # Wait for worker to process and auto-flush
        time.sleep(0.5)
        
        # Shutdown to ensure all events are flushed
        bus.shutdown()
        
        # Check file exists and has content
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        expected_path = Path(tmpdir) / "test_pipeline" / date_str / "test-inv-123.jsonl"
        
        assert expected_path.exists(), f"File not created at {expected_path}"
        
        # Read and verify events
        with open(expected_path) as f:
            lines = f.readlines()
        
        assert len(lines) >= 3, f"Expected at least 3 events, got {len(lines)}"
        
        # Parse and check structure
        events = [json.loads(line) for line in lines]
        assert events[0]["stage"] == "test.stage1"
        assert events[0]["status"] == "started"
        assert events[1]["stage"] == "test.stage1"
        assert events[1]["status"] == "completed"
        assert events[1]["duration_ms"] is not None  # Auto-duration
        
        set_global_bus(None)


def test_event_bus_auto_duration():
    """Test automatic duration calculation for paired events."""
    with tempfile.TemporaryDirectory() as tmpdir:
        observer = VolumeJSONLObserver(
            base_path=tmpdir,
            pipeline_name="duration_test",
            run_id="dur-123",
            batch_size=10,
            flush_seconds=1,
        )
        
        bus = EventBus(
            run_id="dur-123",
            pipeline_name="duration_test",
            observers=[observer],
        )
        bus.start()
        set_global_bus(bus)
        
        # Publish started, wait, then completed
        publish_event(stage="slow.operation", status="started")
        time.sleep(0.1)  # 100ms delay
        publish_event(stage="slow.operation", status="completed", counts={"items": 42})
        
        # Wait and shutdown to flush
        time.sleep(0.2)
        bus.shutdown()
        
        # Read events
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        file_path = Path(tmpdir) / "duration_test" / date_str / "dur-123.jsonl"
        
        with open(file_path) as f:
            events = [json.loads(line) for line in f]
        
        # Check completed event has duration
        completed = [e for e in events if e["status"] == "completed"][0]
        assert completed["duration_ms"] is not None
        assert completed["duration_ms"] >= 100  # At least 100ms
        assert completed["counts"]["items"] == 42
        
        set_global_bus(None)


def test_event_bus_shutdown_flushes():
    """Test that shutdown flushes remaining buffered events."""
    with tempfile.TemporaryDirectory() as tmpdir:
        observer = VolumeJSONLObserver(
            base_path=tmpdir,
            pipeline_name="shutdown_test",
            run_id="shut-123",
            batch_size=100,  # Large buffer - won't auto-flush
            flush_seconds=60,  # Long timeout - won't time-flush
        )
        
        bus = EventBus(
            run_id="shut-123",
            pipeline_name="shutdown_test",
            observers=[observer],
        )
        bus.start()
        set_global_bus(bus)
        
        # Publish only 5 events (buffer is 100)
        for i in range(5):
            publish_event(stage=f"test.stage{i}", status="completed", counts={"idx": i})
        
        # Let worker process events into buffer
        time.sleep(0.2)
        
        # Shutdown - should flush remaining events
        bus.shutdown()
        
        # Check all 5 events were written
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        file_path = Path(tmpdir) / "shutdown_test" / date_str / "shut-123.jsonl"
        
        assert file_path.exists(), "File should exist after shutdown"
        
        with open(file_path) as f:
            events = [json.loads(line) for line in f]
        
        assert len(events) == 5, f"Expected 5 events, got {len(events)}"
        
        set_global_bus(None)


def test_publish_event_no_bus():
    """Test that publish_event handles missing bus gracefully."""
    set_global_bus(None)
    
    # Should not raise - just no-op
    publish_event(stage="test", status="started")
    publish_event(stage="test", status="completed")


def test_event_schema_fields():
    """Test that events have all expected schema fields."""
    with tempfile.TemporaryDirectory() as tmpdir:
        observer = VolumeJSONLObserver(
            base_path=tmpdir,
            pipeline_name="schema_test",
            run_id="schema-123",
            batch_size=1,
        )
        
        bus = EventBus(
            run_id="schema-123",
            pipeline_name="schema_test",
            environment="dev",
            batch_id="2026-01-18",
            pipeline_version="v1.0",
            observers=[observer],
        )
        bus.start()
        set_global_bus(bus)
        
        publish_event(
            stage="test.full",
            status="completed",
            counts={"rows": 100},
            details={"table": "users"},
            error=None,
        )
        
        # Wait and shutdown to ensure flush
        time.sleep(0.2)
        bus.shutdown()
        
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        file_path = Path(tmpdir) / "schema_test" / date_str / "schema-123.jsonl"
        
        with open(file_path) as f:
            event = json.loads(f.readline())
        
        # Check all schema fields present
        assert event["schema_version"] == "1.0"
        assert "event_id" in event
        assert event["seq_no"] >= 1
        assert "ts" in event
        assert event["run_id"] == "schema-123"
        assert event["invocation_id"] == "schema-123"
        assert event["pipeline_name"] == "schema_test"
        assert event["stage"] == "test.full"
        assert event["status"] == "completed"
        assert event["counts"]["rows"] == 100
        assert event["details"]["table"] == "users"
        assert event["environment"] == "dev"
        assert event["batch_id"] == "2026-01-18"
        assert event["pipeline_version"] == "v1.0"
        
        set_global_bus(None)


if __name__ == "__main__":
    # Run tests manually for debugging
    print("Test 1: Basic volume write")
    test_volume_jsonl_observer_basic()
    print("✓ Passed")
    
    print("\nTest 2: Auto-duration")
    test_event_bus_auto_duration()
    print("✓ Passed")
    
    print("\nTest 3: Shutdown flush")
    test_event_bus_shutdown_flushes()
    print("✓ Passed")
    
    print("\nTest 4: No bus graceful")
    test_publish_event_no_bus()
    print("✓ Passed")
    
    print("\nTest 5: Schema fields")
    test_event_schema_fields()
    print("✓ Passed")
    
    print("\n✓ All tests passed!")
