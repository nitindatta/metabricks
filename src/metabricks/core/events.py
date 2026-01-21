from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from queue import Queue, Full, Empty
from typing import Any, Dict, List, Optional

DEFAULT_SCHEMA_VERSION = "1.0"

# Optional global bus for framework-wide access without changing signatures
_GLOBAL_BUS: Optional["EventBus"] = None


def set_global_bus(bus: Optional["EventBus"]) -> None:
    global _GLOBAL_BUS
    _GLOBAL_BUS = bus


def get_global_bus() -> Optional["EventBus"]:
    return _GLOBAL_BUS


def publish_event(
    *,
    stage: str,
    status: str,
    duration_ms: Optional[int] = None,
    counts: Optional[Dict[str, Any]] = None,
    details: Optional[Dict[str, Any]] = None,
    error: Optional[Dict[str, Any]] = None,
) -> None:
    """Module-level helper to publish events to the global bus (if configured).
    
    Handles None checks and exceptions internally; safe to call always.
    Similar to logging API but for functional events.
    """
    bus = _GLOBAL_BUS
    if bus is None:
        return
    try:
        bus.publish(
            stage=stage,
            status=status,
            duration_ms=duration_ms,
            counts=counts,
            details=details,
            error=error,
        )
    except Exception:
        # Swallow observer/bus errors; don't impact pipeline
        pass


class timed_stage:
    """Context manager to track duration of a stage and publish start/complete/fail events.
    
    Usage:
        with timed_stage("source.extract"):
            data = connector.extract()
        
        with timed_stage("sink.write", details={"table": "users"}):
            sink.write(data)
    """
    
    def __init__(
        self,
        stage: str,
        *,
        counts: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.stage = stage
        self.counts = counts
        self.details = details
        self._start_ms: Optional[int] = None
    
    def __enter__(self) -> "timed_stage":
        self._start_ms = int(time.time() * 1000)
        publish_event(stage=self.stage, status="started", details=self.details)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):  # type: ignore
        duration_ms = int(time.time() * 1000) - (self._start_ms or 0)
        if exc_type is not None:
            # Exception occurred
            error_info = {
                "code": exc_type.__name__,
                "message": str(exc_val),
            }
            publish_event(
                stage=self.stage,
                status="failed",
                duration_ms=duration_ms,
                counts=self.counts,
                details=self.details,
                error=error_info,
            )
        else:
            # Success
            publish_event(
                stage=self.stage,
                status="completed",
                duration_ms=duration_ms,
                counts=self.counts,
                details=self.details,
            )
        return False  # Don't suppress exceptions


@dataclass
class FunctionalEvent:
    """Structured functional event for pipeline lifecycle and metrics.

    Decoupled from debug logging; designed for stdout progress and durable JSONL.
    """

    schema_version: str = DEFAULT_SCHEMA_VERSION
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    seq_no: int = 0
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    run_id: str = "-"
    pipeline_name: str = "-"

    stage: str = "-"  # e.g., pipeline.start, source.extract, sink.write
    status: str = "-"  # started|in_progress|completed|failed

    duration_ms: Optional[int] = None
    counts: Optional[Dict[str, Any]] = None
    details: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

    # Snapshot of execution context (optional, for correlation)
    environment: Optional[str] = None
    batch_id: Optional[str] = None
    pipeline_version: Optional[str] = None


class EventObserver:
    """Observer interface for handling functional events."""

    # Whether this observer should be periodically flushed when the queue is idle.
    # Observers that are intended to write only at the end (e.g., MemoryJSONLObserver)
    # should set this to False to avoid expensive repeated writes.
    periodic_flush: bool = True

    def handle(self, event: FunctionalEvent) -> None:  # pragma: no cover
        raise NotImplementedError


class StdoutObserver(EventObserver):
    """Emit concise human-readable progress to stdout (not via debug logger)."""

    def handle(self, event: FunctionalEvent) -> None:
        # Minimal one-line render for run UI
        counts = event.counts or {}
        details = event.details or {}
        duration = f" duration_ms={event.duration_ms}" if event.duration_ms is not None else ""
        msg = (
            f"{event.ts} | run={event.run_id} | pipe={event.pipeline_name} | "
            f"{event.stage} {event.status}{duration}"
        )
        if counts:
            msg += f" | counts={counts}"
        if details:
            # keep details brief; avoid dumping large dicts
            brief = {k: details[k] for k in list(details.keys())[:4]}
            msg += f" | details={brief}"
        if event.error:
            brief_err = {k: event.error.get(k) for k in ("code", "message") if k in event.error}
            msg += f" | error={brief_err}"
        print(msg)


class VolumeJSONLObserver(EventObserver):
    """Buffered JSONL writer to a UC Volume path.

    Writes one JSON object per line; flushes by batch size or time interval.
    """

    def __init__(
        self,
        base_path: str,
        pipeline_name: str,
        run_id: Optional[str] = None,
        *,
        invocation_id: Optional[str] = None,
        batch_size: int = 200,
        flush_seconds: int = 10,
        flush_each: bool = False,
        debug_errors: bool = False,
    ) -> None:
        if run_id is None and invocation_id is None:
            raise ValueError("VolumeJSONLObserver requires run_id (preferred) or invocation_id")
        if run_id is None:
            run_id = invocation_id
        if invocation_id is None:
            invocation_id = run_id
        if run_id is not None and invocation_id is not None and str(run_id) != str(invocation_id):
            raise ValueError("Both run_id and invocation_id were provided with different values")

        self.batch_size = max(1, batch_size)
        self.flush_seconds = max(1, flush_seconds)
        self.flush_each = flush_each
        self.debug_errors = debug_errors
        self._buf: List[str] = []
        self._last_flush = time.time()

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # Layout: /Volumes/.../pipeline_events/<pipeline_name>/<YYYY-MM-DD>/<run_id>.jsonl
        self.dir_path = os.path.join(base_path, pipeline_name, date_str)
        self.file_path = os.path.join(self.dir_path, f"{run_id}.jsonl")

        try:
            os.makedirs(self.dir_path, exist_ok=True)
        except Exception:
            # Degrade gracefully if path cannot be created; later writes will fail silently
            pass

    def handle(self, event: FunctionalEvent) -> None:
        try:
            payload = dict(event.__dict__)
            # Backward-compatible alias in serialized schema
            payload["invocation_id"] = event.run_id
            rec = json.dumps(payload, ensure_ascii=False)
        except Exception:
            # Fallback minimal record
            rec = json.dumps({
                "schema_version": event.schema_version,
                "ts": event.ts,
                "invocation_id": event.run_id,
                "run_id": event.run_id,
                "pipeline_name": event.pipeline_name,
                "stage": event.stage,
                "status": event.status,
            })
        self._buf.append(rec)
        if self.flush_each:
            self.flush()
            return
        now = time.time()
        if len(self._buf) >= self.batch_size or (now - self._last_flush) >= self.flush_seconds:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        try:
            content_bytes = ("\n".join(self._buf) + "\n").encode("utf-8")
            
            # For UC Volumes, use binary mode with seek to avoid "Illegal seek" errors
            # Open in read+append binary mode
            try:
                with open(self.file_path, "ab") as f:
                    f.write(content_bytes)
            except OSError as e:
                if e.errno == 29:  # Illegal seek on UC Volumes
                    # Fallback: accumulate everything and overwrite (last resort)
                    # Read existing content
                    try:
                        with open(self.file_path, "rb") as f:
                            existing = f.read()
                    except FileNotFoundError:
                        existing = b""
                    # Write combined content
                    with open(self.file_path, "wb") as f:
                        f.write(existing + content_bytes)
                else:
                    raise
            
            self._buf.clear()
            self._last_flush = time.time()
        except Exception as e:
            if self.debug_errors:
                print(f"[VolumeJSONLObserver] flush failed: {type(e).__name__}: {e}")
            if len(self._buf) > (self.batch_size * 10):
                self._buf.clear()


class MemoryJSONLObserver(EventObserver):
    """In-memory JSONL buffer that writes once on flush.

    Intended for orchestrator-level events in Databricks to avoid mid-run I/O.
    """

    periodic_flush: bool = False

    def __init__(
        self,
        base_path: str,
        pipeline_name: str,
        run_id: Optional[str] = None,
        *,
        invocation_id: Optional[str] = None,
        debug_errors: bool = False,
    ) -> None:
        if run_id is None and invocation_id is None:
            raise ValueError("MemoryJSONLObserver requires run_id (preferred) or invocation_id")
        if run_id is None:
            run_id = invocation_id
        if invocation_id is None:
            invocation_id = run_id
        if run_id is not None and invocation_id is not None and str(run_id) != str(invocation_id):
            raise ValueError("Both run_id and invocation_id were provided with different values")

        self.debug_errors = debug_errors
        self._buf: List[str] = []
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.dir_path = os.path.join(base_path, pipeline_name, date_str)
        self.file_path = os.path.join(self.dir_path, f"{run_id}.jsonl")
        try:
            os.makedirs(self.dir_path, exist_ok=True)
        except Exception:
            pass

    def handle(self, event: FunctionalEvent) -> None:
        try:
            payload = dict(event.__dict__)
            # Backward-compatible alias in serialized schema
            payload["invocation_id"] = event.run_id
            rec = json.dumps(payload, ensure_ascii=False)
        except Exception:
            rec = json.dumps({
                "schema_version": event.schema_version,
                "ts": event.ts,
                "invocation_id": event.run_id,
                "run_id": event.run_id,
                "pipeline_name": event.pipeline_name,
                "stage": event.stage,
                "status": event.status,
            })
        self._buf.append(rec)

    def flush(self) -> None:
        if not self._buf:
            return
        try:
            content_bytes = ("\n".join(self._buf) + "\n").encode("utf-8")
            # Single overwrite write; safest for UC Volumes
            with open(self.file_path, "wb") as f:
                f.write(content_bytes)
            self._buf.clear()
        except Exception as e:
            if self.debug_errors:
                print(f"[MemoryJSONLObserver] flush failed: {type(e).__name__}: {e}")


class EventBus:
    """Async event bus with background dispatcher and bounded queue.
    
    Automatically tracks duration for paired started/completed events.
    """

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        pipeline_name: str,
        invocation_id: Optional[str] = None,
        environment: Optional[str] = None,
        batch_id: Optional[str] = None,
        pipeline_version: Optional[str] = None,
        observers: Optional[List[EventObserver]] = None,
        queue_size: int = 10_000,
    ) -> None:
        if run_id is None and invocation_id is None:
            raise ValueError("EventBus requires run_id (preferred) or invocation_id")
        if run_id is None:
            run_id = invocation_id
        if invocation_id is None:
            invocation_id = run_id
        if run_id is not None and invocation_id is not None and str(run_id) != str(invocation_id):
            raise ValueError("Both run_id and invocation_id were provided with different values")

        self.run_id = str(run_id)
        # Backward-compat alias
        self.invocation_id = self.run_id
        self.pipeline_name = pipeline_name
        self.environment = environment
        self.batch_id = batch_id
        self.pipeline_version = pipeline_version

        self._observers: List[EventObserver] = observers or []
        self._q: Queue[FunctionalEvent] = Queue(maxsize=max(1, queue_size))
        self._seq_no = 0
        self._running = False
        self._worker: Optional[threading.Thread] = None
        self._dropped = 0
        self._stage_start_times: Dict[str, int] = {}  # Track start time per stage for auto-duration

    def _dispatch_loop(self) -> None:
        while self._running:
            try:
                evt = self._q.get(timeout=0.5)
            except Empty:
                # periodic flush on observers that support it
                for obs in self._observers:
                    if getattr(obs, "periodic_flush", True) is False:
                        continue
                    flush = getattr(obs, "flush", None)
                    if callable(flush):
                        try:
                            flush()
                        except Exception:
                            pass
                continue

            for obs in self._observers:
                try:
                    obs.handle(evt)
                except Exception:
                    # Isolate observer failures
                    pass

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Non-daemon thread to ensure flush completes in notebook environments
        self._worker = threading.Thread(target=self._dispatch_loop, name="functional_event_bus", daemon=False)
        self._worker.start()

    def shutdown(self) -> None:
        if not self._running:
            return
        # Signal stop
        self._running = False
        # Give worker thread time to finish current batch (important for Databricks notebooks)
        if self._worker is not None and self._worker.is_alive():
            try:
                self._worker.join(timeout=2.0)  # Wait up to 2 seconds
            except Exception:
                pass
        # Drain any remaining items in queue
        while not self._q.empty():
            try:
                evt = self._q.get_nowait()
            except Exception:
                break
            for obs in self._observers:
                try:
                    obs.handle(evt)
                except Exception:
                    pass
        # Final flush on all observers
        for obs in self._observers:
            flush = getattr(obs, "flush", None)
            if callable(flush):
                try:
                    flush()
                except Exception:
                    pass
        self._worker = None

    def publish(
        self,
        *,
        stage: str,
        status: str,
        duration_ms: Optional[int] = None,
        counts: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
    ) -> None:
        # Auto-duration tracking for paired started/completed events
        now_ms = int(time.time() * 1000)
        
        if status == "started":
            # Record start time for this stage
            self._stage_start_times[stage] = now_ms
        elif status in ("completed", "failed") and duration_ms is None:
            # Auto-calculate duration if we have a start time
            start_ms = self._stage_start_times.pop(stage, None)
            if start_ms is not None:
                duration_ms = now_ms - start_ms
        
        # Build event with context
        self._seq_no += 1
        evt = FunctionalEvent(
            seq_no=self._seq_no,
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            stage=stage,
            status=status,
            duration_ms=duration_ms,
            counts=counts,
            details=details,
            error=error,
            environment=self.environment,
            batch_id=self.batch_id,
            pipeline_version=self.pipeline_version,
        )
        try:
            self._q.put_nowait(evt)
        except Full:
            # Drop and summarize; avoid blocking producers
            self._dropped += 1
            if self._dropped % 100 == 0:
                # Emit a summary record when many drops occur
                summary = FunctionalEvent(
                    seq_no=self._seq_no + 1,
                    run_id=self.run_id,
                    pipeline_name=self.pipeline_name,
                    stage="event_bus.backpressure",
                    status="dropped",
                    details={"dropped": self._dropped},
                )
                try:
                    self._q.put_nowait(summary)
                except Exception:
                    pass


def _env_flag(name: str, default: str) -> str:
    val = os.getenv(name)
    return val if val not in (None, "") else default


def build_default_bus(
    *,
    run_id: Optional[str] = None,
    invocation_id: Optional[str] = None,
    pipeline_name: str,
    environment: Optional[str] = None,
    batch_id: Optional[str] = None,
    pipeline_version: Optional[str] = None,
) -> Optional[EventBus]:
    """Construct a default EventBus using environment variables for simple enablement.

    METABRICKS_EVENTS_ENABLED: "true" | "false" (default: "true")
    METABRICKS_EVENTS_TRANSPORTS: comma list (default: "stdout,volume_jsonl")
    METABRICKS_EVENTS_VOLUME_PATH: base path (default: "/Volumes/main/ops/pipeline_events")
    METABRICKS_EVENTS_BUFFER_SIZE: int (default: 50)
    METABRICKS_EVENTS_FLUSH_SECS: int (default: 5)
    METABRICKS_EVENTS_FLUSH_EACH: "true" | "false" (default: "false")
    METABRICKS_EVENTS_DEBUG_ERRORS: "true" | "false" (default: "false")
    METABRICKS_EVENTS_WRITE_MODE: "stream" | "end" (default: "end")
    METABRICKS_EVENTS_QUEUE_SIZE: int (default: 10000)
    """
    if run_id is None and invocation_id is None:
        raise ValueError("build_default_bus requires run_id (preferred) or invocation_id")
    if run_id is None:
        run_id = invocation_id
    if invocation_id is None:
        invocation_id = run_id
    if run_id is not None and invocation_id is not None and str(run_id) != str(invocation_id):
        raise ValueError("Both run_id and invocation_id were provided with different values")

    enabled = _env_flag("METABRICKS_EVENTS_ENABLED", "true").lower() == "true"
    if not enabled:
        return None

    transports = [s.strip() for s in _env_flag("METABRICKS_EVENTS_TRANSPORTS", "stdout,volume_jsonl").split(",") if s.strip()]
    volume_path = _env_flag("METABRICKS_EVENTS_VOLUME_PATH", "/Volumes/main/ops/pipeline_events")
    try:
        buffer_size = int(_env_flag("METABRICKS_EVENTS_BUFFER_SIZE", "50"))
    except Exception:
        buffer_size = 50
    try:
        flush_secs = int(_env_flag("METABRICKS_EVENTS_FLUSH_SECS", "5"))
    except Exception:
        flush_secs = 5
    try:
        q_size = int(_env_flag("METABRICKS_EVENTS_QUEUE_SIZE", "10000"))
    except Exception:
        q_size = 10000

    flush_each = _env_flag("METABRICKS_EVENTS_FLUSH_EACH", "false").lower() == "true"
    debug_errors = _env_flag("METABRICKS_EVENTS_DEBUG_ERRORS", "false").lower() == "true"

    observers: List[EventObserver] = []
    if "stdout" in transports:
        observers.append(StdoutObserver())
    if "volume_jsonl" in transports:
        write_mode = _env_flag("METABRICKS_EVENTS_WRITE_MODE", "end").lower()
        if write_mode == "end":
            observers.append(MemoryJSONLObserver(
                base_path=volume_path,
                pipeline_name=pipeline_name,
                run_id=str(run_id),
                debug_errors=debug_errors,
            ))
        else:
            observers.append(VolumeJSONLObserver(
                base_path=volume_path,
                pipeline_name=pipeline_name,
                run_id=str(run_id),
                batch_size=buffer_size,
                flush_seconds=flush_secs,
                flush_each=flush_each,
                debug_errors=debug_errors,
            ))

    bus = EventBus(
        run_id=str(run_id),
        pipeline_name=pipeline_name,
        environment=environment,
        batch_id=batch_id,
        pipeline_version=pipeline_version,
        observers=observers,
        queue_size=q_size,
    )
    return bus
