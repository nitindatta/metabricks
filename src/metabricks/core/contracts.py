from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

PayloadType = Literal["dataframe", "json", "bytes", "text", "stream"]


@dataclass
class ExecutionContext:
    """Orchestrator-provided execution context for pipeline runs.
    
    Contains metadata about how the pipeline was invoked, what it's processing,
    and operational flags for audit, dedup, and replay scenarios.
    """
    run_id: str                                 # Unique run identifier (e.g., Databricks job run_id)
    pipeline_name: str                          # Pipeline identifier
    environment: Optional[str] = None           # dev/staging/prod
    source_system: Optional[str] = None         # Source system identifier
    source_dataset_id: Optional[str] = None     # Dataset/table identifier
    is_replay: bool = False                     # Backfill/reprocess flag
    batch_id: Optional[str] = None              # Batch sequence identifier
    pipeline_version: Optional[str] = None      # Code version (git SHA or semver)
    logical_date: Optional[str] = None          # Scheduler logical date / data interval boundary
    record_hash_keys: Optional[List[str]] = None  # Dedup keys for _meta.record_hash
    attach_audit_meta: bool = False             # Enable _meta struct in sinks
    metadata: Dict[str, Any] = field(default_factory=dict)  # Extensibility hook

    @classmethod
    def from_runtime_vars(
        cls,
        *,
        run_id: str,
        pipeline_name: str,
        environment: Optional[str],
        source_system: Optional[str],
        source_dataset_id: Optional[str],
        attach_audit_meta: bool,
        record_hash_keys: Optional[List[str]],
        is_replay: bool,
        batch_id: Optional[str],
        pipeline_version: Optional[str],
        runtime_vars: Optional[Dict[str, Any]] = None,
    ) -> "ExecutionContext":
        metadata = dict(runtime_vars or {})
        logical_date = metadata.pop("logical_date", None)
        return cls(
            run_id=run_id,
            pipeline_name=pipeline_name,
            environment=environment,
            source_system=source_system,
            source_dataset_id=source_dataset_id,
            attach_audit_meta=attach_audit_meta,
            record_hash_keys=record_hash_keys,
            is_replay=is_replay,
            batch_id=batch_id,
            pipeline_version=pipeline_version,
            logical_date=logical_date,
            metadata=metadata,
        )

    @property
    def invocation_id(self) -> str:
        """Backward-compatible alias for run_id."""
        return self.run_id


@dataclass
class DataEnvelope:
    payload_type: PayloadType           
    data: Any                            
    schema: Optional[Dict[str, Any]] = None
    context: Optional[ExecutionContext] = None
    metadata: Dict[str, Any] = field(default_factory=dict)  # Source-level connector metadata
    
    def __repr__(self) -> str:
        """Custom repr that truncates large data to avoid cluttering notebook output."""
        # Show metadata
        parts = [f"DataEnvelope(payload_type='{self.payload_type}'"]
        
        # Truncate data preview based on type
        if self.data is None:
            data_preview = "None"
        elif isinstance(self.data, list):
            count = len(self.data)
            if count == 0:
                data_preview = "[]"
            elif count <= 3:
                data_preview = repr(self.data)
            else:
                preview_items = self.data[:2]
                data_preview = f"[{preview_items[0]!r}, {preview_items[1]!r}, ... +{count-2} more]"
        elif isinstance(self.data, dict):
            key_count = len(self.data)
            if key_count <= 3:
                data_preview = repr(self.data)
            else:
                preview_keys = list(self.data.keys())[:2]
                data_preview = f"{{{preview_keys[0]!r}: ..., {preview_keys[1]!r}: ..., ... +{key_count-2} more keys}}"
        elif isinstance(self.data, str):
            if len(self.data) <= 100:
                data_preview = repr(self.data)
            else:
                data_preview = f"{self.data[:100]!r}... ({len(self.data)} chars)"
        elif isinstance(self.data, bytes):
            if len(self.data) <= 50:
                data_preview = repr(self.data)
            else:
                data_preview = f"{self.data[:50]!r}... ({len(self.data)} bytes)"
        else:
            # For dataframes or unknown types, show type and basic info
            data_preview = f"<{type(self.data).__name__}>"
            try:
                if hasattr(self.data, 'shape'):
                    data_preview += f" shape={self.data.shape}"
                elif hasattr(self.data, '__len__'):
                    data_preview += f" len={len(self.data)}"
            except Exception:
                pass
        
        parts.append(f", data={data_preview}")
        
        # Show context summary
        if self.context:
            parts.append(f", context=ExecutionContext(run_id='{self.context.run_id}', pipeline_name='{self.context.pipeline_name}')")
        
        if self.schema:
            parts.append(", schema={...}")
        
        parts.append(")")
        return "".join(parts)
