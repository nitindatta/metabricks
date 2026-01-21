from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from metabricks.models.sink_config import SinkConfig
from metabricks.models.source_config import SourceConfig


class ExecutionContextConfig(BaseModel):
    """Design-time configuration for execution context and audit metadata.
    
    These are static decisions about HOW the pipeline should behave.
    Runtime parameters (batch_id, is_replay, pipeline_version) should be
    provided by the orchestrator/scheduler at execution time.
    """
    attach_audit_meta: bool = True              # Enable _meta struct with audit columns in sinks
    record_hash_keys: Optional[List[str]] = None  # Column names for deduplication hash (_meta.record_hash)


class PipelineConfig(BaseModel):
    # --- metadata (mirrors legacy GenericPipelineCfg) ---
    pipeline_name: str

    job_name: Optional[str] = None

    source_dataset_id: Optional[str] = None
    source_dataset_name: Optional[str] = None
    source_dataset_version: int = 1
    source_dataset_profile: str = "small"

    source_system: Optional[str] = None
    environment: Literal["dev", "qa", "prod"] = "dev"

    trigger_cron: Optional[str] = None
    tags: List[str] = Field(default_factory=list)

    copy_enabled: bool = True
    copy_priority: int = 1

    # --- Execution context configuration ---
    execution_context: ExecutionContextConfig = Field(default_factory=ExecutionContextConfig)

    # Forward-only config: source uses discriminated union models.
    source: SourceConfig

    # Forward-only sink config (optional to keep source-only pipelines working).
    sink: Optional[SinkConfig] = None
