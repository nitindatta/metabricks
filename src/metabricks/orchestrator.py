from __future__ import annotations

import uuid
from typing import Optional, Union, Dict, Any

from metabricks.bootstrap import load_builtin_plugins
from metabricks.connectors.registry import ConnectorRegistry
from metabricks.core.contracts import DataEnvelope, ExecutionContext
from metabricks.core.secrets_provider import SecretsProvider
from metabricks.core.logger import get_logger, configure_root_logger, push_run_id, reset_run_id
from metabricks.core.template_resolution import default_template_vars, resolve_templates
from metabricks.models.pipeline_config import PipelineConfig
from metabricks.models.source_config import SourceConfig
from metabricks.sinks.registry import SinkRegistry
from metabricks.wiring.source_registry import SourceWiringRegistry
from metabricks.wiring.sink_registry import SinkWiringRegistry
from metabricks.core.events import build_default_bus, set_global_bus, publish_event


class MetadataOrchestrator:
    """
    High-level orchestrator for executing metadata extraction and ingestion pipelines.
    
    Provides a unified interface for running source extraction, transformations,
    and sink writing operations defined in a PipelineConfig.
    
    Example:
        >>> from metabricks import MetadataOrchestrator
        >>> from metabricks.models.pipeline_config import PipelineConfig
        >>> config = PipelineConfig.from_dict(config_dict)
        >>> orchestrator = MetadataOrchestrator()
        >>> result = orchestrator.run(config)
    """
    
    def __init__(
        self,
        run_id: Optional[str | int] = None,
        *,
        invocation_id: Optional[str | int] = None,
        batch_id: Optional[str] = None,
        pipeline_version: Optional[str] = None,
        is_replay: bool = False,
    ):
        """
        Initialize the orchestrator.
        
        Args:
            run_id: Unique identifier for this orchestration run (e.g., Databricks job run_id).
                    If not provided, a UUID will be generated.
            invocation_id: Deprecated alias for run_id.
            batch_id: Optional batch sequence identifier (e.g., from scheduler).
            pipeline_version: Code version (git SHA or semver) for audit trail.
            is_replay: Flag indicating this is a backfill/rerun operation.
        """
        if run_id is None and invocation_id is not None:
            run_id = invocation_id
        if run_id is not None and invocation_id is not None and str(run_id) != str(invocation_id):
            raise ValueError("Both run_id and invocation_id were provided with different values")

        self.run_id = str(run_id) if run_id is not None else str(uuid.uuid4())
        self.batch_id = batch_id
        self.pipeline_version = pipeline_version
        self.is_replay = is_replay
    
    def run(
        self,
        cfg: Union[Dict[str, Any], PipelineConfig],
        secrets_provider: Optional[SecretsProvider] = None,
        *,
        runtime_vars: Optional[Dict[str, Any]] = None,
    ) -> DataEnvelope:
        """
        Execute the pipeline defined by the configuration.
        
        Args:
            cfg: Pipeline configuration as either:
                - A dict (will be validated and converted to PipelineConfig)
                - A PipelineConfig model object (already validated by pydantic)
            secrets_provider: Optional custom secrets provider. If not provided,
                            the default provider will be used.
        
        Returns:
            DataEnvelope containing extracted data and execution hints (run_id,
            pipeline_name, sink_audit, etc.)
        
        Raises:
            TypeError: If connector.extract() does not return DataEnvelope.
            ValidationError: If config dict is invalid (pydantic will raise this).
            
        Examples:
            # Option 1: Pass a dict (validated internally)
            >>> config_dict = {"pipeline_name": "test", "source": {...}, "sink": None}
            >>> result = orchestrator.run(config_dict)
            
            # Option 2: Pass a validated PipelineConfig model
            >>> config = PipelineConfig.from_dict(config_dict)
            >>> result = orchestrator.run(config)
        """
        # Ensure logging to notebook stdout and context propagation
        configure_root_logger("INFO")
        log = get_logger(__name__)

        # Optional: resolve runtime template variables across the entire config.
        # This is intentionally conservative: it only replaces {{var}} / ${var} where
        # `var` matches a safe identifier pattern (so e.g. {{secrets/foo}} is untouched).
        # Runtime vars can reference user-supplied values (snapshot_date, country, etc.)
        # plus built-in extract_ts_* and orchestrator context fields.
        if runtime_vars is not None or True:  # Always resolve to support extract_ts_* defaults
            template_vars: Dict[str, Any] = {}
            template_vars.update(default_template_vars())
            # Add orchestrator context fields so they can be used in templates
            template_vars.update(
                {
                    "run_id": self.run_id,
                    "batch_id": self.batch_id,
                    "pipeline_version": self.pipeline_version,
                    "is_replay": str(self.is_replay),
                }
            )
            # User-supplied runtime_vars take precedence
            if runtime_vars:
                template_vars.update(runtime_vars)

            if isinstance(cfg, PipelineConfig):
                cfg_dict = cfg.model_dump(mode="json")
                cfg_dict = resolve_templates(cfg_dict, template_vars)
                cfg = PipelineConfig.model_validate(cfg_dict)
            else:
                cfg = resolve_templates(cfg, template_vars)

        # Convert dict to PipelineConfig if needed
        if isinstance(cfg, dict):
            cfg = PipelineConfig.model_validate(cfg)

        token = push_run_id(self.run_id)
        # Build and start functional event bus (stdout + Volume JSONL by default)
        bus = build_default_bus(
            run_id=self.run_id,
            pipeline_name=cfg.pipeline_name,
            environment=cfg.environment,
            batch_id=self.batch_id,
            pipeline_version=self.pipeline_version,
        )
        if bus is not None:
            bus.start()
            set_global_bus(bus)
        publish_event(stage="pipeline", status="started")
        try:
            log.info("Starting pipeline run", extra={})
            result = run_pipeline(
                run_id=self.run_id,
                cfg=cfg,
                secrets_provider=secrets_provider,
                batch_id=self.batch_id,
                pipeline_version=self.pipeline_version,
                is_replay=self.is_replay,
                runtime_vars=runtime_vars,
            )
            log.info("Completed pipeline run")
            publish_event(stage="pipeline", status="completed")
            return result
        finally:
            reset_run_id(token)
            if bus is not None:
                try:
                    bus.shutdown()
                except Exception:
                    pass
            set_global_bus(None)


def run_pipeline(
    run_id: str,
    cfg: PipelineConfig,
    *,
    secrets_provider: Optional[SecretsProvider] = None,
    batch_id: Optional[str] = None,
    pipeline_version: Optional[str] = None,
    is_replay: bool = False,
    runtime_vars: Optional[Dict[str, Any]] = None,
) -> DataEnvelope:
    log = get_logger(__name__)
    
    load_builtin_plugins()
    log.info(f"Pipeline '{cfg.pipeline_name}' started with run_id={run_id}")

    source: SourceConfig = cfg.source
    log.info(f"Initializing source connector: system_type={source.system_type}, mode={source.extraction_mode}")
    publish_event(stage="source.init", status="started", details={"system_type": source.system_type, "mode": source.extraction_mode})

    connector_cls = ConnectorRegistry.get(source.system_type, source.extraction_mode)
    builder = SourceWiringRegistry.get(source.system_type, source.extraction_mode)

    built = builder(source=source, secrets_provider=secrets_provider)
    connector = connector_cls(*built.args, **built.kwargs)
    log.info(f"Source connector initialized: {connector.__class__.__name__}")
    publish_event(stage="source.init", status="completed", details={"connector": connector.__class__.__name__})

    log.info("Starting data extraction from source")
    publish_event(stage="source.extract", status="started")
    env = connector.extract()
    if not isinstance(env, DataEnvelope):
        raise TypeError("Connector.extract() must return DataEnvelope")
    publish_event(stage="source.extract", status="completed", details={"payload_type": env.payload_type})
    
    log.info(f"Data extraction completed: payload_type={env.payload_type}")
    
    # Create ExecutionContext merging:
    # - Runtime parameters (from orchestrator: batch_id, version, replay flag)
    # - Design-time config (from JSON: attach_audit_meta, record_hash_keys)
    exec_cfg = cfg.execution_context
    context = ExecutionContext(
        run_id=run_id,
        pipeline_name=cfg.pipeline_name,
        environment=cfg.environment,
        source_system=cfg.source_system,
        source_dataset_id=cfg.source_dataset_id,
        attach_audit_meta=exec_cfg.attach_audit_meta,
        record_hash_keys=exec_cfg.record_hash_keys,
        # Runtime parameters from orchestrator
        is_replay=is_replay,
        batch_id=batch_id,
        pipeline_version=pipeline_version,
        metadata=dict(runtime_vars or {}),
    )
    env.context = context

    if cfg.sink is not None:
        sink_cfg = cfg.sink
        mode = getattr(sink_cfg, "mode", "batch")
        log.info(f"Initializing sink: system_type={sink_cfg.system_type}, mode={mode}")
        publish_event(stage="sink.init", status="started", details={"system_type": sink_cfg.system_type, "mode": mode})

        sink_cls = SinkRegistry.get(sink_cfg.system_type, mode)
        sink_builder = SinkWiringRegistry.get(sink_cfg.system_type, mode)
        built_sink = sink_builder(sink=sink_cfg, secrets_provider=secrets_provider)
        sink = sink_cls(*built_sink.args, **built_sink.kwargs)
        log.info(f"Sink initialized: {sink.__class__.__name__}")
        publish_event(stage="sink.init", status="completed", details={"sink": sink.__class__.__name__})

        log.info("Starting data write to sink")
        publish_event(stage="sink.write", status="started")
        audit = sink.write(env)
        # Store audit result in context metadata for backward compat
        if env.context:
            env.context.metadata["sink_audit"] = audit
        publish_event(
            stage="sink.write",
            status="completed",
            counts={"records_written": audit.get("record_count")} if isinstance(audit, dict) else None,
            details={"status": audit.get("status")} if isinstance(audit, dict) else None
        )
        log.info(f"Data write completed: status={audit.get('status')}, record_count={audit.get('record_count')}")
    else:
        log.info("No sink configured, skipping write operation")

    log.info(f"Pipeline '{cfg.pipeline_name}' completed successfully")
    return env


