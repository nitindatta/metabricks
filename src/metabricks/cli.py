"""
Command-line interface and entry points for metabricks framework.

This module provides the public API that clients call from Databricks notebooks
or Python jobs. It abstracts the internal implementation details.

Note: When running connectors that require pyspark or delta (e.g., Databricks sinks),
these are expected to be available in the Databricks environment. They are NOT 
bundled in the wheel to reduce size.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from metabricks.bootstrap import load_builtin_plugins
from metabricks.connectors.registry import ConnectorRegistry
from metabricks.orchestrator import MetadataOrchestrator
from metabricks.core.logger import get_logger
from metabricks.models.pipeline_config import PipelineConfig
from metabricks.sinks.registry import SinkRegistry
from metabricks.wiring.source_registry import SourceWiringRegistry
from metabricks.wiring.sink_registry import SinkWiringRegistry

logger = get_logger(__name__)


def main(
    config_path: Optional[str] = None,
    config_dict: Optional[Dict[str, Any]] = None,
    *,
    runtime_vars: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Main entry point for orchestrator execution.
    
    Can be called with either:
    - A config file path (JSON/YAML)
    - A config dictionary (programmatic)
    
    Args:
        config_path: Path to JSON/YAML configuration file
        config_dict: Direct configuration dictionary
        
    Returns:
        Execution result with status, stats, and any warnings
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If neither config_path nor config_dict provided
        Exception: If pipeline execution fails
        
    Example (From Databricks Notebook):
        >>> from metabricks.cli import main
        >>> config = {"name": "test", "source": {...}, "sink": {...}}
        >>> result = main(config_dict=config)
        >>> print(f"Status: {result['status']}")
    """
    try:
        # Load configuration from file or dict
        if config_dict:
            config = config_dict
            logger.info("Using provided config dictionary")
        elif config_path:
            config_file = Path(config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")

            with open(config_file, "r") as f:
                if config_file.suffix == ".json":
                    config = json.load(f)
                elif config_file.suffix in (".yaml", ".yml"):
                    try:
                        import yaml
                        config = yaml.safe_load(f)
                    except ImportError:
                        raise ImportError(
                            "PyYAML required for YAML configs. "
                            "Install with: pip install pyyaml"
                        )
                else:
                    raise ValueError(
                        f"Unsupported config format: {config_file.suffix}. "
                        "Use .json or .yaml"
                    )
            logger.info(f"Loaded config from {config_path}")
        else:
            raise ValueError(
                "Either config_path or config_dict must be provided"
            )

        # Execute using the public orchestrator API.
        pipeline_name = config.get("pipeline_name") or config.get("name") or "unnamed"
        logger.info(f"Starting pipeline: {pipeline_name}")

        orchestrator = MetadataOrchestrator()
        env = orchestrator.run(config, runtime_vars=runtime_vars)

        # Serialize execution context for JSON output
        context_dict = {}
        if env.context:
            context_dict = {
                "run_id": env.context.run_id,
                "pipeline_name": env.context.pipeline_name,
                "environment": env.context.environment,
                "source_system": env.context.source_system,
                "source_dataset_id": env.context.source_dataset_id,
                "is_replay": env.context.is_replay,
                "batch_id": env.context.batch_id,
                "pipeline_version": env.context.pipeline_version,
                "metadata": env.context.metadata,
            }

        result: Dict[str, Any] = {
            "status": "success",
            "pipeline_name": pipeline_name,
            "payload_type": env.payload_type,
            "context": context_dict,
        }

        logger.info("Pipeline completed with status: success")
        return result

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        raise


def validate_config(config_path: str) -> bool:
    """
    Validate configuration without executing the pipeline.
    
    Useful for checking config correctness before scheduling jobs.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        True if configuration is valid
        
    Raises:
        Exception: If configuration is invalid
        
    Example:
        >>> validate_config("/path/to/config.json")
        True
    """
    try:
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_file, "r") as f:
            config = json.load(f)

        logger.info(f"Validating config: {config_path}")

        # Validate schema.
        cfg = PipelineConfig.model_validate(config)

        # Validate that required plugins are registered.
        load_builtin_plugins()
        _ = ConnectorRegistry.get(cfg.source.system_type, cfg.source.extraction_mode)
        _ = SourceWiringRegistry.get(cfg.source.system_type, cfg.source.extraction_mode)

        if cfg.sink is not None:
            _ = SinkRegistry.get(cfg.sink.system_type, cfg.sink.mode)
            _ = SinkWiringRegistry.get(cfg.sink.system_type, cfg.sink.mode)

        logger.info("Configuration is valid")
        return True

    except Exception as e:
        logger.error(f"Config validation failed: {str(e)}")
        raise


def cli() -> None:
    """
    Command-line interface for metabricks.
    
    Supports subcommands:
    - run: Execute a pipeline
    - validate: Validate a configuration
    
    Usage:
        metabricks-run run /path/to/config.json
        metabricks-run validate /path/to/config.json
    """
    parser = argparse.ArgumentParser(
        prog="metabricks",
        description="OECD Metadata Ingestion Framework for Databricks"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        help="Command to execute"
    )

    # 'run' subcommand
    run_parser = subparsers.add_parser(
        "run",
        help="Run metadata ingestion pipeline"
    )
    run_parser.add_argument(
        "config",
        help="Path to configuration file (JSON or YAML)"
    )
    run_parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    # 'validate' subcommand
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate configuration without running pipeline"
    )
    validate_parser.add_argument(
        "config",
        help="Path to configuration file (JSON or YAML)"
    )

    args = parser.parse_args()

    if args.command == "run":
        try:
            result = main(config_path=args.config)
            sys.exit(0 if result.get("status") == "success" else 1)
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            sys.exit(1)

    elif args.command == "validate":
        try:
            validate_config(args.config)
            sys.exit(0)
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            sys.exit(1)

    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    cli()
