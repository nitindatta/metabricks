from __future__ import annotations

import importlib
import sys
from typing import Iterable


BUILTIN_PLUGIN_MODULES: tuple[str, ...] = (
    # API
    "metabricks.connectors.api.batch",
    "metabricks.wiring.api_wiring",
    # Kafka
    "metabricks.connectors.kafka_stream",
    "metabricks.wiring.kafka_wiring",
    # Databricks
    "metabricks.connectors.databricks.batch",
    "metabricks.connectors.databricks.streaming",
    "metabricks.wiring.databricks_wiring",

    # Sink wiring
    "metabricks.wiring.databricks_sink_wiring",
    "metabricks.wiring.filesystem_sink_wiring",
    "metabricks.wiring.s3_sink_wiring",

    # Sinks
    "metabricks.sinks.databricks.batch",
    "metabricks.sinks.databricks.streaming",
    "metabricks.sinks.filesystem_sink",
    "metabricks.sinks.s3_sink",
)


_LOADED = False


def load_builtin_plugins(*, reload: bool = False, modules: Iterable[str] = BUILTIN_PLUGIN_MODULES) -> None:
    """Import built-in connector + wiring modules so decorators register them.

    In production, call with reload=False (default) so imports are cheap.
    In tests, call with reload=True after clearing registries to re-run decorators.
    """

    global _LOADED

    if _LOADED and not reload:
        return

    # In tests we often need true isolation. When `reload=True`, clear registries
    # so decorator registration can run again without duplicate-key failures.
    if reload:
        try:
            from metabricks.connectors.registry import ConnectorRegistry
            from metabricks.sinks.registry import SinkRegistry
            from metabricks.wiring.sink_registry import SinkWiringRegistry
            from metabricks.wiring.source_registry import SourceWiringRegistry

            ConnectorRegistry.clear()
            SourceWiringRegistry.clear()
            SinkRegistry.clear()
            SinkWiringRegistry.clear()
        except Exception:
            # Keep bootstrap resilient: clearing registries is best-effort.
            pass

    for module_name in modules:
        if reload:
            sys.modules.pop(module_name, None)
        importlib.import_module(module_name)

    _LOADED = True
