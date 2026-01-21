"""metabricks.

MetaBricks - Metadata Ingestion Framework for Databricks.

Production-ready framework for ingesting and transforming metadata from various
sources (REST APIs, Kafka, JDBC, FTP, ADLS) into Databricks (Unity Catalog, Volumes, Streaming).

Public API for clients using this framework in Databricks.
"""

from metabricks.orchestrator import MetadataOrchestrator
from metabricks.cli import main, validate_config

__version__ = "0.1.0"

__all__ = [
    "MetadataOrchestrator",
    "main",
    "validate_config",
]
