# Metabricks — A Databricks Metadata Framework for Manifest-Driven Data Pipelines

**Metabricks is an open-source Databricks metadata framework** designed to build scalable, manifest-driven ingestion pipelines on Apache Spark and Delta Lake.

If you're searching for a **Databricks metadata framework** that separates configuration from execution logic and enables reusable ingestion patterns, Metabricks provides a clean, extensible architecture built specifically for modern Lakehouse environments.

---

## What is a Databricks Metadata Framework?

A Databricks metadata framework allows you to define data pipelines using structured metadata (JSON/YAML) instead of hardcoding logic in notebooks or scripts.

Metabricks enables:

- Configuration-driven ingestion pipelines
- Declarative source and sink definitions
- Batch and streaming support (Auto Loader compatible)
- Strict schema validation via Pydantic
- Extensible connector and sink registry pattern

Instead of writing custom code per dataset, you define behavior once in a manifest — and the framework handles orchestration.

---

## Why Metabricks?

Modern Databricks data platforms require:

- Standardised ingestion patterns across teams
- Dev / Test / Prod environment awareness
- Bronze → Silver ingestion layer abstraction
- Reusable Spark execution logic
- Extensibility across JDBC, APIs, files, cloud storage

Metabricks is purpose-built as a **Databricks-first metadata framework** that integrates seamlessly with:

- Delta Lake
- Unity Catalog
- Spark SQL
- Databricks Auto Loader
- Streaming and batch workloads

---

## Core Capabilities

### Metadata-Driven Pipelines

All pipelines are defined via validated `PipelineConfig` models.

Manifests describe:

- Source system type (Databricks, JDBC, API, etc.)
- Extraction mode (batch / streaming)
- Sink configuration (Delta tables, file paths, etc.)
- Partitioning strategy
- Write mode
- Runtime variables

This makes Metabricks a true **manifest-based Databricks ingestion framework**.

---

### Pluggable Connectors and Sinks

Metabricks uses a registry-based plugin system:

- `@register_connector(system_type, extraction_mode)`
- `@register_sink(system_type, mode)`

This allows extension without modifying the orchestration core.

---

### Batch + Streaming Support

Supports:

- Snapshot loads
- Incremental loads
- Databricks Auto Loader streaming ingestion
- Delta Lake writes with partition control

Designed for enterprise-scale Databricks deployments.

---

### Strict Validation & Fail-Fast Design

Invalid manifests fail immediately using Pydantic validation and discriminated unions.

This ensures safe and predictable pipeline execution in production.

---

## Architecture Overview

Metabricks follows a clean execution lifecycle:

1. Load manifest from file, Delta table, or catalog
2. Validate via `PipelineConfig`
3. Resolve connector via registry
4. Run `extract()` → returns `DataEnvelope`
5. Resolve sink via registry
6. Run `write(env)`
7. Return execution metadata

This architecture makes it a structured, extensible **Databricks metadata orchestration framework**.

---

## Repository Structure

src/metabricks/
│
├── models/ # Manifest schemas
├── connectors/ # Source implementations
├── sinks/ # Sink implementations
├── metadata_providers/ # Manifest loaders
├── core/ # Contracts & base classes
└── orchestrator.py # Execution engine


---

## Example: Databricks Batch Ingestion

```json
{
  "pipeline_name": "users_snapshot",
  "environment": "dev",
  "source": {
    "system_type": "databricks",
    "extraction_mode": "batch",
    "format": "delta",
    "source_query": "SELECT * FROM raw.users WHERE ds = :logical_date",
    "logical_date": "2025-01-01"
  },
  "sink": {
    "system_type": "databricks",
    "location_type": "catalog_table",
    "mode": "batch",
    "format": "delta",
    "connection": {
      "catalog": "main",
      "schema_name": "clean"
    },
    "object_name": "users",
    "write_mode": "overwrite",
    "partition_by": ["year", "month"]
  }
}
```
from metabricks import MetadataOrchestrator

orchestrator = MetadataOrchestrator()
result = orchestrator.run(config_dict)

print(result["status"])
```json
{
  "pipeline_name": "users_stream",
  "environment": "dev",
  "source": {
    "system_type": "databricks",
    "extraction_mode": "streaming",
    "format": "json",
    "source_path": "/Volumes/main/raw/landing/users/",
    "schema_location": "/Volumes/main/raw/checkpoints/users_schema/",
    "autoloader_options": {
      "cloudFiles.format": "json"
    }
  },
  "sink": {
    "system_type": "databricks",
    "location_type": "catalog_table",
    "mode": "streaming",
    "format": "delta",
    "connection": {
      "catalog": "main",
      "schema_name": "streaming"
    },
    "object_name": "users_stream",
    "write_mode": "append",
    "checkpoint_path": "/Volumes/main/raw/checkpoints/users_stream/"
  }
}
```

@register_connector(system_type="jdbc", extraction_mode="batch")
class JdbcConnector(BaseConnector):
    def extract(self):
        ...
@register_sink(system_type="databricks", mode="batch")
class DatabricksSink(BaseSink):
    def write(self, env):
        ...
Who Is This For?

Data engineers building enterprise Databricks platforms

Teams standardising ingestion across 100+ datasets

Lakehouse architects implementing metadata-driven ETL

Organisations needing reusable Databricks ingestion patterns

SEO Keywords

Databricks metadata framework
Metadata-driven Databricks framework
Databricks ingestion framework
Databricks manifest-based pipelines
Delta Lake ingestion framework
Spark metadata pipeline framework
Lakehouse metadata architecture

License

MIT License

Metabricks is an open-source Databricks metadata framework designed for scalable, declarative, and extensible Lakehouse ingestion.


