# Metadata-driven ingestion framework

This repository contains a metadata-driven ingestion framework (the `metabricks` package) designed to run data extraction and loading pipelines driven entirely by manifests. The design targets Databricks/Spark execution but is connector-agnostic so it can be extended to JDBC, APIs, file systems, cloud stores, or streaming sources.

Key ideas
- Metadata-first: pipelines are defined by manifest objects (Pydantic models in `src/metabricks/models/config_model.py`) that describe source connection, source object (query, endpoint or file), sink connection and sink object settings, and behaviour (snapshot/incremental/stream).
- Pluggable connectors and sinks: implement `BaseConnector.extract()` to return a `DataEnvelope` and `BaseSink.write()` to consume it. Connectors and sinks are registered via `connectors/config.py`.
- Metadata providers: abstract `MetadataProvider` implementation that can load manifests from files, Delta tables, or other catalogs. The repo includes a `FileMetadataProvider` and `DeltaMetadataProvider`.

Repository layout
- `src/metabricks/models/config_model.py`: Pydantic schema for pipeline manifests (`GenericPipelineCfg`) and connection/object settings.
- `src/metabricks/core/base_connector.py`: `BaseConnector` abstract class and lifecycle hooks for extraction.
- `src/metabricks/core/base_sink.py`: `BaseSink` abstract class for writing data.
- `src/metabricks/core/contracts.py`: `DataEnvelope` contract used to pass extracted payloads (dataframe/json/stream/etc.).
- `src/metabricks/core/metadata_provider.py`: metadata provider interface.
- `src/metabricks/connectors/`: connector implementations (e.g., `databricks_connector.py`, `jdbc_connector.py`).
- `src/metabricks/sinks/`: sink implementations (e.g., `databricks_sink.py`).
- `src/metabricks/metadata_providers/providers.py`: example metadata providers (`FileMetadataProvider`, `DeltaMetadataProvider`).

How it works (high level)
1. Load a pipeline manifest using a `MetadataProvider` (e.g. `FileMetadataProvider`).
2. Instantiate the source connector according to `source_connection_settings.system_type` (lookups in `connectors/config.py`).
3. Call `connector.extract()` -> returns a `DataEnvelope` containing the payload (usually a Spark DataFrame).
4. Instantiate a sink based on `sink_connection_settings.system_type` and `sink_object_settings.file_format`.
5. Call `sink.write(env)` to persist the data.

Minimal example manifest (JSON)

# Metadata-driven ingestion framework

This repository contains a metadata-driven ingestion framework (the `metabricks` package) designed to run extraction + load pipelines driven entirely by manifests.

The framework targets Databricks/Spark execution but is connector-agnostic and can be extended to APIs, JDBC, filesystems, and streaming sources.

Key ideas
- Forward-only manifests: configs are validated with Pydantic models (`PipelineConfig` + discriminated unions for `SourceConfig` and `SinkConfig`).
- Pluggable connectors/sinks: implementations are registered via registries keyed by `(system_type, extraction_mode)` for sources and `(system_type, mode)` for sinks.
- Wiring layer: separates config parsing from runtime construction (builds connector/sink args from the validated models).
- Metadata providers: `MetadataProvider` implementations can load raw manifests from files, Delta tables, or other catalogs.

Repository layout (current)
- `src/metabricks/models/pipeline_config.py`: top-level `PipelineConfig`.
- `src/metabricks/models/source_config.py`: `SourceConfig` discriminated union.
- `src/metabricks/models/sink_config.py`: `SinkConfig` discriminated union.
- `src/metabricks/models/connection_config.py`: connection settings (also discriminated unions).
- `src/metabricks/connectors/registry.py`: `ConnectorRegistry` + `@register_connector`.
- `src/metabricks/sinks/registry.py`: `SinkRegistry` + `@register_sink`.
- `src/metabricks/orchestrator.py`: `MetadataOrchestrator` (public execution entry point).
- `src/metabricks/core/contracts.py`: `DataEnvelope` contract passed from connectors to sinks.
- `src/metabricks/metadata_providers/providers.py`: example metadata providers.

How it works (high level)
1. Load a pipeline manifest as a `dict` using a `MetadataProvider` (e.g. from JSON files).
2. Validate it with `PipelineConfig` (schema + cross-field constraints).
3. Load built-in plugins (register connectors/sinks/wiring).
4. Use registries + wiring to construct the right connector/sink implementations.
5. Run: connector returns a `DataEnvelope` (dataframe/json/stream); sink consumes it.

Minimal example manifest (JSON)

```json
{
	"pipeline_name": "example_users_ingest",
	"job_name": "users_snapshot",
	"source_dataset_id": "users_v1",
	"environment": "dev",
	"source": {
		"system_type": "databricks",
		"extraction_mode": "batch",
		"format": "delta",
		"connection": {"system_type": "databricks"},
		"source_query": "SELECT * FROM raw.users_snapshot WHERE ds = :logical_date",
		"logical_date": "2025-01-01"
	},
	"sink": {
		"system_type": "databricks",
		"location_type": "catalog_table",
		"mode": "batch",
		"format": "delta",
		"connection": {
			"system_type": "databricks",
			"catalog": "lakehouse",
			"schema_name": "clean"
		},
		"object_name": "users",
		"write_mode": "overwrite",
		"overwrite_scope": [{"year": "{{year}}", "month": "{{month}}"}],
		"partition_by": ["year", "month"]
	}
}
```

Quick run example (Python)

```python
from metabricks.cli import main

result = main(
	config_dict=manifest_dict,
	runtime_vars={
		"year": "2025",
		"month": "01",
	},
)
print(result["status"], result["payload_type"])
```

Extending the framework
- Add a connector: implement a connector class and register it with `@register_connector(system_type=..., extraction_mode=...)`.
- Add a sink: implement a sink class and register it with `@register_sink(system_type=..., mode=...)`.
- Add wiring: register source/sink wiring to translate validated config models into runtime constructor args.

Notes
- `load_builtin_plugins()` registers the built-in connectors/sinks/wiring used by the orchestrator.
- `DataEnvelope` (in `core/contracts.py`) standardises payload type and optional hints for sinks.

Files to inspect for implementation details
- `src/metabricks/orchestrator.py`
- `src/metabricks/cli.py`
- `src/metabricks/models/pipeline_config.py`
- `src/metabricks/models/source_config.py`
- `src/metabricks/models/sink_config.py`
- `src/metabricks/connectors/registry.py`
- `src/metabricks/sinks/registry.py`
			"schema_name": "clean"
		},
		"object_name": "users",
		"write_mode": "overwrite",
		"overwrite_scope": [{"year": "{{year}}", "month": "{{month}}"}],
		"partition_by": ["year", "month"]
	}
}
```

## Minimal example config (Databricks streaming: Auto Loader JSON -> Delta)

```json
{
	"pipeline_name": "example_users_stream",
	"environment": "dev",
	"source": {
		"system_type": "databricks",
		"extraction_mode": "streaming",
		"format": "json",
		"connection": {
			"system_type": "databricks",
			"catalog": "hive_metastore",
			"schema_name": "raw"
		},
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
			"system_type": "databricks",
			"catalog": "main",
			"schema_name": "streaming"
		},
		"object_name": "users_stream",
		"write_mode": "append",
		"checkpoint_path": "/Volumes/main/raw/checkpoints/users_stream/"
	}
}
```

## Run example

```python
from metabricks import MetadataOrchestrator

orchestrator = MetadataOrchestrator()
env = orchestrator.run(config_dict)

print(env.payload_type)
print(env.hints.get("sink_audit"))
```

## Extending the framework

- Add a connector: implement an `extract()` method returning `DataEnvelope`, then register via `@register_connector(system_type=..., extraction_mode=...)`.
- Add a sink: implement `write(env: DataEnvelope)`, then register via `@register_sink(system_type=..., mode=...)`.
- Add wiring: register a builder function in the wiring registries to construct connector/sink args from config + secrets.

## Notes

- Invalid manifests fail fast via Pydantic validation (`PipelineConfig.model_validate`).
- `DataEnvelope` carries execution hints (`run_id`, `pipeline_name`, `sink_audit`, etc.).
- Spark/Delta are expected to be available in Databricks (or installed via the `dev` extras for local tests).



