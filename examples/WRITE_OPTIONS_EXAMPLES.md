# Write Options Example Configurations

This directory contains example JSON configurations demonstrating how to use the new write options system in the MetaBricks metadata-driven ingestion framework.

## Quick Start

Each JSON file represents a complete sink configuration that can be passed to the orchestrator. The `delta_write_options` field controls write-time behavior independently from table properties.

## Example Files

### Batch Operations

- **[pipeline_batch_overwrite.json](pipeline_batch_overwrite.json)** - Full table replacement
  - Use: Complete refresh cycles
 Each JSON file represents a complete pipeline configuration that can be passed to the orchestrator. The `delta_write_options` field controls write-time behavior independently from table properties.

- **[pipeline_cdc_append.json](pipeline_cdc_append.json)** - Change Data Capture
  - Use: Incremental append with schema evolution

- **[pipeline_partition_replace.json](pipeline_partition_replace.json)** - Incremental partition updates
  - Use: Daily/hourly loads with partition replacement

- **[pipeline_volume_append.json](pipeline_volume_append.json)** - Long-term archival
  - Use: Data lake storage with Delta format
 For batch selective overwrites (Delta `replaceWhere`), use `overwrite_scope` (preferred) or the advanced `replace_where` override on the sink.

### Streaming Operations

- **[pipeline_streaming_kafka.json](pipeline_streaming_kafka.json)** - Real-time Kafka ingestion
  - Use: Event streams from Kafka
  - Trigger: Automatic micro-batches (availableNow)

    // Note: replaceWhere is not configured here for batch.
    "overwrite_scope": [{"k": "v"}],      // (Optional) Preferred selective overwrite scoping (builds replaceWhere)
    "replace_where": "predicate",          // (Optional) Advanced override for replaceWhere
  - Use: Dashboard/analytics with fixed intervals
  - Trigger: ProcessingTime (5 minutes in example)

 - **Write Options** (in these configs): Transient, applied during write operations
   - `mode`, `merge_schema`, `output_mode`, `trigger_mode`, etc.
   - Controlled via `delta_write_options` field

 - **Selective overwrite scope** (batch only): Controls which partitions/rows are replaced during overwrite
   - Preferred: `overwrite_scope` (structured equality predicates)
   - Advanced: `replace_where` (raw Delta predicate)

## Configuration Structure

Every configuration follows this structure:


   If you need a complex predicate (functions, ranges, casts), use the advanced `replace_where` sink field.
```json
{
  "location_type": "catalog_table|volume|external_location",
  "object_name": "table_name",           // For catalog_table
  "path": "s3://...",                    // For external_location
  "connection": {                         // Only for catalog_table
    "catalog": "main",
    "schema_name": "schema"
  },
  "write_mode": "overwrite|append",      // (Optional) Batch mode default
  "checkpoint_path": "/path/to/cp",      // Required for streaming
  "delta_write_options": {
    // Batch options:
    "mode": "overwrite|append",
    "merge_schema": true|false,
    "replace_where": "predicate",
    
    // OR Streaming options:
    "output_mode": "append|complete|update",
    "merge_schema": true|false,
    "checkpoint_location": "/path/to/cp",
    "trigger_mode": "availableNow|continuous|processingTime",
    "trigger_interval": "5 minutes"      // For processingTime
  }
}
```

## Using These Configurations

### Python API

```python
import json
from metabricks.models.sink_config import DatabricksSinkConfig

# Load from JSON
with open('pipeline_batch_overwrite.json') as f:
    config_dict = json.load(f)

# Create typed configuration
config = DatabricksSinkConfig(**config_dict)

# Access write options with type safety
assert config.delta_write_options.mode == "overwrite"
# Write Options Example Configurations

This directory contains example pipeline JSON configurations demonstrating how to use write-time options and overwrite scoping.

## What These Files Are

Each `pipeline_*.json` file is a forward-only `PipelineConfig` JSON (it contains `pipeline_name`, `source`, and an optional `sink`).

## Key Ideas

- `delta_write_options` controls *write-time behavior* (mode, merge schema, stream trigger settings).
- Selective overwrites (Delta `replaceWhere`) are configured on the **sink**:
  - Preferred: `overwrite_scope` (structured equality predicates)
  - Advanced: `replace_where` (raw predicate string)
- Physical partitioning is configured on the **sink** via `partition_by` (Spark `partitionBy`).

## Example Files

### Batch

- [pipeline_batch_overwrite.json](pipeline_batch_overwrite.json): overwrite whole target
- [pipeline_partition_replace.json](pipeline_partition_replace.json): selective overwrite via `overwrite_scope` + physical `partition_by`
- [pipeline_cdc_append.json](pipeline_cdc_append.json): append with schema evolution (`merge_schema=true`)
- [pipeline_volume_append.json](pipeline_volume_append.json): append to a Delta table stored in a Volume

### Streaming

- [pipeline_streaming_kafka.json](pipeline_streaming_kafka.json): Kafka -> Delta (defaults to `availableNow` trigger)
- [pipeline_scheduled_microbatch.json](pipeline_scheduled_microbatch.json): streaming pipeline with notes on `processingTime`
- [pipeline_continuous_streaming.json](pipeline_continuous_streaming.json): streaming pipeline with notes on `continuous`

## Configuration Shape (High Level)

```json
{
  "pipeline_name": "...",
  "environment": "dev",
  "source": { "system_type": "...", "extraction_mode": "...", "connection": {"system_type": "..."} },
  "sink": {
    "system_type": "databricks",
    "location_type": "catalog_table|volume|external_location",
    "mode": "batch|streaming",
    "format": "delta|parquet|csv|json",

    "write_mode": "overwrite|append",
    "overwrite_scope": [{"k": "v"}],
    "replace_where": "...",
    "partition_by": ["col1", "col2"],

    "delta_write_options": {
      "mode": "overwrite|append",
      "merge_schema": true
    }
  }
}
```

## Running

Use the orchestrator directly:

```python
import json
from metabricks.orchestrator import MetadataOrchestrator

cfg = json.load(open("pipeline_partition_replace.json"))
result = MetadataOrchestrator(run_id="example-run-1").run(cfg)
```
   - `availableNow`: Automatic, recommended for most use cases

   - `processingTime`: Fixed intervals, good for dashboards
