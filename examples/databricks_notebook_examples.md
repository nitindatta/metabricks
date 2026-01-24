# Example: Using MetaBricks from Databricks Notebook

These examples show how clients will use the metabricks framework in Databricks.

## Installation

```python
# Install in Databricks notebook (one-time per cluster)
%pip install /Volumes/main/shared/metabricks-0.1.0-py3-none-any.whl
```

## Example 1: REST API → Unity Catalog

```python
# Databricks Notebook Cell 1: Import and Configure

from metabricks import MetadataOrchestrator
import json

# Define pipeline configuration
config = {
    "name": "rest_api_to_uc",
    "source": {
        "type": "rest",
        "endpoint": "https://api.oecd.org/v1/metadata",
        "auth": {
            "type": "bearer",
            "token_path": "/Volumes/main/shared/secrets/oecd_token.txt"
        },
        "pagination": {
            "type": "offset",
            "limit": 1000,
            "offset_param": "offset"
        }
    },
    "sink": {
        "type": "databricks_uc",
        "catalog": "main",
        "schema": "oecd_data",
        "table": "metadata_raw",
        "write_mode": "overwrite"
    },
    "batch_size": 5000,
    "max_retries": 3
}

# Create orchestrator
orchestrator = MetadataOrchestrator(config)
```

```python
# Databricks Notebook Cell 2: Run Pipeline

# Execute pipeline
result = orchestrator.run()

# Result is a DataEnvelope object
print(f"✅ Payload type: {result.payload_type}")
print(f"🧭 Run ID: {result.context.run_id if result.context else 'N/A'}")
print(f"📛 Pipeline: {result.context.pipeline_name if result.context else 'N/A'}")

# Optional: warnings are surfaced via context.metadata (if any component sets them)
warnings = (result.context.metadata or {}).get('warnings') if result.context else None
if warnings:
    print(f"⚠️  Warnings: {warnings}")

# Inspect data by payload type
if result.payload_type == 'json':
    # Pretty-print JSON payload
    import json
    print(json.dumps(result.data, indent=2)[:2000])
elif result.payload_type == 'dataframe':
    # Show Spark DataFrame schema and sample
    df = result.data
    df.printSchema()
    display(df.limit(10))
```

---

## Example 2: Kafka Streaming → Delta Volume

```python
from metabricks import MetadataOrchestrator

config = {
    "name": "kafka_streaming_to_delta",
    "source": {
        "type": "kafka",
        "brokers": ["kafka-broker-1:9092", "kafka-broker-2:9092"],
        "topic": "oecd.metadata.events",
        "group_id": "oecd-ingestion-group",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "{{secrets/kafka_user}}",
        "sasl_password": "{{secrets/kafka_password}}"
    },
    "sink": {
        "type": "databricks_volume",
        "volume_path": "/Volumes/main/shared/oecd_streaming",
        "format": "delta",
        "mode": "append",
        "checkpoint_path": "/Volumes/main/shared/checkpoints/kafka_ingestion"
    },
    "streaming": {
        "enabled": True,
        "window_duration": "10 minutes",
        "checkpoint_interval": "30 seconds"
    }
}

orchestrator = MetadataOrchestrator(config)
result = orchestrator.run()
```

---

## Example 3: JDBC (PostgreSQL) → UC with Data Quality

```python
from metabricks import MetadataOrchestrator

config = {
    "name": "jdbc_postgres_to_uc_validated",
    "source": {
        "type": "jdbc",
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://prod-db.internal:5432/oecd_source",
        "user": "{{secrets/db_user}}",
        "password": "{{secrets/db_password}}",
        "dbtable": "(SELECT * FROM metadata WHERE updated_at > CURRENT_DATE - 7) t",
        "partitionColumn": "id",
        "lowerBound": 1,
        "upperBound": 1000000,
        "numPartitions": 16
    },
    "sink": {
        "type": "databricks_uc",
        "catalog": "main",
        "schema": "oecd_staging",
        "table": "metadata_staged",
        "mode": "merge",
        "merge_key": "id"
    },
    "data_quality": {
        "enabled": True,
        "rules": [
            {
                "name": "id_not_null",
                "column": "id",
                "type": "not_null",
                "error_action": "fail"
            },
            {
                "name": "name_length",
                "column": "name",
                "type": "max_length",
                "value": 255,
                "error_action": "warn"
            },
            {
                "name": "valid_country_code",
                "column": "country_code",
                "type": "regex",
                "pattern": "^[A-Z]{2}$",
                "error_action": "fail"
            },
            {
                "name": "date_range",
                "column": "created_at",
                "type": "between",
                "min": "2020-01-01",
                "max": "{{current_date}}",
                "error_action": "warn"
            }
        ]
    },
    "error_handling": {
        "bad_records_path": "/Volumes/main/shared/errors/bad_records",
        "mode": "permissive"
    }
}

orchestrator = MetadataOrchestrator(config)
result = orchestrator.run()

# Check data quality results
if result['status'] == 'success':
    print(f"Records loaded: {result['stats']['total_records']}")
    print(f"Data quality checks passed")
else:
    print(f"{len(result['data_quality_issues'])} quality issues found")
    for issue in result['data_quality_issues'][:5]:
        print(f"   - {issue}")
```

---

## Example 4: FTP → ADLS → UC Pipeline

```python
from metabricks import MetadataOrchestrator

config = {
    "name": "ftp_to_adls_to_uc",
    "source": {
        "type": "ftp",
        "host": "ftp.oecd.org",
        "port": 21,
        "username": "{{secrets/ftp_user}}",
        "password": "{{secrets/ftp_password}}",
        "remote_path": "/data/metadata/*.csv",
        "file_pattern": "metadata_*.csv",
        "encoding": "utf-8"
    },
    "transformations": [
        {
            "type": "parse_csv",
            "delimiter": ",",
            "header": True
        },
        {
            "type": "cast_columns",
            "mappings": {
                "created_at": "timestamp",
                "updated_at": "timestamp",
                "version": "integer"
            }
        },
        {
            "type": "deduplicate",
            "on": ["id"]
        }
    ],
    "sink": {
        "type": "databricks_uc",
        "catalog": "main",
        "schema": "oecd_data",
        "table": "metadata_processed",
        "mode": "append"
    }
}

orchestrator = MetadataOrchestrator(config)
result = orchestrator.run()
```

---

## Example 5: Load Config from File

```python
from metabricks import main
import json

# Load configuration from volume
config_path = "/Volumes/main/shared/configs/daily_ingest.json"
with open(config_path) as f:
    config = json.load(f)

# Run via main() entry point
result = main(config_dict=config)
```

---

## Example 6: Scheduled Job in Databricks

```python
# File: /Volumes/main/shared/jobs/daily_metadata_ingest.py
# This runs as a scheduled Databricks Job

import json
from metabricks import MetadataOrchestrator

def run_daily_ingest():
    """Daily OECD metadata ingestion job."""
    
    config = {
        "name": "daily_oecd_ingest",
        "source": {
            "type": "rest",
            "endpoint": "https://api.oecd.org/v1/metadata",
            "auth": {"type": "bearer", "token": "{{secrets/api_key}}"}
        },
        "sink": {
            "type": "databricks_uc",
            "catalog": "main",
            "schema": "oecd_data",
            "table": "metadata"
        }
    }
    
    try:
        orchestrator = MetadataOrchestrator(config)
        result = orchestrator.run()
        
        # Log results
        print(f"Job completed: {result['status']}")
        print(f"   Records: {result['stats']['total_records']}")
        
        return result
        
    except Exception as e:
        print(f"Job failed: {e}")
        raise

if __name__ == "__main__":
    run_daily_ingest()
```

Then in Databricks Job config:

```json
{
  "name": "Daily OECD Metadata Ingest",
  "schedule": {
    "quartz_cron_expression": "0 2 * * ? *",
    "timezone_id": "UTC"
  },
  "libraries": [
    {
      "whl": "dbfs:/Volumes/main/shared/metabricks-0.1.0-py3-none-any.whl"
    }
  ],
  "spark_python_task": {
    "python_file": "dbfs:/Volumes/main/shared/jobs/daily_metadata_ingest.py"
  },
  "timeout_seconds": 3600,
  "max_concurrent_runs": 1
}
```

---

## Example 7: Error Handling

```python
from metabricks import MetadataOrchestrator

config = {
    "name": "robust_pipeline",
    "source": {"type": "rest", "endpoint": "https://api.example.com"},
    "sink": {"type": "databricks_uc", "catalog": "main", "schema": "data", "table": "raw"},
    "error_handling": {
        "on_source_error": "retry",  # retry, skip, fail
        "on_transform_error": "skip",
        "on_sink_error": "fail",
        "max_retries": 3,
        "retry_delay_seconds": 5,
        "bad_records_path": "/Volumes/main/shared/errors"
    }
}

orchestrator = MetadataOrchestrator(config)

try:
    result = orchestrator.run()
except Exception as e:
    print(f"Pipeline failed: {e}")
    # Check error logs in /Volumes/main/shared/errors/
```

---

## Common Patterns

### Pattern 1: Load Configuration from Widgets

```python
# Databricks widgets for parameterization
dbutils.widgets.text("config_path", "/Volumes/main/shared/configs/default.json")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "oecd_data")

config_path = dbutils.widgets.get("config_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Load and override
import json
with open(config_path) as f:
    config = json.load(f)
    
config["sink"]["catalog"] = catalog
config["sink"]["schema"] = schema

from metabricks import MetadataOrchestrator
result = MetadataOrchestrator(config).run()
```

### Pattern 2: Multi-Source Pipeline

```python
from metabricks import MetadataOrchestrator

# Run multiple sequential pipelines
sources = ["rest_api", "kafka_stream", "jdbc_database"]
results = {}

for source_name in sources:
    config = load_config(f"/Volumes/main/shared/configs/{source_name}.json")
    orchestrator = MetadataOrchestrator(config)
    results[source_name] = orchestrator.run()
    print(f"{source_name}: {results[source_name]['status']}")

# Summary
total_records = sum(r['stats']['total_records'] for r in results.values())
print(f"Total records ingested: {total_records}")
```

### Pattern 3: Conditional Logic

```python
from metabricks import MetadataOrchestrator
from datetime import datetime, timedelta

config = {
    "name": "conditional_ingest",
    "source": {
        "type": "rest",
        "endpoint": "https://api.example.com",
        "filter": {
            "field": "updated_at",
            "operator": "gte",
            "value": (datetime.now() - timedelta(days=1)).isoformat()
        }
    },
    "sink": {"type": "databricks_uc", "catalog": "main", "schema": "data", "table": "incremental"}
}

orchestrator = MetadataOrchestrator(config)
result = orchestrator.run()

if result['stats']['total_records'] > 0:
    print("New data ingested")
else:
    print("ℹNo new data available")
```
