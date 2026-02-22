"""
Example: Using MetadataOrchestrator with runtime parameters.

This shows the separation between:
- Design-time config: Static pipeline definition (from JSON/dict)
- Runtime parameters: Provided by orchestration system at execution time
"""

from metabricks.orchestrator import MetadataOrchestrator
import json

# Load static pipeline configuration
with open("examples/pipeline_with_audit_metadata.json") as f:
    config_dict = json.load(f)

# Design-time config from JSON includes:
# - attach_audit_meta: true (enable audit columns)
# - record_hash_keys: ["id", "timestamp"] (dedup logic)


# =============================================================================
# Example 1: Regular scheduled run
# =============================================================================
orchestrator = MetadataOrchestrator(
    run_id="run_20260118_140530_abc123",
    batch_id="2026-01-18",              # ← From scheduler (daily batch)
    pipeline_version="v1.2.3-abc1234",  # ← From git SHA/deployment
    is_replay=False,                     # ← Regular run
)

result = orchestrator.run(config_dict)
print(f"Regular run completed: {result.context.run_id}")
print(f"   Batch ID: {result.context.batch_id}")
print(f"   Version: {result.context.pipeline_version}")
print(f"   Is Replay: {result.context.is_replay}")


# =============================================================================
# Example 2: Backfill/replay run
# =============================================================================
orchestrator_replay = MetadataOrchestrator(
    run_id="replay_20260118_150000_def456",
    batch_id="2026-01-15",              # ← Backfilling old date
    pipeline_version="v1.2.3-abc1234",  # ← Same version
    is_replay=True,                      # ← Flagged as replay
)

result_replay = orchestrator_replay.run(config_dict)
print(f"\nReplay run completed: {result_replay.context.run_id}")
print(f"   Batch ID: {result_replay.context.batch_id}")
print(f"   Is Replay: {result_replay.context.is_replay}")


# =============================================================================
# Example 3: Integration with Databricks Jobs
# =============================================================================
# In Databricks notebook, runtime params come from job parameters:

"""
# Databricks notebook cell:

import os
from metabricks.orchestrator import MetadataOrchestrator
from metabricks.cli import main

# Get runtime parameters from Databricks widgets/job params
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("pipeline_version", "")
dbutils.widgets.text("is_replay", "false")

batch_id = dbutils.widgets.get("batch_id")
pipeline_version = dbutils.widgets.get("pipeline_version") or os.getenv("GIT_COMMIT", "unknown")
is_replay = dbutils.widgets.get("is_replay").lower() == "true"

# Load static config
config_path = "/Workspace/Shared/configs/my_pipeline.json"

# Run with runtime parameters
orchestrator = MetadataOrchestrator(
    batch_id=batch_id,
    pipeline_version=pipeline_version,
    is_replay=is_replay,
)

result = main(config_path=config_path)
"""


# =============================================================================
# Example 4: Integration with Airflow
# =============================================================================
"""
# Airflow DAG:

from airflow import DAG
from airflow.operators.python import PythonOperator
from metabricks.orchestrator import MetadataOrchestrator
import json

def run_metabricks_pipeline(**context):
    # Runtime params from Airflow context
    execution_date = context['ds']  # YYYY-MM-DD
    run_id = context['run_id']
    
    with open("/path/to/config.json") as f:
        config = json.load(f)
    
    orchestrator = MetadataOrchestrator(
        run_id=run_id,
        batch_id=execution_date,
        pipeline_version=os.getenv("GIT_COMMIT"),
        is_replay=context.get('dag_run').conf.get('is_replay', False),
    )
    
    result = orchestrator.run(config)
    return result

with DAG('metabricks_pipeline', ...) as dag:
    task = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_metabricks_pipeline,
    )
"""
