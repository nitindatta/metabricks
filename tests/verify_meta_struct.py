from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from metabricks.sinks.databricks.batch import DatabricksSink
from metabricks.sinks.types import DatabricksSinkRuntimeConfig
from metabricks.systems.databricks.types import DatabricksCatalogTableTarget, DatabricksConnection
from metabricks.core.contracts import ExecutionContext

def verify_meta_struct():
    spark = SparkSession.builder \
        .appName("VerifyMetaStruct") \
        .master("local[*]") \
        .getOrCreate()
    
    print("Spark session created")

    # Create dummy config
    config = DatabricksSinkRuntimeConfig(
        mode="batch",
        format="delta",
        connection=DatabricksConnection(host="test", token="test", catalog="cat", schema_name="sch"),
        target=DatabricksCatalogTableTarget(object_name="tab")
    )
    
    sink = DatabricksSink(config, spark=spark)
    
    # Create dummy DF
    data = [("A", 1), ("B", 2)]
    schema = StructType([
        StructField("col1", StringType()),
        StructField("col2", IntegerType())
    ])
    df = spark.createDataFrame(data, schema)
    
    # Create context
    context = ExecutionContext(
        run_id="run-123",
        batch_id="batch-456",
        pipeline_name="my-pipe",
        pipeline_version="v1",
        is_replay=True,
        record_hash_keys=["col1", "col2"],
        attach_audit_meta=True
    )
    
    print("Calling _attach_meta_struct (with hash keys)...")
    try:
        new_df = sink._attach_meta_struct(df, context)
        new_df.printSchema()
        new_df.show(truncate=False)
        
        # Verify new schema structure: top-level columns + nested _meta
        assert "_ingest_ts" in new_df.columns, "Missing top-level _ingest_ts"
        assert "run_id" in new_df.columns, "Missing top-level run_id"
        assert "_meta" in new_df.columns, "Missing _meta struct"
        
        # Verify _meta struct fields (no longer contains ingest_ts or run_id)
        meta_fields = [f.name for f in new_df.schema["_meta"].dataType.fields]
        assert "batch_id" in meta_fields
        assert "pipeline_name" in meta_fields
        assert "record_hash" in meta_fields
        assert "ingest_ts" not in meta_fields, "_meta should NOT contain ingest_ts (moved to top-level)"
        assert "run_id" not in meta_fields, "_meta should NOT contain run_id (run_id is top-level)"
        
        print("SUCCESS: _attach_meta_struct (with keys) executed without error.")
        print(f"  Top-level columns: _ingest_ts={new_df.schema['_ingest_ts'].dataType}, run_id={new_df.schema['run_id'].dataType}")
        print(f"  _meta struct fields: {meta_fields}")
    except Exception as e:
        print(f"FAILED (with keys): {e}")
        raise e

    # Test without hash keys (NULL record_hash case)
    print("\nCalling _attach_meta_struct (NO hash keys)...")
    context_no_hash = ExecutionContext(
        run_id="run-123",
        batch_id="batch-456",
        pipeline_name="my-pipe",
        pipeline_version="v1",
        is_replay=True,
        record_hash_keys=[], # EMPTY
        attach_audit_meta=True
    )
    try:
        new_df_no = sink._attach_meta_struct(df, context_no_hash)
        new_df_no.printSchema()
        new_df_no.show(truncate=False)
        
        # Same validation for no-keys case
        assert "_ingest_ts" in new_df_no.columns
        assert "run_id" in new_df_no.columns
        assert "_meta" in new_df_no.columns
        
        print("SUCCESS: _attach_meta_struct (no keys) executed without error.")
    except Exception as e:
        print(f"FAILED (no keys): {e}")
        raise e
    
    finally:
        spark.stop()

if __name__ == "__main__":
    verify_meta_struct()
