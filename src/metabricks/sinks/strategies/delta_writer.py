from __future__ import annotations

from typing import Any, Dict, List, Optional

from metabricks.core.contracts import DataEnvelope
from metabricks.core.logger import get_logger
from metabricks.models.delta_write_options import DeltaBatchWriteOptions

from metabricks.sinks.strategies.base import WriterStrategy

def _build_replace_where_from_scope(overwrite_scope: list[dict[str, str]]) -> str:
    if not overwrite_scope:
        return ""

    keys = list(overwrite_scope[0].keys())
    if not keys:
        return ""

    # Deterministic ordering (helps tests/logging) without requiring users to care.
    ordered_keys = sorted(keys)

    def _esc(v: str) -> str:
        return v.replace("'", "''")

    predicates: list[str] = []
    for entry in overwrite_scope:
        clause = " AND ".join(
            [f"{k}='{_esc(str(entry[k]))}'" for k in ordered_keys]
        )
        predicates.append(f"({clause})")
    return " OR ".join(predicates)


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def _quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _schema_to_ddl(schema) -> str:
    columns = []
    for field in schema.fields:
        col_type = field.dataType.simpleString()
        columns.append(f"{_quote_ident(field.name)} {col_type}")
    return ", ".join(columns)


def _build_create_table_sql(
    *,
    table_name: str,
    schema,
    partition_by: List[str],
    table_properties: Dict[str, str],
) -> str:
    ddl_cols = _schema_to_ddl(schema)
    partition_sql = ""
    if partition_by:
        partition_cols = ", ".join(_quote_ident(col) for col in partition_by)
        partition_sql = f" PARTITIONED BY ({partition_cols})"
    props_sql = ""
    if table_properties:
        props = ", ".join(
            [f"'{_escape_sql(k)}'='{_escape_sql(str(v))}'" for k, v in table_properties.items()]
        )
        props_sql = f" TBLPROPERTIES ({props})"
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({ddl_cols}) USING DELTA {partition_sql}{props_sql}"


class DeltaWriterStrategy(WriterStrategy):
    """Write a batch dataframe as Delta.

    `delta-spark` is optional: if available, it is used to query write metrics.
    If not available, this strategy falls back to `df.count()`.
    """
    
    def __init__(self):
        self.log = get_logger(self.__class__.__name__)

    def supports_payload_type(self, payload_type: str) -> bool:
        return payload_type == "dataframe"

    def write(self, envelope: DataEnvelope, config: Dict[str, Any]) -> Dict[str, Any]:
        if envelope.payload_type != "dataframe":
            raise TypeError("Delta writer requires dataframe payload")

        df = envelope.data
        table_name: Optional[str] = config.get("table_name")
        path: Optional[str] = config.get("path")
        overwrite_scope: Optional[List[Dict[str, str]]] = config.get("overwrite_scope")
        replace_where: Optional[str] = config.get("replace_where")
        partition_by: List[str] = config.get("partition_by") or []
        spark = config.get("spark")
        write_options: DeltaBatchWriteOptions = config.get("write_options")
        table_properties: Dict[str, str] = config.get("table_properties") or {}
        
        # Extract from typed model
        mode = write_options.mode
        merge_schema = write_options.merge_schema
        # Note: Template resolution for overwrite_scope should have already happened
        # at orchestrator level.
        
        target = table_name or path
        self.log.info(f"Writing Delta: target={target}, mode={mode}")

        writer = df.write.format("delta").mode(mode)

        if partition_by:
            writer = writer.partitionBy(partition_by)

        # Apply write-time options
        if merge_schema:
            writer = writer.option("mergeSchema", "true")

        if overwrite_scope and mode == "overwrite" and not replace_where:
            replace_where = _build_replace_where_from_scope(overwrite_scope)
        
        if replace_where:
            self.log.info(f"Using replaceWhere optimization: {replace_where[:100]}...")
            writer = writer.option("replaceWhere", replace_where)

        if table_name and path:
            raise ValueError("Provide either table_name or path, not both")

        if table_name:
            if spark is not None and table_properties:
                try:
                    exists = bool(spark.catalog.tableExists(table_name))
                except Exception:
                    exists = False
                if not exists:
                    create_sql = _build_create_table_sql(
                        table_name=table_name,
                        schema=df.schema,
                        partition_by=partition_by,
                        table_properties=table_properties,
                    )
                    self.log.info(
                        "Creating Delta table: table=%s, properties=%s",
                        table_name,
                        table_properties,
                    )
                    spark.sql(create_sql)
            writer.saveAsTable(table_name)
            record_count = _try_delta_operation_metrics(spark, table_name) or df.count()
            self.log.info(f"Delta table write completed: record_count={record_count}, table={table_name}")
            return {
                "kind": "delta",
                "target_location": table_name,
                "mode": mode,
                "status": "success",
                "record_count": record_count,
            }

        if path:
            writer.save(path)
            rows = df.count()
            self.log.info(f"Delta path write completed: record_count={rows}, path={path}")
            return {
                "kind": "delta",
                "target_location": path,
                "mode": mode,
                "status": "success",
                "record_count": rows,
            }

        raise ValueError("Delta writer requires table_name or path")


def _try_delta_operation_metrics(spark: Any, table_name: str) -> Optional[int]:
    try:
        from delta.tables import DeltaTable  # type: ignore
    except Exception:
        return None

    if spark is None:
        return None

    try:
        metrics = (
            DeltaTable.forName(spark, table_name)
            .history(1)
            .collect()[0]
            .get("operationMetrics", {})
        )
        val = metrics.get("numOutputRows")
        return int(val) if val is not None else None
    except Exception:
        return None
