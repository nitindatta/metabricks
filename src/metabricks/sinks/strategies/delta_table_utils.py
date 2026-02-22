"""Utilities for managing Delta table properties via SQL DDL.

Handles:
- Creating tables with properties
- Updating properties on existing tables
- Validating property compatibility
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from metabricks.core.logger import get_logger
from metabricks.models.delta_table_properties import DeltaTableProperties


def _build_tblproperties_clause(props: Dict[str, str]) -> str:
    """Build SQL TBLPROPERTIES clause from dict.
    
    Escapes single quotes in values.
    
    Args:
        props: Dict of property name -> value
        
    Returns:
        SQL clause like: TBLPROPERTIES ('key1' = 'value1', 'key2' = 'value2')
    """
    if not props:
        return ""
    
    pairs = []
    for key, value in props.items():
        # Escape single quotes in value
        escaped_value = str(value).replace("'", "''")
        pairs.append(f"'{key}' = '{escaped_value}'")
    
    return f"TBLPROPERTIES ({', '.join(pairs)})"


def ensure_table_exists_with_properties(
    spark: Any,
    table_name: str,
    schema: Any,
    table_properties: Optional[DeltaTableProperties] = None,
) -> None:
    """Create Delta table if not exists with initial properties.
    
    Args:
        spark: SparkSession
        table_name: Fully qualified table name (catalog.schema.table)
        schema: StructType schema
        table_properties: DeltaTableProperties or None
        
    Raises:
        ValueError: If table creation fails
    """
    log = get_logger("ensure_table_exists_with_properties")
    
    if spark.catalog.tableExists(table_name):
        log.info(f"Table {table_name} already exists, skipping creation")
        # Apply properties to existing table if provided
        if table_properties:
            apply_table_properties(spark, table_name, table_properties)
        return
    
    # Build CREATE TABLE statement
    schema_str = schema.simpleString() if hasattr(schema, 'simpleString') else str(schema)
    
    tblprops = table_properties.to_sql_properties() if table_properties else {}
    tblprops_clause = _build_tblproperties_clause(tblprops)
    
    create_sql = f"""
        CREATE TABLE {table_name}
        USING DELTA
        AS SELECT * FROM (SELECT 1 as dummy) LIMIT 0
    """
    
    if tblprops_clause:
        create_sql = f"""
            CREATE TABLE {table_name}
            USING DELTA
            {tblprops_clause}
            AS SELECT * FROM (SELECT 1 as dummy) LIMIT 0
        """
    
    try:
        spark.sql(create_sql)
        log.info(f"Created table {table_name} with properties")
    except Exception as e:
        raise ValueError(f"Failed to create table {table_name}: {e}")


def apply_table_properties(
    spark: Any,
    table_name: str,
    table_properties: DeltaTableProperties,
) -> None:
    """Apply or update properties on existing table.
    
    Args:
        spark: SparkSession
        table_name: Fully qualified table name (catalog.schema.table)
        table_properties: DeltaTableProperties to apply
        
    Raises:
        ValueError: If table doesn't exist or property update fails
    """
    log = get_logger("apply_table_properties")
    
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Table {table_name} does not exist")
    
    props = table_properties.to_sql_properties()
    if not props:
        log.info(f"No properties to apply to {table_name}")
        return
    
    for key, value in props.items():
        try:
            # Escape single quotes in value
            escaped_value = str(value).replace("'", "''")
            sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{key}' = '{escaped_value}')"
            spark.sql(sql)
            log.debug(f"Applied property {key}={value} to {table_name}")
        except Exception as e:
            log.error(f"Failed to set property {key} on {table_name}: {e}")
            raise ValueError(f"Failed to apply table property {key}: {e}")
    
    log.info(f"Applied {len(props)} properties to {table_name}")


def get_table_properties(spark: Any, table_name: str) -> Dict[str, str]:
    """Retrieve current TBLPROPERTIES from table.
    
    Args:
        spark: SparkSession
        table_name: Fully qualified table name
        
    Returns:
        Dict of all table properties
    """
    log = get_logger("get_table_properties")
    
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Table {table_name} does not exist")
    
    try:
        describe_result = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").collect()
        
        # Properties are in rows with col_name starting with "Table Properties:"
        properties = {}
        in_props_section = False
        
        for row in describe_result:
            col_name = row[0] if row[0] else ""
            col_val = row[1] if len(row) > 1 else ""
            
            if "Table Properties:" in col_name:
                in_props_section = True
                continue
            
            if in_props_section:
                if col_name.strip() and not col_name.startswith(" "):
                    break  # End of properties section
                
                if "=" in col_val:
                    key, val = col_val.split("=", 1)
                    properties[key.strip()] = val.strip()
        
        return properties
    except Exception as e:
        log.warning(f"Could not retrieve properties for {table_name}: {e}")
        return {}


def apply_check_constraints(
    spark: Any,
    table_name: str,
    constraints: Dict[str, str],
) -> None:
    """Apply CHECK constraints to table.
    
    Args:
        spark: SparkSession
        table_name: Fully qualified table name
        constraints: Dict of constraint_name -> expression
        
    Raises:
        ValueError: If constraint application fails
    """
    log = get_logger("apply_check_constraints")
    
    for name, expression in constraints.items():
        try:
            sql = f"ALTER TABLE {table_name} ADD CONSTRAINT {name} CHECK ({expression})"
            spark.sql(sql)
            log.info(f"Applied constraint {name} to {table_name}")
        except Exception as e:
            # Constraint might already exist; log but don't fail
            log.warning(f"Could not apply constraint {name}: {e}")


def apply_z_order_optimization(
    spark: Any,
    table_name: str,
    columns: list[str],
) -> None:
    """Apply Z-order clustering for data skipping.
    
    Args:
        spark: SparkSession
        table_name: Fully qualified table name
        columns: Columns to Z-order by
        
    Note:
        This runs OPTIMIZE which can be expensive. Should be scheduled,
        not run on every write.
    """
    log = get_logger("apply_z_order_optimization")
    
    if not columns:
        return
    
    col_list = ", ".join(columns)
    try:
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({col_list})")
        log.info(f"Applied Z-order optimization to {table_name} on columns: {col_list}")
    except Exception as e:
        log.warning(f"Could not apply Z-order optimization: {e}")
