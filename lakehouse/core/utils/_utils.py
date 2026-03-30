import json
from pathlib import Path
from typing import Any


def read_json_and_decode(workspace_path: str | Path) -> Any:
    """Read JSON from a local path and decode it."""
    path = Path(workspace_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    return json.loads(path.read_text(encoding="utf-8"))


def get_table_schema(table_name, columns_to_drop=[]):
    """
    Read a table with Spark and return its schema, optionally dropping columns first.
    """
    df = spark.read.table(table_name)
    if len(columns_to_drop) > 0:
        schema = df.drop(*columns_to_drop).schema
    else:
        schema = df.schema
    return schema


def ensure_table_exists(spark, table_name, schema_string):
    """
    Create a Delta table with the given schema string if it does not already exist.
    """
    if not spark.catalog.tableExists(table_name):
        spark.sql(f"CREATE TABLE {table_name} ({schema_string}) USING DELTA")


def create_table_if_not_exists(spark, table_name, df):
    """
    Create an empty Delta table matching df's schema if it does not already exist.
    """
    if not spark.catalog.tableExists(table_name):
        spark.createDataFrame([], df.schema).write.format("delta").mode("overwrite").saveAsTable(table_name)
