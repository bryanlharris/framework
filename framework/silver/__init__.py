from framework.silver.ingest import from_table

silver_functions = {
    "from_table": from_table,
    "framework.silver.functions.from_table": from_table,
}

__all__ = ["silver_functions", "from_table"]
