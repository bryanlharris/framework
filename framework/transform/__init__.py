from framework.transform.metadata import (
    add_row_sha_checksum,
    add_source_metadata,
    add_timestamp_column,
)
from framework.transform.columns import (
    cast_data_types,
    rename_columns,
)

transform_registry = {
    "add_row_sha_checksum": add_row_sha_checksum,
    "add_source_metadata": add_source_metadata,
    "add_timestamp_column": add_timestamp_column,
    "cast_data_types": cast_data_types,
    "rename_columns": rename_columns,
    "framework.transform.functions.add_row_sha_checksum": add_row_sha_checksum,
    "framework.transform.functions.add_source_metadata": add_source_metadata,
    "framework.transform.functions.add_timestamp_column": add_timestamp_column,
    "framework.transform.functions.cast_data_types": cast_data_types,
    "framework.transform.functions.rename_columns": rename_columns,
}

# Stable public import path: import transform_registry from framework.transform.
__all__ = [
    "transform_registry",
    "add_row_sha_checksum",
    "add_source_metadata",
    "add_timestamp_column",
    "cast_data_types",
    "rename_columns",
]
