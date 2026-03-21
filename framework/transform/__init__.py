from framework.transform.functions import (
    addRowShaChecksum,
    addSourceMetadata,
    addTimestampColumn,
    cast_data_types,
    rename_columns,
)

transform_registry = {
    "addRowShaChecksum": addRowShaChecksum,
    "framework.transform.functions.addRowShaChecksum": addRowShaChecksum,
    "addSourceMetadata": addSourceMetadata,
    "framework.transform.functions.addSourceMetadata": addSourceMetadata,
    "addTimestampColumn": addTimestampColumn,
    "framework.transform.functions.addTimestampColumn": addTimestampColumn,
    "cast_data_types": cast_data_types,
    "framework.transform.functions.cast_data_types": cast_data_types,
    "rename_columns": rename_columns,
    "framework.transform.functions.rename_columns": rename_columns,
}

# Stable public import path: import transform_registry from framework.transform.
__all__ = [
    "transform_registry",
    "addRowShaChecksum",
    "addSourceMetadata",
    "addTimestampColumn",
    "cast_data_types",
    "rename_columns",
]
