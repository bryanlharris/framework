from framework.transforms.functions import (
    addRowShaChecksum,
    addSourceMetadata,
    addTimestampColumn,
    cast_data_types,
    rename_columns,
)

transform_registry = {
    "addRowShaChecksum": addRowShaChecksum,
    "framework.transforms.functions.addRowShaChecksum": addRowShaChecksum,
    "addSourceMetadata": addSourceMetadata,
    "framework.transforms.functions.addSourceMetadata": addSourceMetadata,
    "addTimestampColumn": addTimestampColumn,
    "framework.transforms.functions.addTimestampColumn": addTimestampColumn,
    "cast_data_types": cast_data_types,
    "framework.transforms.functions.cast_data_types": cast_data_types,
    "rename_columns": rename_columns,
    "framework.transforms.functions.rename_columns": rename_columns,
}

# Stable public import path: import transform_registry from framework.transforms.
__all__ = [
    "transform_registry",
    "addRowShaChecksum",
    "addSourceMetadata",
    "addTimestampColumn",
    "cast_data_types",
    "rename_columns",
]
