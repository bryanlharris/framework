from framework.standardTransformations import (
    addRowShaChecksum,
    addSourceMetadata,
    addTimestampColumn,
    cast_data_types,
    rename_columns,
)

transform_registry = {
    "addRowShaChecksum": addRowShaChecksum,
    "framework.standardTransformations.addRowShaChecksum": addRowShaChecksum,
    "addSourceMetadata": addSourceMetadata,
    "framework.standardTransformations.addSourceMetadata": addSourceMetadata,
    "addTimestampColumn": addTimestampColumn,
    "framework.standardTransformations.addTimestampColumn": addTimestampColumn,
    "cast_data_types": cast_data_types,
    "framework.standardTransformations.cast_data_types": cast_data_types,
    "rename_columns": rename_columns,
    "framework.standardTransformations.rename_columns": rename_columns,
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
