from framework.transforms.metadata import addRowShaChecksum, addSourceMetadata, addTimestampColumn
from framework.transforms.schema import cast_data_types, rename_columns

transform_registry = {
    "addRowShaChecksum": addRowShaChecksum,
    "framework.transforms.metadata.addRowShaChecksum": addRowShaChecksum,
    "framework.transforms.functions.addRowShaChecksum": addRowShaChecksum,  # backwards-compat alias
    "addSourceMetadata": addSourceMetadata,
    "framework.transforms.metadata.addSourceMetadata": addSourceMetadata,
    "framework.transforms.functions.addSourceMetadata": addSourceMetadata,  # backwards-compat alias
    "addTimestampColumn": addTimestampColumn,
    "framework.transforms.metadata.addTimestampColumn": addTimestampColumn,
    "framework.transforms.functions.addTimestampColumn": addTimestampColumn,  # backwards-compat alias
    "cast_data_types": cast_data_types,
    "framework.transforms.schema.cast_data_types": cast_data_types,
    "framework.transforms.functions.cast_data_types": cast_data_types,  # backwards-compat alias
    "rename_columns": rename_columns,
    "framework.transforms.schema.rename_columns": rename_columns,
    "framework.transforms.functions.rename_columns": rename_columns,  # backwards-compat alias
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
