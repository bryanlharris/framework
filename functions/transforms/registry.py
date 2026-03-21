from functions.standardTransformations import (
    addRowShaChecksum,
    addSourceMetadata,
    addTimestampColumn,
    cast_data_types,
    rename_columns,
)

TRANSFORM_REGISTRY = {
    "addRowShaChecksum": addRowShaChecksum,
    "addSourceMetadata": addSourceMetadata,
    "addTimestampColumn": addTimestampColumn,
    "cast_data_types": cast_data_types,
    "rename_columns": rename_columns,
}
