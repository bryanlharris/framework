from functions.standardTransformations import (
    addRowShaChecksum,
    addSourceMetadata,
    addTimestampColumn,
    cast_data_types,
    rename_columns,
)

transform_registry = {
    "addRowShaChecksum": addRowShaChecksum,
    "addSourceMetadata": addSourceMetadata,
    "addTimestampColumn": addTimestampColumn,
    "cast_data_types": cast_data_types,
    "rename_columns": rename_columns,
}
