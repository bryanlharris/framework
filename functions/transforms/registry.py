from functions.standardTransformations import (
    addRowShaChecksum,
    addSourceMetadata,
    addTimestampColumn,
    cast_data_types,
    rename_columns,
)

transform_registry = {
    "addRowShaChecksum": addRowShaChecksum,
    "functions.standardTransformations.addRowShaChecksum": addRowShaChecksum,
    "addSourceMetadata": addSourceMetadata,
    "functions.standardTransformations.addSourceMetadata": addSourceMetadata,
    "addTimestampColumn": addTimestampColumn,
    "functions.standardTransformations.addTimestampColumn": addTimestampColumn,
    "cast_data_types": cast_data_types,
    "functions.standardTransformations.cast_data_types": cast_data_types,
    "rename_columns": rename_columns,
    "functions.standardTransformations.rename_columns": rename_columns,
}
