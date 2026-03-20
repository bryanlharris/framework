from functions.standardTransformations import (
    addRowShaChecksum,
    addRowShaChecksumWithSep,
    addSourceMetadata,
    addTimestampColumn,
    cast_data_types,
    rename_columns,
)

TRANSFORM_REGISTRY = {
    "addRowShaChecksum": addRowShaChecksum,
    "addRowShaChecksumWithSep": addRowShaChecksumWithSep,
    "addSourceMetadata": addSourceMetadata,
    "addTimestampColumn": addTimestampColumn,
    "cast_data_types": cast_data_types,
    "rename_columns": rename_columns,
}
