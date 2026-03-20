from functions.silver.upsert_with_pk_columns_checksum import (
    upsert_with_pk_columns_checksum,
)

STATIC_FUNCTIONS = {
    "upsert_with_pk_columns_checksum": upsert_with_pk_columns_checksum,
    "functions.silver.upsert_with_pk_columns_checksum.upsert_with_pk_columns_checksum": upsert_with_pk_columns_checksum,
}

__all__ = ["STATIC_FUNCTIONS", "upsert_with_pk_columns_checksum"]
