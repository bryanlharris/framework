from pyspark.sql.functions import sha2, col, lit, current_timestamp, to_json, struct, transform, coalesce, array
from pyspark.sql.types import StructType, ArrayType, MapType


def make_null_safe(field_type, col_expr):
    if isinstance(field_type, MapType):
        raise TypeError(
            "MapType columns are not supported for checksum hashing "
            "because map key ordering is not stable in to_json"
        )
    if isinstance(field_type, StructType):
        safe_fields = [
            make_null_safe(f.dataType, col_expr[f.name]).alias(f.name)
            for f in field_type.fields
        ]
        return struct(*safe_fields)
    if isinstance(field_type, ArrayType):
        if isinstance(field_type.elementType, (StructType, ArrayType, MapType)):
            return coalesce(
                transform(col_expr, lambda x: make_null_safe(field_type.elementType, x)),
                array()
            )
        return coalesce(col_expr, array())
    return col_expr


def add_row_sha_checksum(df, col_name='row_checksum', columns=None):
    if columns is not None:
        cols = [c.strip() for c in columns.split(",")] if isinstance(columns, str) else list(columns)
    else:
        cols = df.columns

    field_map = {f.name: f.dataType for f in df.schema.fields}

    normalized = [
        make_null_safe(field_map[c], col(c)).alias(c)
        for c in cols
    ]

    return df.withColumn(col_name, sha2(to_json(struct(*normalized)), 256))


def flatten_source_metadata(df):
    return df.select(
        "*",
        col("source_metadata.file_path").alias("file_path"),
        col("source_metadata.file_modification_time").alias(
            "file_modification_time"
        ),
        current_timestamp().alias("ingest_time"),
    )


def add_scd2_columns(df, ingest_time_column):
    from pyspark.sql.functions import col, lit
    return (
        df
        .withColumn("created_on", col(ingest_time_column))
        .withColumn("deleted_on", lit(None).cast("timestamp"))
        .withColumn("current_flag", lit("Yes"))
        .withColumn("valid_from", col(ingest_time_column))
        .withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp"))
    )
