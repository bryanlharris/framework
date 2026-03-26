from lakehouse.silver.merge import upsertByPK, scd2UpsertByBusinessKey, fullSyncMerge, fullSyncMergeSQL
from lakehouse.silver.transform.metadata import add_row_sha_checksum, flatten_source_metadata
from lakehouse.silver.transform.columns import cast_data_types, rename_columns


def _from_table_with_strategy(spark, settings, strategy_name):
    source_table        = settings["source_table"]
    destination_table   = settings["destination_table"]
    column_map          = settings["column_map"]
    data_type_map       = settings["data_type_map"]
    writeStream_options = settings["writeStream_options"]
    pk                  = settings["pk"]["name"]
    pk_columns          = settings["pk"]["columns"]
    pk_columns_str      = ",".join(pk_columns) if isinstance(pk_columns, list) else pk_columns
    readStream_options  = settings["readStream_options"]

    _strategies = {
        "upsert_by_pk":                lambda: upsertByPK(pk, destination_table, pk),
        "scd2_upsert_by_business_key": lambda: scd2UpsertByBusinessKey(pk_columns, destination_table, pk),
        "full_sync_merge":             lambda: fullSyncMerge(pk_columns, destination_table),
        "full_sync_merge_sql":         lambda: fullSyncMergeSQL(pk_columns, destination_table),
    }

    df = (
        spark.readStream
        .options(**readStream_options)
        .table(source_table)
        .drop("ingest_time")
        .transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .transform(flatten_source_metadata)
        .transform(add_row_sha_checksum, col_name=pk, columns=pk_columns_str)
    )

    (
        df.writeStream
        .queryName(destination_table)
        .format("delta")
        .options(**writeStream_options)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(_strategies[strategy_name]())
        .start()
    )


# _from_table_no_defaults — custom transform entry point
#
# Use this helper when the default transform pipeline (rename, cast, flatten
# metadata, add checksum) does not fit your source table.  Instead of
# modifying _from_table_with_strategy, add a new public function below that:
#
#   1. Pulls whatever values it needs from `settings`.
#   2. Defines a local `transform_fn(df) -> df` that applies all required
#      transforms to the raw streaming DataFrame.
#   3. Calls _from_table_no_defaults(spark, settings, transform_fn).
#
# The merge strategy used here is upsertByPK.  Add additional helpers if
# you need other strategies with custom transforms.
def _from_table_no_defaults(spark, settings, transform_fn):
    source_table        = settings["source_table"]
    destination_table   = settings["destination_table"]
    writeStream_options = settings["writeStream_options"]
    readStream_options  = settings["readStream_options"]
    pk                  = settings["pk"]["name"]

    df = (
        spark.readStream
        .options(**readStream_options)
        .table(source_table)
    )

    df = transform_fn(df)

    (
        df.writeStream
        .queryName(destination_table)
        .format("delta")
        .options(**writeStream_options)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(upsertByPK(pk, destination_table, pk))
        .start()
    )


def example_custom(spark, settings):
    column_map    = settings["column_map"]
    data_type_map = settings["data_type_map"]

    def transform_fn(df):
        return (
            df
            .drop("ingest_time")
            .transform(rename_columns, column_map)
            .transform(cast_data_types, data_type_map)
        )

    return _from_table_no_defaults(spark, settings, transform_fn)


def upsert_by_pk(spark, settings):
    return _from_table_with_strategy(spark, settings, "upsert_by_pk")


def scd2_upsert_by_business_key(spark, settings):
    return _from_table_with_strategy(spark, settings, "scd2_upsert_by_business_key")


def full_sync_merge(spark, settings):
    return _from_table_with_strategy(spark, settings, "full_sync_merge")


def full_sync_merge_sql(spark, settings):
    return _from_table_with_strategy(spark, settings, "full_sync_merge_sql")
