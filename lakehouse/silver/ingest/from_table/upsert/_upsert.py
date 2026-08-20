from lakehouse.silver.merge import upsertByPK, fullSyncMerge
from lakehouse.silver.transform.metadata import add_row_hash, add_ingest_metadata
from lakehouse.silver.transform.columns import cast_data_types, parse_json_columns, rename_columns, _check_cast_nulls
from lakehouse.silver.ingest.from_table._checks import _check_duplicate_pk, _dedupe_latest_pk

_ON_DUPLICATE_PK_MODES = {"raise", "dedupe_latest"}


def _resolve_on_duplicate_pk(settings):
    mode = settings.get("on_duplicate_pk", "raise")
    if mode not in _ON_DUPLICATE_PK_MODES:
        raise ValueError(
            f'Invalid "on_duplicate_pk" setting: {mode!r}. '
            f"Must be one of {sorted(_ON_DUPLICATE_PK_MODES)}."
        )
    return mode


def without_delete(spark, settings):
    """
    Read streaming data from a bronze table, apply column renames, type casts,
    metadata flattening, and row checksums, then upsert into a silver Delta table
    by primary key without deleting records absent from the source.

    Example settings:
    {
        "function_path": "lakehouse.silver.ingest.from_table.upsert.without_delete",
        "column_map": {},
        "data_type_map": {
            "date": "timestamp",
            "value": "double"
        },
        "pk": {
            "name": "primary_key",
            "columns": ["observation_date", "FEDFUNDS"]
        }
    }
    """
    source_table        = settings["source_table"]
    destination_table   = settings["destination_table"]
    column_map          = settings["column_map"]
    data_type_map       = settings["data_type_map"]
    json_column_map     = settings.get("json_column_map", {})
    writeStream_options = settings["writeStream_options"]
    pk                  = settings["pk"]["name"]
    pk_columns          = settings["pk"]["columns"]
    pk_columns_str      = ",".join(pk_columns) if isinstance(pk_columns, list) else pk_columns
    readStream_options  = settings["readStream_options"]
    on_duplicate_pk     = _resolve_on_duplicate_pk(settings)

    df = (
        spark.readStream
        .options(**readStream_options)
        .table(source_table)
        .drop("ingest_time")
        .transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .transform(parse_json_columns, json_column_map)
        .transform(add_ingest_metadata)
        .transform(add_row_hash, col_name=pk, columns=pk_columns_str)
    )

    _merge_fn = upsertByPK(pk, destination_table, pk)

    def _do_upsert(microBatchDF, batchId):
        _check_cast_nulls(microBatchDF, data_type_map, source_table)
        if on_duplicate_pk == "dedupe_latest":
            microBatchDF = _dedupe_latest_pk(microBatchDF, pk_columns, source_table)
        _check_duplicate_pk(microBatchDF, pk_columns, source_table)
        _merge_fn(microBatchDF, batchId)

    (
        df.writeStream
        .queryName(destination_table)
        .format("delta")
        .options(**writeStream_options)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(_do_upsert)
        .start()
    )


def with_delete(spark, settings):
    """
    Read streaming data from a bronze table, apply column renames, type casts,
    metadata flattening, and row checksums, then perform a full-sync merge into a
    silver Delta table by primary key, including deletion of records absent from
    the source.

    Example settings:
    {
        "function_path": "lakehouse.silver.ingest.from_table.upsert.with_delete",
        "column_map": {
            "marketId": "market_id"
        },
        "data_type_map": {
            "market_id": "long",
            "updateTime": "timestamp"
        },
        "pk": {
            "name": "primary_key",
            "columns": ["market_id"]
        }
    }
    """
    source_table        = settings["source_table"]
    destination_table   = settings["destination_table"]
    column_map          = settings["column_map"]
    data_type_map       = settings["data_type_map"]
    json_column_map     = settings.get("json_column_map", {})
    writeStream_options = settings["writeStream_options"]
    pk                  = settings["pk"]["name"]
    pk_columns          = settings["pk"]["columns"]
    pk_columns_str      = ",".join(pk_columns) if isinstance(pk_columns, list) else pk_columns
    readStream_options  = settings["readStream_options"]
    on_duplicate_pk     = _resolve_on_duplicate_pk(settings)

    df = (
        spark.readStream
        .options(**readStream_options)
        .table(source_table)
        .drop("ingest_time")
        .transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .transform(parse_json_columns, json_column_map)
        .transform(add_ingest_metadata)
        .transform(add_row_hash, col_name=pk, columns=pk_columns_str)
    )

    _merge_fn = fullSyncMerge(pk_columns, destination_table)

    def _do_upsert(microBatchDF, batchId):
        _check_cast_nulls(microBatchDF, data_type_map, source_table)
        if on_duplicate_pk == "dedupe_latest":
            microBatchDF = _dedupe_latest_pk(microBatchDF, pk_columns, source_table)
        _check_duplicate_pk(microBatchDF, pk_columns, source_table)
        _merge_fn(microBatchDF, batchId)

    (
        df.writeStream
        .queryName(destination_table)
        .format("delta")
        .options(**writeStream_options)
        .outputMode("update")
        .trigger(availableNow=True)
        .foreachBatch(_do_upsert)
        .start()
    )
