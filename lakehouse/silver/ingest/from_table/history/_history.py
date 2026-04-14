from datetime import timedelta

from lakehouse.silver.merge import scd2UpsertByBusinessKey
from lakehouse.silver.transform.metadata import add_row_hash, add_ingest_metadata
from lakehouse.silver.transform.columns import cast_data_types, rename_columns


def _check_duplicate_business_keys(df, business_key, source_table):
    from pyspark.sql.functions import count

    dup_keys = (
        df.groupBy(*business_key)
        .agg(count("*").alias("_cnt"))
        .filter("_cnt > 1")
        .drop("_cnt")
    )

    if dup_keys.isEmpty():
        return

    conflicting = (
        df.join(dup_keys, on=business_key, how="inner")
        .select("file_path", "file_modification_time")
        .distinct()
        .orderBy("file_modification_time")
        .collect()
    )

    file_paths = [row["file_path"] for row in conflicting]

    if len(file_paths) == 1:
        raise ValueError(
            f"Duplicate business keys detected in micro-batch.\n\n"
            f"The source file itself contains duplicate business keys:\n"
            f"  {file_paths[0]}\n\n"
            f"This file must be fixed at the source before re-running."
        )

    earlier_path  = conflicting[0]["file_path"]
    later_path    = conflicting[-1]["file_path"]
    min_file_time = conflicting[0]["file_modification_time"]

    modified_after = (min_file_time + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    history = df.sparkSession.sql(f"DESCRIBE HISTORY {source_table} LIMIT 50").collect()

    current_version = history[0]["version"] if history else None

    bad_version = None
    for row in reversed(history):
        if row["timestamp"] >= min_file_time:
            bad_version = row["version"]
            break

    restore_version  = bad_version - 1 if bad_version is not None else "<version before earlier file>"
    starting_version = current_version + 2 if current_version is not None else "<post-restore version + 1>"

    raise ValueError(
        f"Duplicate business keys detected in micro-batch.\n\n"
        f"Most likely cause: two source files contain conflicting data for the same entity.\n\n"
        f"  Earlier file: {earlier_path}\n"
        f"  Later file:   {later_path}\n\n"
        f"Recovery steps (retains data from the later file):\n\n"
        f"  1. RESTORE TABLE {source_table} TO VERSION AS OF {restore_version}\n"
        f"  2. Clear the bronze checkpoint\n"
        f"  3. Choose one:\n"
        f"       a. Delete the earlier file from the landing zone, OR\n"
        f'       b. In the bronze settings set:  "modifiedAfter": "{modified_after}"\n'
        f'  4. In the silver settings set:  "startingVersion": "{starting_version}"\n'
        f"  5. Clear the silver checkpoint\n"
        f"  6. Re-run the job\n"
        f"  7. After a successful run:\n"
        f"       - Remove modifiedAfter from the bronze settings (if used in step 3b)\n"
        f"       - Remove startingVersion from the silver settings"
    )


def scd2(spark, settings):
    """
    Read streaming data from a bronze table, apply column renames, type casts,
    metadata flattening, and row checksums, then write to a silver Delta table
    using SCD2 merge logic (insert new/changed records, expire old ones).

    Example settings:
    {
        "function_path": "lakehouse.silver.ingest.from_table.history.scd2",
        "business_key": ["id", "power"],
        "surrogate_key": ["allegiance", "government", "powerState", "state"],
        "column_map": {},
        "data_type_map": {
            "date": "timestamp"
        },
        "ingest_time_column": "derived_ingest_time"
    }
    """
    source_table        = settings["source_table"]
    destination_table   = settings["destination_table"]
    column_map          = settings.get("column_map", {})
    data_type_map       = settings.get("data_type_map", {})
    writeStream_options = settings["writeStream_options"]
    business_key        = settings["business_key"]
    surrogate_key       = settings["surrogate_key"]
    readStream_options  = settings.get("readStream_options", {})
    use_row_hash        = settings.get("use_row_hash", False)
    row_hash_col        = settings.get("row_hash_col", "row_hash")
    ingest_time_column  = settings["ingest_time_column"]

    surrogate_key_str = ",".join(surrogate_key) if isinstance(surrogate_key, list) else surrogate_key

    df = (
        spark.readStream
        .options(**readStream_options)
        .table(source_table)
        .drop("ingest_time")
        .transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .transform(add_ingest_metadata)
        .transform(add_row_hash, col_name=row_hash_col, columns=surrogate_key_str)
    )

    _merge_fn = scd2UpsertByBusinessKey(business_key, surrogate_key, destination_table, ingest_time_column, use_row_hash, row_hash_col)

    def _do_upsert(microBatchDF, batchId):
        _check_duplicate_business_keys(microBatchDF, business_key, source_table)
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
