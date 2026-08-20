from datetime import timedelta


def _dedupe_latest_pk(df, pk_columns, source_table):
    """
    Keep only the most recently modified row per pk within a micro-batch,
    dropping earlier duplicates. Used by jobs that opt in via
    settings["on_duplicate_pk"] == "dedupe_latest" instead of failing hard
    on duplicate primary keys.
    """
    from pyspark.sql import Window
    from pyspark.sql.functions import row_number, col

    before = df.count()

    w = Window.partitionBy(*pk_columns).orderBy(col("file_modification_time").desc())
    deduped = (
        df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    after = deduped.count()
    if after < before:
        print(
            f"[{source_table}] on_duplicate_pk=dedupe_latest: dropped {before - after} "
            f"duplicate-pk row(s) from micro-batch, keeping the most recently modified "
            f"file per key."
        )

    return deduped


def _check_duplicate_pk(df, pk_columns, source_table):
    from pyspark.sql.functions import count

    dup_keys = (
        df.groupBy(*pk_columns)
        .agg(count("*").alias("_cnt"))
        .filter("_cnt > 1")
        .drop("_cnt")
    )

    if dup_keys.isEmpty():
        return

    conflicting = (
        df.join(dup_keys, on=pk_columns, how="inner")
        .select("file_path", "file_modification_time")
        .distinct()
        .orderBy("file_modification_time")
        .collect()
    )

    file_paths = [row["file_path"] for row in conflicting]

    if len(file_paths) == 1:
        raise ValueError(
            f"Duplicate primary keys detected in micro-batch.\n\n"
            f"The source file itself contains duplicate primary keys:\n"
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

    restore_version = bad_version - 1 if bad_version is not None else "<version before earlier file>"

    starting_version_clean  = current_version
    starting_version_bronze = current_version + 2 if current_version is not None else "<post-restore version + 1>"

    raise ValueError(
        f"Duplicate primary keys detected in micro-batch.\n\n"
        f"The same primary key appears in multiple source files in this micro-batch:\n\n"
        f"  Earlier file: {earlier_path}\n"
        f"  Later file:   {later_path}\n\n"
        f"Most likely cause: the same dataset was ingested more than once into bronze\n"
        f"(e.g. daily downloads of a rolling snapshot). Bronze is probably fine.\n\n"
        f"Quick fix — start silver from the latest bronze version only:\n\n"
        f"  1. In the silver readStream_options, add:\n"
        f'       "startingVersion": {starting_version_clean}\n'
        f"  2. Clear the silver checkpoint\n"
        f"  3. Re-run the job\n"
        f"  4. After a successful run, remove startingVersion from the silver settings\n\n"
        f"If you believe the conflicting files represent bad data rather than repeated\n"
        f"ingestion, fix bronze first then follow up with a silver fix:\n\n"
        f"  1. RESTORE TABLE {source_table} TO VERSION AS OF {restore_version}\n"
        f"  2. Clear the bronze checkpoint\n"
        f"  3. Choose one:\n"
        f"       a. Delete the earlier file from the landing zone, OR\n"
        f'       b. In the bronze settings set:  "modifiedAfter": "{modified_after}"\n'
        f"  4. Re-run bronze\n"
        f"  5. In the silver readStream_options, add:\n"
        f'       "startingVersion": {starting_version_bronze}\n'
        f'       "ignoreDeletes": true\n'
        f"  6. Clear the silver checkpoint\n"
        f"  7. Re-run the job\n"
        f"  8. After a successful run:\n"
        f"       - Remove startingVersion and ignoreDeletes from the silver settings\n"
        f"       - Remove modifiedAfter from the bronze settings (if used in step 3b)"
    )
