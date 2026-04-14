from datetime import timedelta


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

    raise ValueError(
        f"Duplicate primary keys detected in micro-batch.\n\n"
        f"Most likely cause: two source files contain conflicting data for the same entity.\n\n"
        f"  Earlier file: {earlier_path}\n"
        f"  Later file:   {later_path}\n\n"
        f"Recovery steps (retains data from the later file):\n\n"
        f"  1. RESTORE TABLE {source_table} TO VERSION AS OF {restore_version}\n"
        f"  2. Clear the bronze checkpoint\n"
        f"  3. Choose one:\n"
        f"       a. Delete the earlier file from the landing zone, OR\n"
        f'       b. In the bronze settings set:  "modifiedAfter": "{modified_after}"\n'
        f"  4. Clear the silver checkpoint\n"
        f"  5. Re-run the job\n"
        f"  6. After a successful run:\n"
        f"       - Remove modifiedAfter from the bronze settings (if used in step 3b)"
    )
