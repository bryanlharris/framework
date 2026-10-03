import pyspark.sql.functions as F
from pyspark.sql.functions import col, regexp_replace, regexp_extract, from_json
from pyspark.sql.functions import to_date, to_timestamp, when


def _check_cast_nulls(df, data_type_map, source_table):
    cast_cols = list(data_type_map.keys())
    if not cast_cols:
        return

    null_counts = df.agg(
        *[F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in cast_cols]
    ).collect()[0]

    failed_cols = [c for c in cast_cols if null_counts[c] > 0]
    if not failed_cols:
        return

    null_filter = F.lit(False)
    for c in failed_cols:
        null_filter = null_filter | F.col(c).isNull()

    file_paths = (
        df.filter(null_filter)
        .select("file_path")
        .distinct()
        .orderBy("file_path")
        .collect()
    )
    file_paths = [row["file_path"] for row in file_paths]
    file_paths_str = "\n".join(f"  {p}" for p in file_paths)

    col_summary = ", ".join(
        f"{c} ({null_counts[c]} null{'s' if null_counts[c] != 1 else ''})"
        for c in failed_cols
    )

    raise ValueError(
        f"Cast failures detected in micro-batch for table {source_table}: {col_summary}\n\n"
        f"This is a source data quality issue. The bad data was never written to silver —\n"
        f"no Delta restore is needed.\n\n"
        f"Affected source file(s):\n{file_paths_str}\n\n"
        f"Recovery steps:\n\n"
        f"  1. Fix the malformed values in the source file(s)\n"
        f"  2. Replace the corrected file(s) in the landing zone\n"
        f'  3. In the bronze settings set:  "modifiedAfter": "<timestamp just before corrected file>"\n'
        f"  4. Clear the bronze checkpoint, re-run bronze\n"
        f'  5. In the silver settings set:  "startingVersion": "<bronze version after re-ingest>"\n'
        f"  6. Clear the silver checkpoint, re-run silver\n"
        f"  7. After a successful run:\n"
        f"       - Remove modifiedAfter from the bronze settings\n"
        f"       - Remove startingVersion from the silver settings"
    )


def rename_columns(df, column_map):
    """
    Rename DataFrame columns according to column_map (old_name -> new_name).
    Columns not present in the map are preserved unchanged.
    Preserves the order of the columns.
    """
    unmatched = [k for k in column_map if k not in df.columns]
    if unmatched:
        raise ValueError(
            f"rename_columns: the following column_map keys do not exist in the source DataFrame:\n"
            f"  Missing: {unmatched}\n"
            f"  Available columns: {df.columns}\n\n"
            f"Possible causes: typo in the settings JSON, or the source schema has changed "
            f"(column renamed or dropped at bronze)."
        )

    df = df.select([col(c).alias(column_map.get(c, c)) for c in df.columns])

    return df


def cast_data_types(df, data_type_map):
    """
    Cast DataFrame columns to the types specified in data_type_map. Handles numeric
    types (strips leading $ and , characters), dates in M/d/yyyy, d-M-yyyy, and
    yyyy-M-d formats, timestamps, and plain Spark casts. Columns not in the map are
    preserved unchanged.
    Preserves the order of the columns.
    """
    unmatched = [k for k in data_type_map if k not in df.columns]
    if unmatched:
        raise ValueError(
            f"cast_data_types: the following data_type_map keys do not exist in the DataFrame "
            f"(checked after column renames have been applied):\n"
            f"  Missing: {unmatched}\n"
            f"  Available columns: {df.columns}\n\n"
            f"Possible causes: typo in the settings JSON, column renamed or dropped at bronze, "
            f"or data_type_map references a pre-rename name that should now be the post-rename name."
        )

    cast_expressions = {}

    for column_name, data_type in data_type_map.items():
        if data_type in ["integer", "double", "short", "float"]:
            cast_expressions[column_name] = col(column_name).cast(data_type).alias(column_name)
        elif data_type.startswith("decimal("):
            cast_expressions[column_name] = regexp_replace(col(column_name), '[$,]', '').cast(data_type).alias(column_name)
        elif data_type.startswith("numeric("):
            cast_expressions[column_name] = regexp_replace(col(column_name), '[$,]', '').cast(data_type).alias(column_name)
        elif data_type == "date":
            cast_expressions[column_name] = (
                when(col(column_name).rlike(r'\d{1,2}/\d{1,2}/\d{4}'), to_date(col(column_name), 'M/d/yyyy'))
                .when(col(column_name).rlike(r'\d{1,2}-\d{1,2}-\d{4}'), to_date(col(column_name), 'd-M-yyyy'))
                .when(col(column_name).rlike(r'\d{4}-\d{1,2}-\d{1,2}'), to_date(col(column_name), 'yyyy-M-d'))
                .when(col(column_name).rlike(r'^\d{8}$'), to_date(col(column_name), 'yyyyMMdd'))
                .alias(column_name)
            )
        elif data_type == "timestamp":
            cast_expressions[column_name] = (
                when(col(column_name).rlike(r'\d{1,2}/\d{1,2}/\d{4}'), to_date(col(column_name), 'M/d/yyyy'))
                .when(col(column_name).rlike(r'\d{1,2}-\d{1,2}-\d{4}'), to_date(col(column_name), 'd-M-yyyy'))
                .otherwise(to_timestamp(col(column_name)))
                .alias(column_name)
            )
        else:
            cast_expressions[column_name] = col(column_name).alias(column_name)

    df = df.select([cast_expressions.get(c, col(c)) for c in df.columns])

    return df


def parse_json_columns(df, json_column_map):
    """
    Parse JSON string columns into structured Spark types using from_json.
    json_column_map maps column names to Spark DDL type strings (e.g. "ARRAY<STRING>").
    Preserves column order.
    """
    if not json_column_map:
        return df

    active_map = {c: ddl for c, ddl in json_column_map.items() if c in df.columns}
    parse_expressions = {c: from_json(col(c), ddl).alias(c) for c, ddl in active_map.items()}
    return df.select([parse_expressions.get(c, col(c)) for c in df.columns])


def regex_extract(df, regex_extract_list):
    """
    Add columns by matching a regex against a source column. Each entry in
    regex_extract_list is a dict:
      - source:  column to match against; may be nested (e.g. "source_metadata.file_path")
      - pattern: Java regex with one capture group per name in columns
      - columns: new column names, filled from capture groups 1, 2, ... in order
      - types (optional): maps a new column name to a Spark SQL type (e.g. "timestamp", "int")

    Rows where the pattern does not match get null in every column of that entry,
    so unparsed rows are kept and easy to find. Type conversion uses try_cast,
    so it never fails the batch; let the pattern enforce the shape of each value.
    New columns are added at the end, replacing any existing column of the same name.
    """
    if not regex_extract_list:
        return df

    for entry in regex_extract_list:
        source  = entry["source"]
        pattern = entry["pattern"]
        columns = entry["columns"]
        types   = entry.get("types", {})

        if source.split(".")[0] not in df.columns:
            raise ValueError(
                f"regex_extract: source column {source!r} does not exist in the DataFrame "
                f"(checked after column renames have been applied).\n"
                f"  Available columns: {df.columns}"
            )
        unknown_types = [c for c in types if c not in columns]
        if unknown_types:
            raise ValueError(
                f"regex_extract: 'types' names columns that are not in 'columns': {unknown_types}"
            )

        matched = col(source).rlike(pattern)
        for group, name in enumerate(columns, start=1):
            df = df.withColumn(name, when(matched, regexp_extract(col(source), pattern, group)))
            if name in types:
                df = df.withColumn(name, F.expr(f"try_cast(`{name}` AS {types[name]})"))

    return df
