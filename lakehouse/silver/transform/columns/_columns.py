from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import to_date, to_timestamp, when


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
