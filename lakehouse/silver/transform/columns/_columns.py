from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import to_date, to_timestamp, when


def rename_columns(df, column_map):
    """
    Rename DataFrame columns according to column_map (old_name -> new_name).
    Columns not present in the map are preserved unchanged.
    """
    renamed_columns = [col(old).alias(new) for old, new in column_map.items()]

    for column_name in df.columns:
        if column_name not in column_map:
            renamed_columns.append(col(column_name))

    df = df.select(renamed_columns)

    return df


def cast_data_types(df, data_type_map):
    """
    Cast DataFrame columns to the types specified in data_type_map. Handles numeric
    types (strips leading $ and , characters), dates in M/d/yyyy, d-M-yyyy, and
    yyyy-M-d formats, timestamps, and plain Spark casts. Columns not in the map are
    preserved unchanged.
    """
    selected_columns = []

    for column_name, data_type in data_type_map.items():
        if column_name in df.columns:
            if data_type in ["integer", "double", "short", "float"]:
                selected_columns.append(col(column_name).cast(data_type).alias(column_name))
            elif data_type.startswith("decimal("):
                selected_columns.append(regexp_replace(col(column_name), '[$,]', '').cast(data_type).alias(column_name))
            elif data_type.startswith("numeric("):
                selected_columns.append(regexp_replace(col(column_name), '[$,]', '').cast(data_type).alias(column_name))
            elif data_type == "date":
                selected_columns.append(
                    when(col(column_name).rlike(r'\d{1,2}/\d{1,2}/\d{4}'), to_date(col(column_name), 'M/d/yyyy'))
                    .when(col(column_name).rlike(r'\d{1,2}-\d{1,2}-\d{4}'), to_date(col(column_name), 'd-M-yyyy'))
                    .when(col(column_name).rlike(r'\d{4}-\d{1,2}-\d{1,2}'), to_date(col(column_name), 'yyyy-M-d'))
                    .alias(column_name)
                )
            elif data_type == "timestamp":
                selected_columns.append(
                    when(col(column_name).rlike(r'\d{1,2}/\d{1,2}/\d{4}'), to_date(col(column_name), 'M/d/yyyy'))
                    .when(col(column_name).rlike(r'\d{1,2}-\d{1,2}-\d{4}'), to_date(col(column_name), 'd-M-yyyy'))
                    .otherwise(to_timestamp(col(column_name)))
                    .alias(column_name)
                )
            else:
                selected_columns.append(col(column_name).alias(column_name))

    for column_name in df.columns:
        if column_name not in data_type_map:
            selected_columns.append(col(column_name))

    df = df.select(selected_columns)

    return df
