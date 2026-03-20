


from pyspark.sql.functions import current_timestamp, regexp_replace
from pyspark.sql.functions import to_date, to_timestamp, when
from pyspark.sql.functions import sha2, concat_ws, coalesce, col, lit


def addRowShaChecksum(df, checksum_col_name='row_checksum', hash_cols="all", seperator='-', bitlength=256):
    if hash_cols.lower() == "all":
        cols = df.columns
    else:
        cols = hash_cols.split(",")

    cols_expr = [coalesce(col(c).cast("string"), lit("-")) for c in cols]

    return df.withColumn(checksum_col_name, sha2(concat_ws(seperator, *cols_expr), bitlength))


def addTimestampColumn(df, colName ):
    new_df = df.withColumn(colName, current_timestamp())
    return new_df


def addSourceMetadata(df, colName ):
    """
    Adds or renames a column in the DataFrame using the existing `_metadata` column. This function selects all columns from the DataFrame and renames the `_metadata` column to the specified column name. If `_metadata` is part of the DataFrame's schema, it will be added as a new column or renamed to the desired name.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame.
        colName (str): The name of the new column to replace `_metadata`.
    Returns: pyspark.sql.DataFrame: A new DataFrame with the renamed `_metadata` column.
    """
    new_df = df.selectExpr("*", f"_metadata as {colName}")
    return new_df


def rename_columns(df, column_map):
    """
        Renames colums in a DataFrame based on a provided mapping.
    Parameters:
        df (DataFrame): The DataFrame containing the colums to be renamed.
        column_map (dict): A dictionary where keys are the current column names and values are the new column names.
    Returns: DataFrame: A new DataFrame with the columns renamed.
    """
    renamed_columns = [col(old).alias(new) for old, new in column_map.items()]

    for column_name in df.columns:
        if column_name not in column_map:
            renamed_columns.append(col(column_name))

    df = df.select(renamed_columns)

    return df


def cast_data_types(df, data_type_map):
    """
        Casts the data types of specified columns in a DataFrame based on a provided mapping.
    Parameters:
        df (DataFrame): The DataFrame containing the columns to be cast.
        data_type_map (dict): A dictionary where keys are the column names and values are the target data types.
        settings (dict): A dictionary that contains a data_type_map.
    Returns: DataFrame: A new DataFrame with the columns cast to the specified data types.
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
                selected_columns.append(col(column_name).alias(column_name))  # Keep column unchanged (in case it was not recognized)

    # Not sure if I want to include or not
    # Maybe need an option
    # I think I needed this for metadata columns that were not part of the data, but I didn't want to lose
    for column_name in df.columns:
        if column_name not in data_type_map:
            selected_columns.append(col(column_name))

    df = df.select(selected_columns)

    return df
