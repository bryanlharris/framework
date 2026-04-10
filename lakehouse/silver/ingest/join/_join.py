from pyspark.sql.functions import col

def left(spark, settings):
    left_table    = settings["left_table"]
    right_table   = settings["right_table"]
    join_keys     = settings["join_keys"]
    destination   = settings["destination_table"]
    write_options = settings.get("write_options", {})
    write_mode    = settings.get("write_mode", "overwrite")

    left_df  = spark.read.table(left_table)
    right_df = spark.read.table(right_table)

    joined = left_df.join(right_df, on=join_keys, how="left")

    (
        joined.write
        .format("delta")
        .options(**write_options)
        .mode(write_mode)
        .saveAsTable(destination)
    )
