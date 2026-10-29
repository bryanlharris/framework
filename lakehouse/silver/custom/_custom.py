BRONZE_METADATA_COLS = ["_rescued_data", "source_metadata", "ingest_time", "derived_ingest_time"]


def systems_with_power(spark, settings):
    # Table names come from settings so ${catalog_prefix} applies to them.
    systems_table, power_table = settings["depends_on"]
    left_df  = spark.read.table(systems_table)
    right_df = spark.read.table(power_table)

    left_df  = left_df.drop(*BRONZE_METADATA_COLS)
    right_df = right_df.drop(*BRONZE_METADATA_COLS, "coords", "date", "id64", "name")

    joined = left_df.join(right_df, on=["id"], how="left")

    (
        joined.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(settings["destination_table"])
    )
