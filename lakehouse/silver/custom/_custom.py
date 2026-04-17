BRONZE_METADATA_COLS = ["_rescued_data", "source_metadata", "ingest_time", "derived_ingest_time"]


def systems_with_power(spark, settings):
    left_df  = spark.read.table("edsm.bronze.systemsWithCoordinates7days")
    right_df = spark.read.table("edsm.bronze.powerPlay")

    left_df  = left_df.drop(*BRONZE_METADATA_COLS)
    right_df = right_df.drop(*BRONZE_METADATA_COLS, "coords", "date", "id64", "name")

    joined = left_df.join(right_df, on=["id"], how="left")

    (
        joined.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("edsm.silver.systems_with_power")
    )
