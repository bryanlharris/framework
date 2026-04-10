def systems_with_power(spark, settings):
    left_df  = spark.read.table("edsm.bronze.systemsWithCoordinates7days")
    right_df = spark.read.table("edsm.bronze.powerPlay")

    left_df  = left_df.drop("_rescued_data")
    right_df = right_df.drop("_rescued_data")

    joined = left_df.join(right_df, on=["id"], how="left")

    (
        joined.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("edsm.silver.systems_with_power")
    )
