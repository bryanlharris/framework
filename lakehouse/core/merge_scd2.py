from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def scd2UpsertByBusinessKey(business_key, surrogate_key, destinationTable, ingest_time_column, use_row_hash=False, row_hash_col="row_hash"):
    def _do_upsert(microBatchDF, batchId):
        window = Window.partitionBy(*business_key).orderBy(col(ingest_time_column).desc())
        df = (
            microBatchDF.withColumn("rn", row_number().over(window))
            .filter("rn = 1")
            .drop("rn")
        )
        merge_condition = " and ".join([f"t.{k} = s.{k}" for k in business_key])

        if use_row_hash:
            change_condition = f"t.{row_hash_col} <> s.{row_hash_col}"
        else:
            change_condition = " or ".join([f"t.{k} <> s.{k}" for k in surrogate_key])
        df.createOrReplaceTempView("updates")
        df.sparkSession.sql(
            f"""
            MERGE INTO {destinationTable} t
            USING updates s
            ON {merge_condition} AND t.current_flag='Yes'
            WHEN MATCHED AND ({change_condition}) THEN
                UPDATE SET
                    t.deleted_on=s.{ingest_time_column},
                    t.current_flag='No',
                    t.valid_to=s.{ingest_time_column}
            """
        )
        df.sparkSession.sql(
            f"""
            INSERT INTO {destinationTable}
            SELECT
                s.* EXCEPT (created_on, deleted_on, current_flag, valid_from, valid_to),
                s.{ingest_time_column} AS created_on,
                NULL AS deleted_on,
                'Yes' AS current_flag,
                s.{ingest_time_column} AS valid_from,
                CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS valid_to
            FROM updates s
            LEFT JOIN {destinationTable} t
                ON {merge_condition} AND t.current_flag='Yes'
            WHERE t.current_flag IS NULL
            """
        )
    return _do_upsert
