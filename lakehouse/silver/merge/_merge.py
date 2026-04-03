from delta.tables import DeltaTable
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from lakehouse.core.utils import create_table_if_not_exists


def fullSyncMergeSQL(mergeKeys, destinationTable):
    """
    Return a foreachBatch function that performs a full-sync MERGE via SQL,
    handling UPDATE, INSERT, and DELETE for records not present in the source batch.
    """
    def _do_upsert(microBatchDF, batchId):
        merge_condition = " AND ".join([f"source.{col} = target.{col}" for col in mergeKeys])

        microBatchDF.createOrReplaceTempView("microBatchDF")

        microBatchDF.sparkSession.sql(f"""
            MERGE INTO {destinationTable} AS target
            USING microBatchDF AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)

    return _do_upsert


def fullSyncMerge(mergeKeys, destinationTable):
    """
    Return a foreachBatch function that performs a full-sync MERGE via the Delta API,
    handling UPDATE, INSERT, and DELETE for records not present in the source batch.
    Creates the destination table on first run if it does not exist.
    """
    def _do_upsert(microBatchDF, batchId):
        if not microBatchDF.sparkSession.catalog.tableExists(destinationTable):
            microBatchDF.write.format("delta").mode("overwrite").saveAsTable(destinationTable)
            return
        deltaTable = DeltaTable.forName(microBatchDF.sparkSession, destinationTable)

        merge_condition = " AND ".join([f"s.{col} = d.{col}" for col in mergeKeys])
        (
            deltaTable.alias("d")
            .merge(microBatchDF.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
    return _do_upsert


def upsertByPK(sourcePK, destinationTable, destinationPK):
    """
    Return a foreachBatch function that upserts records by primary key via the Delta API
    (UPDATE + INSERT only, no deletes). Creates the destination table on first run.
    """
    def _do_upsert(microBatchDF, batchId):
        if not microBatchDF.sparkSession.catalog.tableExists(destinationTable):
            microBatchDF.write.format("delta").mode("overwrite").saveAsTable(destinationTable)
            return
        deltaTable = DeltaTable.forName(microBatchDF.sparkSession, destinationTable)
        (
            deltaTable.alias("d")
            .merge(microBatchDF.alias("s"), f"s.{sourcePK} = d.{destinationPK}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    return _do_upsert


def scd2UpsertByBusinessKey(business_key, surrogate_key, destinationTable, ingest_time_column, use_row_hash=False, row_hash_col="row_hash"):
    """
    Return a foreachBatch function that implements SCD2 upsert logic. Deduplicates the
    micro-batch by business key (keeping the latest record by ingest_time_column), marks
    changed current records as expired, and inserts new or changed records with SCD2
    tracking columns (created_on, deleted_on, current_flag, valid_from, valid_to).
    When use_row_hash is True, change detection uses row_hash_col instead of comparing
    surrogate_key columns individually.

    Expire + insert are performed in a single atomic MERGE using an augmented USING clause:
    - Set A (NULL merge keys): new/changed records to insert — never matches, always INSERT
    - Set B (real merge keys): existing current rows to expire — matches, triggers UPDATE
    """
    def _do_upsert(microBatchDF, batchId):
        window = Window.partitionBy(*business_key).orderBy(col(ingest_time_column).desc())
        df = (
            microBatchDF.withColumn("rn", row_number().over(window))
            .filter("rn = 1")
            .drop("rn")
        )
        create_table_if_not_exists(df.sparkSession, destinationTable, df)
        df.createOrReplaceTempView("updates")

        merge_condition = " and ".join([f"t.{k} = s.{k}" for k in business_key])

        if use_row_hash:
            change_condition = f"t.{row_hash_col} <> s.{row_hash_col}"
        else:
            change_condition = " or ".join([f"t.{k} <> s.{k}" for k in surrogate_key])

        # Merge key columns added to the USING source to drive MATCHED vs NOT MATCHED:
        # NULL  → condition `t.k = staged._mk_k` is always false → WHEN NOT MATCHED INSERT
        # real  → condition matches the existing current row       → WHEN MATCHED UPDATE
        mk_null  = ", ".join([f"CAST(NULL AS STRING) AS _mk_{k}" for k in business_key])
        mk_real  = ", ".join([f"t.{k} AS _mk_{k}" for k in business_key])
        mk_match = " and ".join([f"t.{k} = staged._mk_{k}" for k in business_key])

        # Set A: only include records that are new (no current row) or have changed.
        # business_key columns are assumed non-nullable, so IS NULL reliably detects no match.
        no_current_row = f"t.{business_key[0]} IS NULL"

        insert_cols     = df.columns
        insert_cols_str = ", ".join(insert_cols)
        insert_vals_str = ", ".join([f"staged.{c}" for c in insert_cols])

        df.sparkSession.sql(f"""
            MERGE INTO {destinationTable} t
            USING (
                -- Set A: new/changed records to insert as the new current version
                SELECT s.*, {mk_null}, s.{ingest_time_column} AS _src_ingest_time
                FROM updates s
                LEFT JOIN {destinationTable} t ON {merge_condition} AND t.current_flag = 'Yes'
                WHERE {no_current_row} OR ({change_condition})
                UNION ALL
                -- Set B: existing current rows that need to be expired
                SELECT t.*, {mk_real}, s.{ingest_time_column} AS _src_ingest_time
                FROM {destinationTable} t
                JOIN updates s ON {merge_condition} AND t.current_flag = 'Yes'
                WHERE ({change_condition})
            ) staged
            ON {mk_match} AND t.current_flag = 'Yes'
            WHEN MATCHED THEN
                UPDATE SET
                    t.deleted_on   = staged._src_ingest_time,
                    t.current_flag = 'No',
                    t.valid_to     = staged._src_ingest_time
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols_str})
                VALUES ({insert_vals_str})
        """)
    return _do_upsert
