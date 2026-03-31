-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PowerPlay Over Time
-- MAGIC System counts per power, tracked via SCD2 history.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Systems Controlled Per Power Over Time
-- MAGIC *One row per power per snapshot date — use as a filled/stacked area chart*

-- COMMAND ----------

WITH dates AS (
    SELECT DISTINCT CAST(valid_from AS DATE) AS snapshot_date
    FROM edsm.silver.powerplay
    WHERE valid_from IS NOT NULL
),
snapshot AS (
    SELECT
        d.snapshot_date,
        p.power,
        COUNT(DISTINCT p.id) AS system_count
    FROM dates d
    JOIN edsm.silver.powerplay p
        ON CAST(p.valid_from AS DATE) <= d.snapshot_date
        AND CAST(p.valid_to   AS DATE) >  d.snapshot_date
    WHERE p.valid_from IS NOT NULL
    GROUP BY d.snapshot_date, p.power
)
SELECT *
FROM snapshot
ORDER BY snapshot_date, power;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Total Systems Tracked Over Time (All Powers Combined)

-- COMMAND ----------

SELECT
    valid_from      AS snapshot_date,
    COUNT(DISTINCT id) AS total_systems
FROM edsm.silver.v_powerplay
GROUP BY valid_from
ORDER BY snapshot_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Most Volatile Systems (Changed Power or State the Most)

-- COMMAND ----------

SELECT
    name,
    COUNT(*) AS version_count
FROM edsm.silver.v_powerplay
GROUP BY name
HAVING COUNT(*) > 1
ORDER BY version_count DESC
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## powerState Distribution Per Power (Current Records Only)

-- COMMAND ----------

SELECT
    power,
    powerState,
    COUNT(DISTINCT id) AS system_count
FROM edsm.silver.v_powerplay
WHERE current_flag = 'Yes'
GROUP BY power, powerState
ORDER BY power, system_count DESC;
