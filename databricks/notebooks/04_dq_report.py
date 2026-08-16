# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data Quality Summary Report
# MAGIC Summarizes the DQ check results table into a single-row-per-run scorecard
# MAGIC that feeds the "Data Quality" page of the Power BI Sales Performance dashboard.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("loadDate", "")
load_date = dbutils.widgets.get("loadDate")

GOLD = "abfss://retail-lake@retailsalesadls.dfs.core.windows.net/gold"

dq_log = spark.read.format("delta").load(f"{GOLD}/dq_check_results")

scorecard = (
    dq_log.groupBy("dataset")
    .agg(
        F.count("*").alias("checks_run"),
        F.sum(F.when(F.col("status") == "PASSED", 1).otherwise(0)).alias("checks_passed"),
        F.sum(F.when(F.col("status") == "WARNING", 1).otherwise(0)).alias("checks_warning"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("checks_failed"),
        F.sum("records_checked").alias("total_records_checked"),
        F.sum("records_failed").alias("total_records_flagged"),
    )
    .withColumn("load_date", F.lit(load_date))
)

scorecard.write.format("delta").mode("append").save(f"{GOLD}/dq_scorecard")
display(scorecard)

dbutils.notebook.exit("PASSED: DQ report generated.")
