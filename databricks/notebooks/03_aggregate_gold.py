# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Aggregate: Silver -> Gold (Delta Lake, Power BI-ready)
# MAGIC Builds the analytical, pre-aggregated Delta datasets that back the 3 Power BI
# MAGIC dashboards: **Sales Performance**, **Product Trends**, and **Regional Insights**.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("loadDate", "")
load_date = dbutils.widgets.get("loadDate")

SILVER = "abfss://retail-lake@retailsalesadls.dfs.core.windows.net/silver"
GOLD = "abfss://retail-lake@retailsalesadls.dfs.core.windows.net/gold"

sales_unified = spark.read.format("delta").load(f"{SILVER}/sales_unified")
product_catalog = spark.read.format("delta").load(f"{SILVER}/product_catalog")
store_master = spark.read.format("delta").load(f"{SILVER}/store_master")

sales_enriched = sales_unified.join(
    product_catalog.select("product_id", "product_name", "category", "sub_category", "unit_cost"),
    on="product_id", how="left"
)

# COMMAND ----------
# MAGIC %md ### Gold 1 - Sales Performance (daily / monthly grain, by channel & region)

# COMMAND ----------
sales_performance_daily = (
    sales_enriched
    .groupBy("sale_date", "region", "channel")
    .agg(
        F.countDistinct("sale_id").alias("num_transactions"),
        F.sum("quantity").alias("units_sold"),
        F.sum("total_amount").alias("gross_revenue"),
        F.sum(F.col("quantity") * F.col("unit_cost")).alias("total_cost"),
    )
    .withColumn("gross_margin", F.round(F.col("gross_revenue") - F.col("total_cost"), 2))
    .withColumn("avg_order_value", F.round(F.col("gross_revenue") / F.col("num_transactions"), 2))
)

sales_performance_monthly = (
    sales_enriched
    .withColumn("year_month", F.date_format("sale_date", "yyyy-MM"))
    .groupBy("year_month", "region", "channel")
    .agg(
        F.countDistinct("sale_id").alias("num_transactions"),
        F.sum("quantity").alias("units_sold"),
        F.sum("total_amount").alias("gross_revenue"),
    )
)

# COMMAND ----------
# MAGIC %md ### Gold 2 - Product Trends (category / product performance & ranking)

# COMMAND ----------
product_trends = (
    sales_enriched
    .groupBy("category", "sub_category", "product_id", "product_name")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("total_amount").alias("revenue"),
        F.countDistinct("sale_id").alias("num_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn("rank_in_category", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"]).Window
        .partitionBy("category").orderBy(F.desc("revenue"))
    ))
)

category_trends_monthly = (
    sales_enriched
    .withColumn("year_month", F.date_format("sale_date", "yyyy-MM"))
    .groupBy("year_month", "category")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("total_amount").alias("revenue"),
    )
)

# COMMAND ----------
# MAGIC %md ### Gold 3 - Regional Insights (region-level KPIs across 10+ regions)

# COMMAND ----------
regional_insights = (
    sales_enriched
    .groupBy("region")
    .agg(
        F.countDistinct("sale_id").alias("num_transactions"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("units_sold"),
        F.sum("total_amount").alias("gross_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
    )
    .withColumn("revenue_rank", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"]).Window.orderBy(F.desc("gross_revenue"))
    ))
)

regional_channel_mix = (
    sales_enriched.groupBy("region", "channel")
    .agg(F.sum("total_amount").alias("revenue"), F.countDistinct("sale_id").alias("num_transactions"))
)

# COMMAND ----------
# MAGIC %md ### Write gold Delta tables (Power BI import / DirectQuery source)

# COMMAND ----------
sales_performance_daily.write.format("delta").mode("overwrite").save(f"{GOLD}/sales_performance_daily")
sales_performance_monthly.write.format("delta").mode("overwrite").save(f"{GOLD}/sales_performance_monthly")
product_trends.write.format("delta").mode("overwrite").save(f"{GOLD}/product_trends")
category_trends_monthly.write.format("delta").mode("overwrite").save(f"{GOLD}/category_trends_monthly")
regional_insights.write.format("delta").mode("overwrite").save(f"{GOLD}/regional_insights")
regional_channel_mix.write.format("delta").mode("overwrite").save(f"{GOLD}/regional_channel_mix")

print(f"Regions covered: {regional_insights.count()}")
print(f"Gold sales_performance_daily rows: {sales_performance_daily.count()}")

dbutils.notebook.exit("PASSED: gold layer written.")
