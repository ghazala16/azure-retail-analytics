# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Cleanse & Normalize: Bronze -> Silver (Delta Lake)
# MAGIC Applies PySpark transformations to the 4 raw retail datasets: type casting,
# MAGIC null/negative-value handling, timestamp normalization, deduplication, and
# MAGIC region/category standardization. Writes conformed Delta tables to the
# MAGIC silver zone.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

dbutils.widgets.text("loadDate", "")
load_date = dbutils.widgets.get("loadDate")

BRONZE = "abfss://retail-lake@retailsalesadls.dfs.core.windows.net/bronze"
SILVER = "abfss://retail-lake@retailsalesadls.dfs.core.windows.net/silver"

# COMMAND ----------
# MAGIC %md ### Store sales - cleanse

# COMMAND ----------
store_sales_raw = spark.read.option("header", True).csv(f"{BRONZE}/store_sales/{load_date}")

store_sales_silver = (
    store_sales_raw
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    .withColumn("discount_pct", F.col("discount_pct").cast(DoubleType()))
    .withColumn("transaction_ts", F.to_timestamp("transaction_ts", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("sale_date", F.to_date("transaction_ts"))
    .withColumn("channel", F.lit("In-Store"))
    # data-quality remediation: drop hard-invalid rows, recompute total when missing
    .filter(F.col("transaction_id").isNotNull())
    .filter((F.col("quantity").isNull()) | (F.col("quantity") > 0))
    .withColumn(
        "total_amount",
        F.when(F.col("total_amount").isNull(), F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2))
         .otherwise(F.col("total_amount"))
    )
    .filter(F.col("total_amount").isNotNull() & (F.col("total_amount") >= 0))
    .filter(F.col("transaction_ts").isNotNull())
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------
# MAGIC %md ### Online sales - cleanse

# COMMAND ----------
online_sales_raw = spark.read.option("header", True).csv(f"{BRONZE}/online_sales/{load_date}")

online_sales_silver = (
    online_sales_raw
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    .withColumn("order_total", F.col("order_total").cast(DoubleType()))
    .withColumn("order_ts", F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("sale_date", F.to_date("order_ts"))
    .withColumnRenamed("channel", "channel")
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("shipping_region").isNotNull() & (F.trim(F.col("shipping_region")) != ""))
    .filter((F.col("quantity").isNotNull()) & (F.col("quantity") > 0))
    .filter((F.col("order_total").isNotNull()) & (F.col("order_total") >= 0))
    .dropDuplicates(["order_id"])
)

# COMMAND ----------
# MAGIC %md ### Product catalog & store master - normalize

# COMMAND ----------
product_catalog_silver = (
    spark.read.option("header", True).csv(f"{BRONZE}/product_catalog/{load_date}")
    .withColumn("unit_cost", F.col("unit_cost").cast(DoubleType()))
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .filter(F.col("unit_price").isNotNull())
    .dropDuplicates(["product_id"])
)

store_master_silver = (
    spark.read.option("header", True).csv(f"{BRONZE}/store_master/{load_date}")
    .withColumn("region", F.trim(F.col("region")))
    .dropDuplicates(["store_id"])
)

# COMMAND ----------
# MAGIC %md ### Unify store + online sales into one conformed `sales_unified` silver table
# MAGIC A single normalized fact grain (one row per line-item sale, either channel)
# MAGIC makes downstream aggregation and Power BI modeling much simpler.

# COMMAND ----------
sales_unified = store_sales_silver.select(
    F.col("transaction_id").alias("sale_id"),
    "store_id", "product_id", "quantity", "unit_price", "total_amount",
    "sale_date", "channel", "customer_id",
).join(
    store_master_silver.select("store_id", "region"), on="store_id", how="left"
).unionByName(
    online_sales_silver.select(
        F.col("order_id").alias("sale_id"),
        F.lit(None).cast("string").alias("store_id"),
        "product_id", "quantity", "unit_price",
        F.col("order_total").alias("total_amount"),
        "sale_date",
        F.col("channel"),
        "customer_id",
    ).join(
        online_sales_silver.select("order_id", "shipping_region").withColumnRenamed("order_id", "_oid"),
        on=F.col("sale_id") == F.col("_oid"), how="left"
    ).withColumnRenamed("shipping_region", "region").drop("_oid")
)

# COMMAND ----------
# MAGIC %md ### Write silver Delta tables

# COMMAND ----------
store_sales_silver.write.format("delta").mode("overwrite").save(f"{SILVER}/store_sales")
online_sales_silver.write.format("delta").mode("overwrite").save(f"{SILVER}/online_sales")
product_catalog_silver.write.format("delta").mode("overwrite").save(f"{SILVER}/product_catalog")
store_master_silver.write.format("delta").mode("overwrite").save(f"{SILVER}/store_master")
sales_unified.write.format("delta").mode("overwrite").partitionBy("sale_date").save(f"{SILVER}/sales_unified")

print(f"Silver row counts -> store_sales: {store_sales_silver.count()}, "
      f"online_sales: {online_sales_silver.count()}, "
      f"sales_unified: {sales_unified.count()}")

dbutils.notebook.exit("PASSED: silver layer written.")
