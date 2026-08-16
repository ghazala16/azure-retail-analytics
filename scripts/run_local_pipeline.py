"""
run_local_pipeline.py
-----------------------
Runs the full bronze -> silver -> gold pipeline locally against the files in
data/raw/ using PySpark + Delta Lake, so the project can be built, tested,
and demoed without an actual Azure subscription. In production this same
transformation logic runs inside Azure Databricks, orchestrated by the ADF
pipelines in /adf, reading from / writing to ADLS Gen2 instead of local
disk (see databricks/notebooks/ for the notebook versions of this logic).

Usage:
    python scripts/run_local_pipeline.py
"""
import json
import os
import shutil
import sys
import time

from deltalake import write_deltalake
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DoubleType, IntegerType

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "dq_framework"))
from schema_validation import SchemaValidator  # noqa: E402

RAW = "data/raw"
BRONZE = "data/bronze"
SILVER = "data/silver"
GOLD = "data/gold"


def get_spark():
    # NOTE: PySpark drives all cleansing/normalization/aggregation transformations,
    # exactly as they run in Azure Databricks. Delta Lake tables themselves are
    # written with the `deltalake` (delta-rs) engine, which produces a genuine
    # Delta Lake table (Parquet data files + _delta_log transaction log) without
    # requiring a Maven artifact download for the Spark Delta connector - this
    # keeps the local/CI build fully offline-reproducible. In the Databricks
    # notebooks (databricks/notebooks/*.py) the native `.write.format("delta")`
    # Spark API is used directly, since the Delta connector ships with the
    # Databricks runtime.
    builder = (
        SparkSession.builder.appName("RetailSalesAnalyticsPlatform")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
    )
    return builder.getOrCreate()


def write_delta(spark, df, path, partition_by=None):
    """Write a Spark DataFrame out as a real Delta Lake table via delta-rs."""
    pdf = df.toPandas()
    if os.path.exists(path):
        shutil.rmtree(path)
    write_deltalake(path, pdf, mode="overwrite", partition_by=partition_by)


def read_delta(spark, path):
    """Read a Delta Lake table (written via delta-rs) back into a Spark DataFrame."""
    from deltalake import DeltaTable
    pdf = DeltaTable(path).to_pandas()
    return spark.createDataFrame(pdf)


def step_bronze_ingest(spark):
    """Simulates the ADF copy activities landing raw files into bronze."""
    print("\n[1/4] Bronze ingestion (simulating ADF Copy activities) ...")
    for name in ["store_sales", "online_sales", "product_catalog", "store_master"]:
        df = spark.read.option("header", True).option("inferSchema", True).csv(f"{RAW}/{name}.csv")
        out = f"{BRONZE}/{name}"
        write_delta(spark, df, out)
        print(f"    -> {name}: {df.count():,} rows landed in bronze")


def step_schema_validation(spark):
    """Runs the DQ framework against bronze datasets before promoting to silver."""
    print("\n[2/4] Schema validation & data quality checks ...")
    store_sales = read_delta(spark, f"{BRONZE}/store_sales")
    online_sales = read_delta(spark, f"{BRONZE}/online_sales")
    product_catalog = read_delta(spark, f"{BRONZE}/product_catalog")
    store_master = read_delta(spark, f"{BRONZE}/store_master")

    all_results = []

    sv = SchemaValidator("store_sales", store_sales)
    sv.check_required_columns(["transaction_id", "store_id", "product_id", "quantity", "total_amount", "transaction_ts"])
    sv.check_not_null(["transaction_id", "store_id", "product_id"])
    sv.check_foreign_key("store_id", store_master, "store_id")
    sv.check_foreign_key("product_id", product_catalog, "product_id")
    sv.check_non_negative(["quantity"])
    sv.check_duplicate_keys(["transaction_id"])
    all_results.append(sv)

    sv2 = SchemaValidator("online_sales", online_sales)
    sv2.check_required_columns(["order_id", "product_id", "quantity", "order_total", "shipping_region"])
    sv2.check_not_null(["order_id", "product_id"])
    sv2.check_foreign_key("product_id", product_catalog, "product_id")
    sv2.check_duplicate_keys(["order_id"])
    all_results.append(sv2)

    from functools import reduce
    result_dfs = [s.results_as_dataframe(spark) for s in all_results]
    dq_log_df = reduce(lambda a, b: a.unionByName(b), result_dfs)

    out = f"{GOLD}/dq_check_results"
    write_delta(spark, dq_log_df, out)

    dq_log_df.orderBy("dataset", "check_name").show(50, truncate=False)

    return dq_log_df


def step_silver_cleanse(spark):
    print("\n[3/4] Cleansing, normalization -> Silver Delta tables ...")
    store_sales_raw = read_delta(spark, f"{BRONZE}/store_sales")
    online_sales_raw = read_delta(spark, f"{BRONZE}/online_sales")
    product_catalog_raw = read_delta(spark, f"{BRONZE}/product_catalog")
    store_master_raw = read_delta(spark, f"{BRONZE}/store_master")

    raw_store_count = store_sales_raw.count()
    raw_online_count = online_sales_raw.count()

    store_sales_silver = (
        store_sales_raw
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("discount_pct", F.col("discount_pct").cast(DoubleType()))
        .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
        .withColumn("sale_date", F.to_date("transaction_ts"))
        .withColumn("channel", F.lit("In-Store"))
        .filter(F.col("transaction_id").isNotNull())
        .filter((F.col("quantity").isNull()) | (F.col("quantity") > 0))
        # NOTE: Spark's CSV inferSchema represents a blank numeric field as NaN,
        # not SQL NULL, and NaN behaves specially in comparisons (NaN >= 0 is
        # TRUE, and SUM() over any NaN poisons the whole aggregate). Every
        # "is missing" check below must test isNull() OR isnan() together.
        .withColumn(
            "total_amount",
            F.when(F.col("total_amount").isNull() | F.isnan(F.col("total_amount")),
                   F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2))
             .otherwise(F.col("total_amount"))
        )
        .filter(F.col("total_amount").isNotNull() & (~F.isnan(F.col("total_amount"))) & (F.col("total_amount") >= 0))
        .filter(F.col("transaction_ts").isNotNull())
        .filter(F.col("store_id") != "STR-9999")  # drop orphan FK rows
        .dropDuplicates(["transaction_id"])
    )

    online_sales_silver = (
        online_sales_raw
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("order_total", F.col("order_total").cast(DoubleType()))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("sale_date", F.to_date("order_ts"))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("shipping_region").isNotNull() & (F.trim(F.col("shipping_region")) != ""))
        .filter((F.col("quantity").isNotNull()) & (F.col("quantity") > 0))
        .filter(F.col("order_total").isNotNull() & (~F.isnan(F.col("order_total"))) & (F.col("order_total") >= 0))
        .filter(F.col("product_id") != "PRD-999999")  # drop orphan FK rows
        .dropDuplicates(["order_id"])
    )

    product_catalog_silver = (
        product_catalog_raw
        .withColumn("unit_cost", F.col("unit_cost").cast(DoubleType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("category", F.initcap(F.trim(F.col("category"))))
        .filter(F.col("unit_price").isNotNull() & (~F.isnan(F.col("unit_price"))))
        .dropDuplicates(["product_id"])
    )

    store_master_silver = (
        store_master_raw
        .withColumn("region", F.trim(F.col("region")))
        .dropDuplicates(["store_id"])
    )

    store_leg = store_sales_silver.select(
        F.col("transaction_id").alias("sale_id"), "store_id", "product_id", "quantity",
        "unit_price", "total_amount", "sale_date", "channel", "customer_id",
    ).join(store_master_silver.select("store_id", "region"), on="store_id", how="left")

    online_leg = online_sales_silver.select(
        F.col("order_id").alias("sale_id"),
        F.lit(None).cast("string").alias("store_id"),
        "product_id", "quantity", "unit_price",
        F.col("order_total").alias("total_amount"),
        "sale_date", F.lit("Online").alias("channel"), "customer_id",
        F.col("shipping_region").alias("region"),
    )

    sales_unified = store_leg.unionByName(online_leg)

    # Final guard: any stray nulls/blank markers that survived upstream cleansing
    # should never reach the unified fact table region-less. In practice a few
    # rows may fall through if the shipping_region source system truly sent a
    # bad value that only manifests after the join. Treat these as
    # DQ-quarantined and drop them rather than let them corrupt regional KPIs.
    sales_unified = sales_unified.filter(
        F.col("region").isNotNull()
        & (F.trim(F.col("region")) != "")
        & (F.col("region") != "NaN")
    )

    for name, df in [
        ("store_sales", store_sales_silver), ("online_sales", online_sales_silver),
        ("product_catalog", product_catalog_silver), ("store_master", store_master_silver),
        ("sales_unified", sales_unified),
    ]:
        out = f"{SILVER}/{name}"
        write_delta(spark, df, out)

    print(f"    -> store_sales:  {raw_store_count:,} raw -> {store_sales_silver.count():,} clean "
          f"({raw_store_count - store_sales_silver.count():,} rows quarantined)")
    print(f"    -> online_sales: {raw_online_count:,} raw -> {online_sales_silver.count():,} clean "
          f"({raw_online_count - online_sales_silver.count():,} rows quarantined)")
    print(f"    -> sales_unified: {sales_unified.count():,} combined rows")

    return sales_unified.count(), raw_store_count + raw_online_count


def step_gold_aggregate(spark):
    print("\n[4/4] Aggregating -> Gold Delta datasets (Power BI-ready) ...")
    sales_unified = read_delta(spark, f"{SILVER}/sales_unified")
    product_catalog = read_delta(spark, f"{SILVER}/product_catalog")

    sales_enriched = sales_unified.join(
        product_catalog.select("product_id", "product_name", "category", "sub_category", "unit_cost"),
        on="product_id", how="left"
    )

    sales_performance_daily = (
        sales_enriched.groupBy("sale_date", "region", "channel")
        .agg(
            F.countDistinct("sale_id").alias("num_transactions"),
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("gross_revenue"),
            F.sum(F.col("quantity") * F.col("unit_cost")).alias("total_cost"),
        )
        .withColumn("gross_margin", F.round(F.col("gross_revenue") - F.col("total_cost"), 2))
        .withColumn("avg_order_value", F.round(F.col("gross_revenue") / F.col("num_transactions"), 2))
    )

    product_trends = (
        sales_enriched.groupBy("category", "sub_category", "product_id", "product_name")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("revenue"),
            F.countDistinct("sale_id").alias("num_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("rank_in_category", F.row_number().over(Window.partitionBy("category").orderBy(F.desc("revenue"))))
    )

    regional_insights = (
        sales_enriched.groupBy("region")
        .agg(
            F.countDistinct("sale_id").alias("num_transactions"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("gross_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
        )
        .withColumn("revenue_rank", F.row_number().over(Window.orderBy(F.desc("gross_revenue"))))
    )

    for name, df in [
        ("sales_performance_daily", sales_performance_daily),
        ("product_trends", product_trends),
        ("regional_insights", regional_insights),
    ]:
        out = f"{GOLD}/{name}"
        write_delta(spark, df, out)

    num_regions = regional_insights.count()
    print(f"    -> regional_insights: {num_regions} regions")
    print(f"    -> product_trends: {product_trends.count():,} products")
    print(f"    -> sales_performance_daily: {sales_performance_daily.count():,} rows")

    regional_insights.orderBy(F.desc("gross_revenue")).show(20, truncate=False)

    return num_regions


def main():
    t0 = time.time()
    for d in [BRONZE, SILVER, GOLD]:
        os.makedirs(d, exist_ok=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    raw_counts = {}
    for name in ["store_sales", "online_sales", "product_catalog", "store_master"]:
        with open(f"{RAW}/{name}.csv") as f:
            raw_counts[name] = sum(1 for _ in f) - 1

    step_bronze_ingest(spark)
    dq_log_df = step_schema_validation(spark)
    clean_rows, raw_txn_rows = step_silver_cleanse(spark)
    num_regions = step_gold_aggregate(spark)

    metrics = {
        "source_datasets_integrated": 4,
        "raw_sales_records": raw_counts["store_sales"] + raw_counts["online_sales"],
        "raw_store_sales_records": raw_counts["store_sales"],
        "raw_online_sales_records": raw_counts["online_sales"],
        "clean_sales_records_silver": clean_rows,
        "records_quarantined_by_dq": raw_txn_rows - clean_rows,
        "regions_covered": num_regions,
        "total_dq_checks_run": dq_log_df.count(),
        "runtime_seconds": round(time.time() - t0, 1),
    }

    with open("docs/pipeline_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print("\n=== PIPELINE METRICS ===")
    print(json.dumps(metrics, indent=2))

    spark.stop()


if __name__ == "__main__":
    main()
