# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Schema Validation
# MAGIC Reads the 4 raw retail source datasets landed in the ADLS Gen2 **bronze** zone
# MAGIC by Azure Data Factory and runs the schema/DQ validation framework against each
# MAGIC one before allowing the pipeline to proceed to the silver stage.

# COMMAND ----------
import sys
sys.path.append("/Workspace/Shared/retail-analytics/dq_framework")
from schema_validation import SchemaValidator

dbutils.widgets.text("loadDate", "")
load_date = dbutils.widgets.get("loadDate")

BRONZE_PATH = "abfss://retail-lake@retailsalesadls.dfs.core.windows.net/bronze"

# COMMAND ----------
store_sales = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{BRONZE_PATH}/store_sales/{load_date}")

online_sales = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{BRONZE_PATH}/online_sales/{load_date}")

product_catalog = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{BRONZE_PATH}/product_catalog/{load_date}")

store_master = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{BRONZE_PATH}/store_master/{load_date}")

# COMMAND ----------
# MAGIC %md ### Run validations per dataset

# COMMAND ----------
all_results = []

sv = SchemaValidator("store_sales", store_sales)
sv.check_required_columns(["transaction_id", "store_id", "product_id", "quantity", "total_amount", "transaction_ts"])
sv.check_not_null(["transaction_id", "store_id", "product_id"])
sv.check_foreign_key("store_id", store_master, "store_id")
sv.check_foreign_key("product_id", product_catalog, "product_id")
sv.check_non_negative(["quantity", "total_amount"])
sv.check_duplicate_keys(["transaction_id"])
all_results.append(sv)

sv2 = SchemaValidator("online_sales", online_sales)
sv2.check_required_columns(["order_id", "product_id", "quantity", "order_total", "shipping_region"])
sv2.check_not_null(["order_id", "product_id", "shipping_region"])
sv2.check_foreign_key("product_id", product_catalog, "product_id")
sv2.check_non_negative(["quantity", "order_total"])
sv2.check_duplicate_keys(["order_id"])
all_results.append(sv2)

# COMMAND ----------
# MAGIC %md ### Persist DQ results and fail the activity if any dataset has a FAILED check

# COMMAND ----------
from functools import reduce

result_dfs = [s.results_as_dataframe(spark) for s in all_results]
dq_log_df = reduce(lambda a, b: a.unionByName(b), result_dfs)

dq_log_df.write.format("delta").mode("append") \
    .save("abfss://retail-lake@retailsalesadls.dfs.core.windows.net/gold/dq_check_results")

failed_checks = dq_log_df.filter("status = 'FAILED'").count()
if failed_checks > 0:
    dbutils.notebook.exit(f"FAILED: {failed_checks} hard schema check(s) failed. See dq_check_results.")
else:
    dbutils.notebook.exit("PASSED: bronze schema validation complete.")
