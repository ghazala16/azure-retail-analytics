"""
tests/test_schema_validation.py
---------------------------------
Unit tests for the reusable DQ framework in dq_framework/schema_validation.py.
Uses small in-memory Spark DataFrames so the suite runs in seconds without
touching the full 500K-row dataset.
"""
import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "dq_framework"))
from schema_validation import SchemaValidator  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("dq-framework-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sales_df(spark):
    data = [
        ("TXN-1", "STR-1", "PRD-1", 2, 50.0),
        ("TXN-2", "STR-1", "PRD-2", 1, 20.0),
        ("TXN-3", "STR-2", "PRD-1", 3, 30.0),
        ("TXN-3", "STR-2", "PRD-1", 3, 35.0),  # duplicate transaction_id, different amount
        ("TXN-4", None, "PRD-1", 1, 10.0),  # null store_id
        ("TXN-5", "STR-1", "PRD-99", -2, 15.0),  # negative qty + orphan product
    ]
    cols = ["transaction_id", "store_id", "product_id", "quantity", "total_amount"]
    return spark.createDataFrame(data, cols)


@pytest.fixture
def store_ref_df(spark):
    return spark.createDataFrame([("STR-1",), ("STR-2",)], ["store_id"])


@pytest.fixture
def product_ref_df(spark):
    return spark.createDataFrame([("PRD-1",), ("PRD-2",)], ["product_id"])


class TestRequiredColumns:
    def test_all_present_passes(self, sales_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_required_columns(["transaction_id", "store_id", "quantity"])
        assert result.status == "PASSED"
        assert result.records_failed == 0

    def test_missing_column_fails(self, sales_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_required_columns(["transaction_id", "region"])
        assert result.status == "FAILED"
        assert "region" in result.details


class TestNotNull:
    def test_flags_null_store_id(self, sales_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_not_null(["transaction_id", "store_id"])
        assert result.records_failed == 1  # the TXN-4 row
        assert result.status in ("WARNING", "FAILED")

    def test_no_nulls_passes(self, sales_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_not_null(["transaction_id"])
        assert result.status == "PASSED"
        assert result.records_failed == 0


class TestForeignKey:
    def test_detects_orphan_store_id(self, sales_df, store_ref_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_foreign_key("store_id", store_ref_df, "store_id")
        # the null store_id row counts as an orphan (left_anti join)
        assert result.records_failed >= 1

    def test_detects_orphan_product_id(self, sales_df, product_ref_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_foreign_key("product_id", product_ref_df, "product_id")
        assert result.records_failed == 1  # PRD-99


class TestNonNegative:
    def test_detects_negative_quantity(self, sales_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_non_negative(["quantity"])
        assert result.records_failed == 1
        assert result.status == "WARNING"

    def test_no_negatives_passes(self, sales_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_non_negative(["total_amount"])
        assert result.status == "PASSED"


class TestDuplicateKeys:
    def test_detects_duplicate_transaction_id(self, sales_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_duplicate_keys(["transaction_id"])
        # 6 rows, 5 distinct transaction_ids -> 1 duplicate
        assert result.records_failed == 1
        assert result.status == "WARNING"

    def test_composite_key_no_duplicates(self, sales_df):
        sv = SchemaValidator("sales", sales_df)
        # transaction_id alone has 1 duplicate (TXN-3), but paired with the
        # differing total_amount the composite key is unique per row.
        result = sv.check_duplicate_keys(["transaction_id", "total_amount"])
        assert result.status == "PASSED"


class TestResultsAggregation:
    def test_results_as_dataframe_row_count(self, spark, sales_df, store_ref_df, product_ref_df):
        sv = SchemaValidator("sales", sales_df)
        sv.check_required_columns(["transaction_id"])
        sv.check_not_null(["transaction_id"])
        sv.check_foreign_key("store_id", store_ref_df, "store_id")
        sv.check_non_negative(["quantity"])
        sv.check_duplicate_keys(["transaction_id"])

        result_df = sv.results_as_dataframe(spark)
        assert result_df.count() == 5
        assert set(result_df.columns) == {
            "check_name", "dataset", "status", "records_checked",
            "records_failed", "failure_rate_pct", "details",
        }

    def test_failure_rate_pct_calculation(self, sales_df):
        sv = SchemaValidator("sales", sales_df)
        result = sv.check_duplicate_keys(["transaction_id"])
        expected_rate = round(100 * result.records_failed / result.records_checked, 4)
        assert result.failure_rate_pct == expected_rate
