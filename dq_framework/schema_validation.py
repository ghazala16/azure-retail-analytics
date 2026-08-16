"""
schema_validation.py
---------------------
A lightweight, reusable PySpark data-quality framework used across the
bronze -> silver stage of the pipeline. It performs:

  1. Schema conformance checks   (required columns present, correct types)
  2. Null / completeness checks  (mandatory fields must not be null)
  3. Referential integrity checks (foreign keys must exist in dimension tables)
  4. Value-range / business-rule checks (no negative quantities or amounts)
  5. Duplicate primary-key checks

Every check produces a structured DQResult that gets written to a DQ log
table (`data/gold/dq_check_results`) so failures are auditable and can be
surfaced in the Power BI "Data Quality" dashboard page.
"""
from dataclasses import dataclass, field
from typing import List, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType


@dataclass
class DQResult:
    check_name: str
    dataset: str
    status: str  # PASSED | FAILED | WARNING
    records_checked: int
    records_failed: int
    details: str = ""

    @property
    def failure_rate_pct(self) -> float:
        if self.records_checked == 0:
            return 0.0
        return round(100 * self.records_failed / self.records_checked, 4)


class SchemaValidator:
    """Runs a battery of schema + data-quality checks against a DataFrame."""

    def __init__(self, dataset_name: str, df: DataFrame):
        self.dataset_name = dataset_name
        self.df = df
        self.results: List[DQResult] = []

    # ---------- 1. Schema conformance ----------
    def check_required_columns(self, required_cols: List[str]) -> DQResult:
        present = set(self.df.columns)
        missing = [c for c in required_cols if c not in present]
        status = "PASSED" if not missing else "FAILED"
        result = DQResult(
            check_name="required_columns_present",
            dataset=self.dataset_name,
            status=status,
            records_checked=len(required_cols),
            records_failed=len(missing),
            details=f"Missing columns: {missing}" if missing else "All required columns present",
        )
        self.results.append(result)
        return result

    def check_column_types(self, expected_types: dict) -> DQResult:
        actual = dict(self.df.dtypes)
        mismatches = []
        for col, expected_type in expected_types.items():
            if col in actual and actual[col] != expected_type:
                mismatches.append(f"{col}: expected {expected_type}, got {actual[col]}")
        status = "PASSED" if not mismatches else "WARNING"
        result = DQResult(
            check_name="column_type_conformance",
            dataset=self.dataset_name,
            status=status,
            records_checked=len(expected_types),
            records_failed=len(mismatches),
            details="; ".join(mismatches) if mismatches else "All types conform",
        )
        self.results.append(result)
        return result

    # ---------- 2. Completeness ----------
    def check_not_null(self, mandatory_cols: List[str]) -> DQResult:
        total = self.df.count()
        null_condition = None
        for c in mandatory_cols:
            cond = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
            null_condition = cond if null_condition is None else (null_condition | cond)
        failed = self.df.filter(null_condition).count() if null_condition is not None else 0
        status = "PASSED" if failed == 0 else ("WARNING" if failed / max(total, 1) < 0.02 else "FAILED")
        result = DQResult(
            check_name="not_null_mandatory_fields",
            dataset=self.dataset_name,
            status=status,
            records_checked=total,
            records_failed=failed,
            details=f"Null/blank values found in one of {mandatory_cols}",
        )
        self.results.append(result)
        return result

    # ---------- 3. Referential integrity ----------
    def check_foreign_key(self, fk_col: str, ref_df: DataFrame, ref_col: str) -> DQResult:
        total = self.df.count()
        ref_values = ref_df.select(F.col(ref_col).alias("_ref")).distinct()
        orphans = (
            self.df.select(fk_col)
            .join(ref_values, self.df[fk_col] == ref_values["_ref"], "left_anti")
            .count()
        )
        status = "PASSED" if orphans == 0 else ("WARNING" if orphans / max(total, 1) < 0.02 else "FAILED")
        result = DQResult(
            check_name=f"referential_integrity_{fk_col}",
            dataset=self.dataset_name,
            status=status,
            records_checked=total,
            records_failed=orphans,
            details=f"{orphans} rows have {fk_col} values not present in reference table",
        )
        self.results.append(result)
        return result

    # ---------- 4. Business rules ----------
    def check_non_negative(self, numeric_cols: List[str]) -> DQResult:
        total = self.df.count()
        cond = None
        for c in numeric_cols:
            # NaN (as opposed to SQL NULL) sorts as "greater than everything" in
            # Spark, so `col < 0` silently lets NaNs through undetected. Flag
            # NaNs explicitly alongside true negatives.
            c_cond = (F.col(c) < 0) | F.isnan(F.col(c).cast("double"))
            cond = c_cond if cond is None else (cond | c_cond)
        failed = self.df.filter(cond).count() if cond is not None else 0
        status = "PASSED" if failed == 0 else "WARNING"
        result = DQResult(
            check_name="non_negative_values",
            dataset=self.dataset_name,
            status=status,
            records_checked=total,
            records_failed=failed,
            details=f"Negative values found in one of {numeric_cols}",
        )
        self.results.append(result)
        return result

    # ---------- 5. Duplicates ----------
    def check_duplicate_keys(self, key_cols: List[str]) -> DQResult:
        total = self.df.count()
        distinct = self.df.select(*key_cols).distinct().count()
        dupes = total - distinct
        status = "PASSED" if dupes == 0 else "WARNING"
        result = DQResult(
            check_name="duplicate_primary_key",
            dataset=self.dataset_name,
            status=status,
            records_checked=total,
            records_failed=dupes,
            details=f"{dupes} duplicate rows on key(s) {key_cols}",
        )
        self.results.append(result)
        return result

    def results_as_dataframe(self, spark) -> DataFrame:
        rows = [
            (r.check_name, r.dataset, r.status, r.records_checked, r.records_failed, r.failure_rate_pct, r.details)
            for r in self.results
        ]
        cols = ["check_name", "dataset", "status", "records_checked", "records_failed", "failure_rate_pct", "details"]
        return spark.createDataFrame(rows, cols)
