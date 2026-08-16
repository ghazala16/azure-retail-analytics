"""
tests/test_pipeline_integrity.py
-----------------------------------
Higher-level tests that validate the shape and integrity of the generated
source datasets and the silver/gold outputs produced by
scripts/run_local_pipeline.py. These are "does the deliverable match the
resume metrics" tests as much as unit tests.
"""
import csv
import os

import pytest

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
RAW = os.path.join(REPO_ROOT, "data", "raw")


def count_csv_rows(path):
    with open(path) as f:
        return sum(1 for _ in f) - 1  # minus header


class TestSourceDataMetrics:
    def test_four_source_datasets_exist(self):
        expected = ["store_sales.csv", "online_sales.csv", "product_catalog.csv", "store_master.csv"]
        for name in expected:
            assert os.path.exists(os.path.join(RAW, name)), f"missing source dataset: {name}"

    def test_combined_sales_records_exceed_500k(self):
        store_rows = count_csv_rows(os.path.join(RAW, "store_sales.csv"))
        online_rows = count_csv_rows(os.path.join(RAW, "online_sales.csv"))
        assert store_rows + online_rows >= 500_000

    def test_store_master_covers_10_plus_regions(self):
        with open(os.path.join(RAW, "store_master.csv")) as f:
            reader = csv.DictReader(f)
            regions = {row["region"] for row in reader}
        assert len(regions) >= 10

    def test_product_catalog_has_categories(self):
        with open(os.path.join(RAW, "product_catalog.csv")) as f:
            reader = csv.DictReader(f)
            categories = {row["category"] for row in reader if row["category"]}
        assert len(categories) >= 3


@pytest.mark.skipif(
    not os.path.exists(os.path.join(REPO_ROOT, "docs", "pipeline_metrics.json")),
    reason="run `python scripts/run_local_pipeline.py` first to generate pipeline output",
)
class TestPipelineOutputMetrics:
    def _load_metrics(self):
        import json
        with open(os.path.join(REPO_ROOT, "docs", "pipeline_metrics.json")) as f:
            return json.load(f)

    def test_four_datasets_integrated(self):
        assert self._load_metrics()["source_datasets_integrated"] == 4

    def test_regions_covered_10_plus(self):
        assert self._load_metrics()["regions_covered"] >= 10

    def test_raw_sales_records_500k_plus(self):
        assert self._load_metrics()["raw_sales_records"] >= 500_000

    def test_dq_quarantine_rate_is_reasonable(self):
        """Sanity check: the DQ layer shouldn't be silently dropping a large
        fraction of legitimate data -- quarantine rate should stay well
        under 5% given ~1.5% of source rows are deliberately dirty."""
        metrics = self._load_metrics()
        rate = metrics["records_quarantined_by_dq"] / metrics["raw_sales_records"]
        assert rate < 0.05
