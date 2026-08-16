# Power BI Dashboards — Specification

Three dashboards (`.pbix` report pages) are built on top of the semantic
model described in `data_model.md`, using the DAX measures in
`dax/measures.dax`. Since building this repo outside of Windows/Power BI
Desktop, the `.pbix` binary itself isn't included — instead this spec plus
`preview_*.png` mockups (rendered from the actual pipeline output in
`docs/dashboard_previews/`) document exactly what each page contains so the
report can be rebuilt in Power BI Desktop in minutes by pointing Get Data at
the gold Delta tables and following the layout below.

## Dashboard 1 — Sales Performance

**Purpose:** daily/monthly revenue trend and channel mix, for store ops and
finance stakeholders.

| Visual | Type | Fields |
|---|---|---|
| KPI cards | Card ×4 | Total Revenue, Revenue MoM %, Avg Order Value, Total Transactions |
| Revenue over time | Line chart | `sale_date` (axis) × `Total Revenue` (values), split by `channel` |
| Revenue by channel | Donut chart | `channel` × `Total Revenue` |
| Revenue vs. margin | Combo chart (line + column) | `sale_date` × `Total Revenue` (column) + `Gross Margin %` (line) |
| Top/bottom day filter | Slicer | `sale_date` range |

## Dashboard 2 — Product Trends

**Purpose:** category and product-level performance, for merchandising /
category managers.

| Visual | Type | Fields |
|---|---|---|
| KPI cards | Card ×3 | Product Revenue, Product Units Sold, Revenue per Unique Customer |
| Top 10 products | Bar chart (horizontal) | `product_name` × `Product Revenue`, filtered to Top 10 |
| Revenue by category | Treemap | `category` / `sub_category` × `Product Revenue` |
| Category trend over time | Line chart | `year_month` × `revenue`, split by `category` |
| Category rank table | Table | `category`, `Category Revenue Share %`, `Category Rank by Revenue` |

## Dashboard 3 — Regional Insights

**Purpose:** region-level KPIs across all 11 operating regions, for regional
VPs.

| Visual | Type | Fields |
|---|---|---|
| KPI cards | Card ×3 | Regional Revenue, Regions Above Global Average, Avg Transaction Value by Region |
| Revenue by region | Filled/choropleth-style bar map (or bar chart if geo roles unavailable) | `region` × `Regional Revenue` |
| Region ranking | Bar chart (horizontal, sorted) | `region` × `Regional Revenue`, colored by `Region Revenue Rank` |
| Channel mix by region | 100% stacked bar | `region` (axis) × `revenue` (value), `channel` (legend) |
| Region detail table | Table | `region`, `num_transactions`, `unique_customers`, `Avg Transaction Value by Region`, `Region Revenue Rank` |

## Shared — Data Quality footer page

A 4th lightweight page (linked from all 3 dashboards via a bookmark button)
surfaces `DQ Checks Passed %`, `DQ Records Flagged`, and `DQ Flagged Rate %`
from the `dq_scorecard` gold table, so stakeholders can see pipeline health
alongside the business numbers rather than trusting the data blindly.

## Rebuilding the .pbix

1. Power BI Desktop → **Get Data** → **Azure Data Lake Storage Gen2** →
   point at `gold/` container path (or **Databricks** connector against the
   gold schema if Unity Catalog is enabled).
2. Import `sales_performance_daily`, `product_trends`,
   `category_trends_monthly`, `regional_insights`, `regional_channel_mix`,
   `dq_scorecard`.
3. Model view: add a `Dim_Date` calculated table (`CALENDAR(...)`), mark it
   as the date table, and wire up the relationships shown in `data_model.md`.
4. Paste in the measures from `dax/measures.dax` (create them under a
   dedicated `_Measures` table for cleanliness).
5. Build the 3 pages per the visual tables above.
