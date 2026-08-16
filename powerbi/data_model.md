# Power BI Data Model

This project's Power BI report connects to the **gold** Delta Lake tables
(via the Databricks SQL / Delta connector, or Power BI's ADLS Gen2 connector
pointed at `gold/*`) and builds a small star-schema semantic model on top of
them.

## Source tables (gold layer)

| Delta table                 | Grain                                    | Used by dashboard(s)            |
|------------------------------|-------------------------------------------|----------------------------------|
| `sales_performance_daily`    | 1 row per (date, region, channel)         | Sales Performance                |
| `product_trends`             | 1 row per product                         | Product Trends                   |
| `category_trends_monthly`    | 1 row per (month, category)               | Product Trends                   |
| `regional_insights`          | 1 row per region (10+ regions)            | Regional Insights                |
| `regional_channel_mix`       | 1 row per (region, channel)               | Regional Insights                |
| `dq_scorecard`                | 1 row per pipeline run                    | Data Quality (all dashboards)    |

## Star schema

```
                     ┌────────────────────┐
                     │   Dim_Date          │
                     │  date (PK)          │
                     │  year, month,       │
                     │  year_month, ...    │
                     └─────────┬───────────┘
                               │ 1:*
┌──────────────────┐   ┌───────┴────────────────┐   ┌─────────────────────┐
│  Dim_Region       │*1 │ Fact_SalesPerformance   │1* │  Dim_Channel         │
│  region (PK)      ├───┤ date, region, channel   ├───┤  channel (PK)        │
│  ... from          │   │ num_transactions,       │   │  (In-Store / Online) │
│  regional_insights │   │ units_sold,             │   └─────────────────────┘
└──────────┬─────────┘   │ gross_revenue,          │
           │             │ gross_margin,           │
           │ 1:*         │ avg_order_value         │
┌──────────┴─────────┐   └─────────────────────────┘
│  Fact_ProductTrends │
│  product_id (PK)    │
│  category,          │
│  sub_category,      │
│  units_sold, revenue│
└─────────────────────┘
```

* **Dim_Date** is a standard auto-generated date table (`CALENDAR(MIN(...), MAX(...))`)
  marked as the model's date table, used for time intelligence (MTD/YTD/YoY).
* **Dim_Region** is built from `regional_insights` (11 real regions + `revenue_rank`).
* **Dim_Channel** is a 2-row table (`In-Store`, `Online`).
* Relationships are single-direction, star-schema (fact tables many-to-one to dims),
  which keeps DAX filter propagation predictable and the model fast at 500K+ row scale.

## Import mode

`sales_performance_daily` (~365 days × 11 regions × 2 channels ≈ 8K rows) and
`product_trends` (1,200 rows) are small enough for **Import** mode, which is
what's used here for the fastest report interactivity. For a production
deployment where gold tables are refreshed hourly instead of daily, swapping
to **DirectQuery** against the Databricks SQL warehouse would be the next
optimization (documented as a stretch goal in the main README).
