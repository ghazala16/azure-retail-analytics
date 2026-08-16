"""
generate_dashboard_previews.py
--------------------------------
Renders static PNG previews of the 3 Power BI dashboards directly from the
real gold-layer Delta tables produced by scripts/run_local_pipeline.py, so
the repo has visual proof of the pipeline output without requiring Power BI
Desktop / Windows to open a .pbix file. These are matplotlib mockups, not a
substitute for the interactive report described in powerbi/dashboard_specs.md.
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from deltalake import DeltaTable

OUT_DIR = "docs/dashboard_previews"
os.makedirs(OUT_DIR, exist_ok=True)

plt.rcParams.update({
    "figure.facecolor": "#0f1115",
    "axes.facecolor": "#0f1115",
    "savefig.facecolor": "#0f1115",
    "axes.edgecolor": "#3a3f4b",
    "axes.labelcolor": "#e6e6e6",
    "xtick.color": "#c9c9c9",
    "ytick.color": "#c9c9c9",
    "text.color": "#f2f2f2",
    "font.size": 10,
    "font.family": "DejaVu Sans",
})

ACCENT = "#00C2A8"
ACCENT2 = "#6C63FF"
ACCENT3 = "#F2994A"
PALETTE = ["#00C2A8", "#6C63FF", "#F2994A", "#EB5757", "#2D9CDB",
           "#BB6BD9", "#F2C94C", "#56CCF2", "#27AE60", "#9B51E0", "#E0E0E0"]


def kpi_row(ax, kpis):
    ax.axis("off")
    n = len(kpis)
    for i, (label, value) in enumerate(kpis):
        x = i / n + 0.02
        ax.text(x, 0.55, value, fontsize=20, fontweight="bold", color=ACCENT,
                 transform=ax.transAxes)
        ax.text(x, 0.15, label, fontsize=10, color="#c9c9c9", transform=ax.transAxes)


def fmt_millions(x, pos):
    return f"${x/1e6:.1f}M"


# =====================================================================
# Dashboard 1 — Sales Performance
# =====================================================================
perf = DeltaTable("data/gold/sales_performance_daily").to_pandas()
perf["sale_date"] = perf["sale_date"].astype(str)
daily = perf.groupby("sale_date").agg(
    gross_revenue=("gross_revenue", "sum"),
    num_transactions=("num_transactions", "sum"),
).reset_index().sort_values("sale_date")
by_channel = perf.groupby("channel")["gross_revenue"].sum()

total_revenue = perf["gross_revenue"].sum()
total_txn = perf["num_transactions"].sum()
avg_order = total_revenue / total_txn
online_share = by_channel.get("Online", 0) / total_revenue * 100

fig = plt.figure(figsize=(13, 8))
gs = fig.add_gridspec(3, 2, height_ratios=[0.6, 2, 2], hspace=0.55, wspace=0.3)

fig.suptitle("Sales Performance Dashboard", fontsize=18, fontweight="bold", y=0.98,
             color="white", ha="left", x=0.06)
fig.text(0.06, 0.935, "Retail Sales Analytics Platform  |  520K+ transactions across 4 integrated source datasets",
          fontsize=10, color="#9a9a9a")

ax_kpi = fig.add_subplot(gs[0, :])
kpi_row(ax_kpi, [
    ("TOTAL REVENUE", f"${total_revenue/1e6:.1f}M"),
    ("TOTAL TRANSACTIONS", f"{total_txn:,.0f}"),
    ("AVG ORDER VALUE", f"${avg_order:,.0f}"),
    ("ONLINE REVENUE SHARE", f"{online_share:.1f}%"),
])

ax1 = fig.add_subplot(gs[1, :])
# smooth via 7-day rolling for readability
daily["roll"] = daily["gross_revenue"].rolling(7, min_periods=1).mean()
ax1.plot(range(len(daily)), daily["gross_revenue"], color=ACCENT, alpha=0.25, linewidth=1)
ax1.plot(range(len(daily)), daily["roll"], color=ACCENT, linewidth=2.2, label="7-day avg revenue")
ax1.set_title("Daily Revenue Trend (7-day rolling average)", loc="left", fontsize=12, color="white")
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_millions))
ax1.set_xticks([])
ax1.grid(axis="y", color="#2a2e37", linewidth=0.6)
for spine in ["top", "right"]:
    ax1.spines[spine].set_visible(False)
ax1.legend(frameon=False, loc="upper left")

ax2 = fig.add_subplot(gs[2, 0])
ax2.pie(by_channel.values, labels=by_channel.index, autopct="%1.0f%%",
        colors=[ACCENT, ACCENT2], startangle=90,
        textprops={"color": "white", "fontsize": 10},
        wedgeprops={"edgecolor": "#0f1115", "linewidth": 2})
ax2.set_title("Revenue by Channel", loc="left", fontsize=12, color="white")

ax3 = fig.add_subplot(gs[2, 1])
top_regions = perf.groupby("region")["gross_revenue"].sum().sort_values(ascending=True).tail(6)
ax3.barh(top_regions.index, top_regions.values, color=ACCENT3)
ax3.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_millions))
ax3.set_title("Top 6 Regions by Revenue", loc="left", fontsize=12, color="white")
ax3.grid(axis="x", color="#2a2e37", linewidth=0.6)
for spine in ["top", "right"]:
    ax3.spines[spine].set_visible(False)

plt.savefig(f"{OUT_DIR}/01_sales_performance.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print("Saved 01_sales_performance.png")

# =====================================================================
# Dashboard 2 — Product Trends
# =====================================================================
prod = DeltaTable("data/gold/product_trends").to_pandas()
cat_revenue = prod.groupby("category")["revenue"].sum().sort_values(ascending=False)
top10 = prod.sort_values("revenue", ascending=False).head(10)

total_product_revenue = prod["revenue"].sum()
total_units = prod["units_sold"].sum()
unique_customers = prod["unique_customers"].sum()

fig = plt.figure(figsize=(13, 8))
gs = fig.add_gridspec(3, 2, height_ratios=[0.6, 2, 2], hspace=0.55, wspace=0.3)

fig.suptitle("Product Trends Dashboard", fontsize=18, fontweight="bold", y=0.98,
             color="white", ha="left", x=0.06)
fig.text(0.06, 0.935, "1,200 products across 6 categories  |  Delta Lake gold layer (product_trends)",
          fontsize=10, color="#9a9a9a")

ax_kpi = fig.add_subplot(gs[0, :])
kpi_row(ax_kpi, [
    ("PRODUCT REVENUE", f"${total_product_revenue/1e6:.1f}M"),
    ("UNITS SOLD", f"{total_units:,.0f}"),
    ("UNIQUE CUSTOMERS", f"{unique_customers:,.0f}"),
])

ax1 = fig.add_subplot(gs[1, :])
bars = ax1.barh(top10["product_name"][::-1], top10["revenue"][::-1], color=PALETTE[0])
ax1.set_title("Top 10 Products by Revenue", loc="left", fontsize=12, color="white")
ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x/1e3:.0f}K"))
ax1.grid(axis="x", color="#2a2e37", linewidth=0.6)
for spine in ["top", "right"]:
    ax1.spines[spine].set_visible(False)

ax2 = fig.add_subplot(gs[2, 0])
ax2.pie(cat_revenue.values, labels=cat_revenue.index, autopct="%1.0f%%",
        colors=PALETTE, startangle=90, textprops={"color": "white", "fontsize": 9},
        wedgeprops={"edgecolor": "#0f1115", "linewidth": 2})
ax2.set_title("Revenue Share by Category", loc="left", fontsize=12, color="white")

ax3 = fig.add_subplot(gs[2, 1])
cat_units = prod.groupby("category")["units_sold"].sum().sort_values(ascending=True)
ax3.barh(cat_units.index, cat_units.values, color=ACCENT2)
ax3.set_title("Units Sold by Category", loc="left", fontsize=12, color="white")
ax3.grid(axis="x", color="#2a2e37", linewidth=0.6)
for spine in ["top", "right"]:
    ax3.spines[spine].set_visible(False)

plt.savefig(f"{OUT_DIR}/02_product_trends.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print("Saved 02_product_trends.png")

# =====================================================================
# Dashboard 3 — Regional Insights
# =====================================================================
reg = DeltaTable("data/gold/regional_insights").to_pandas().sort_values("gross_revenue", ascending=False)

total_regions = len(reg)
avg_txn_value = reg["avg_transaction_value"].mean()
top_region = reg.iloc[0]["region"]

fig = plt.figure(figsize=(13, 8))
gs = fig.add_gridspec(3, 1, height_ratios=[0.6, 2.2, 2], hspace=0.6)

fig.suptitle("Regional Insights Dashboard", fontsize=18, fontweight="bold", y=0.98,
             color="white", ha="left", x=0.06)
fig.text(0.06, 0.935, f"{total_regions} operating regions  |  Delta Lake gold layer (regional_insights)",
          fontsize=10, color="#9a9a9a")

ax_kpi = fig.add_subplot(gs[0, :])
kpi_row(ax_kpi, [
    ("REGIONS COVERED", f"{total_regions}"),
    ("TOP REGION", top_region),
    ("AVG TRANSACTION VALUE", f"${avg_txn_value:,.0f}"),
])

ax1 = fig.add_subplot(gs[1, :])
colors = [PALETTE[i % len(PALETTE)] for i in range(len(reg))]
bars = ax1.bar(reg["region"], reg["gross_revenue"], color=colors)
ax1.set_title("Revenue by Region (ranked)", loc="left", fontsize=12, color="white")
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_millions))
ax1.set_xticklabels([])
ax1.tick_params(axis="x", length=0)
ax1.grid(axis="y", color="#2a2e37", linewidth=0.6)
for spine in ["top", "right"]:
    ax1.spines[spine].set_visible(False)
ax1.legend(bars, reg["region"], loc="upper center", bbox_to_anchor=(0.5, -0.02),
           ncol=6, frameon=False, fontsize=8)

ax2 = fig.add_subplot(gs[2, :])
ax2.axis("off")
table_data = reg[["region", "num_transactions", "unique_customers", "avg_transaction_value"]].copy()
table_data.columns = ["Region", "Transactions", "Unique Customers", "Avg Txn Value"]
table_data["Transactions"] = table_data["Transactions"].map("{:,}".format)
table_data["Unique Customers"] = table_data["Unique Customers"].map("{:,}".format)
table_data["Avg Txn Value"] = table_data["Avg Txn Value"].map("${:,.2f}".format)
tbl = ax2.table(cellText=table_data.values, colLabels=table_data.columns,
                 loc="center", cellLoc="left")
tbl.auto_set_font_size(False)
tbl.set_fontsize(9)
tbl.scale(1, 1.6)
for (row, col), cell in tbl.get_celld().items():
    cell.set_edgecolor("#2a2e37")
    if row == 0:
        cell.set_text_props(fontweight="bold", color="white")
        cell.set_facecolor("#1b1e26")
    else:
        cell.set_facecolor("#0f1115")
        cell.set_text_props(color="#e6e6e6")

plt.savefig(f"{OUT_DIR}/03_regional_insights.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print("Saved 03_regional_insights.png")

print("\nAll 3 dashboard previews generated from real gold-layer pipeline output.")
