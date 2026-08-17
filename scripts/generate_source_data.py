"""
generate_source_data.py
------------------------
Generates 4 synthetic retail source datasets that simulate the systems a
real retail analytics platform would ingest from:

    1. store_sales.csv      -> POS / in-store transaction system
    2. online_sales.csv     -> E-commerce platform
    3. product_catalog.csv  -> Product master (ERP)
    4. store_master.csv     -> Store / region reference data (ERP)

Combined, store_sales + online_sales exceed 500,000 transaction records
spanning 10+ regions, matching the metrics called out in the project README.

A controlled percentage of "dirty" records (nulls, negative quantities,
bad dates, duplicate IDs, orphan foreign keys) is deliberately injected so
that the schema validation / DQ framework downstream has real issues to
detect and report on.
"""
import csv
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUT_DIR = "data/raw"

REGIONS = [
    "North America", "Latin America", "Western Europe", "Eastern Europe",
    "Middle East", "North Africa", "Sub-Saharan Africa", "South Asia",
    "Southeast Asia", "East Asia", "Oceania",
]

CATEGORIES = {
    "Electronics": ["Headphones", "Smartphone", "Laptop", "Smartwatch", "Tablet", "Speaker"],
    "Apparel": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Hat"],
    "Home & Kitchen": ["Blender", "Cookware Set", "Vacuum Cleaner", "Air Fryer", "Bedding Set"],
    "Beauty": ["Moisturizer", "Shampoo", "Perfume", "Lipstick", "Sunscreen"],
    "Grocery": ["Coffee Beans", "Olive Oil", "Snack Pack", "Cereal", "Protein Bar"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Running Shoes", "Cycling Helmet", "Water Bottle"],
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "Gift Card"]
CHANNELS_ONLINE = ["Web", "Mobile App"]

START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2026, 5, 31)


def random_date(start, end):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def build_product_catalog(n_products=1200):
    """Product master dataset (ERP source)."""
    rows = []
    product_ids = []
    for i in range(n_products):
        category = random.choice(list(CATEGORIES.keys()))
        product_name = random.choice(CATEGORIES[category])
        product_id = f"PRD-{i:06d}"
        product_ids.append(product_id)
        unit_cost = round(random.uniform(3, 400), 2)
        margin = random.uniform(1.2, 2.8)
        unit_price = round(unit_cost * margin, 2)
        rows.append({
            "product_id": product_id,
            "product_name": f"{product_name} {fake.word().capitalize()}",
            "category": category,
            "sub_category": product_name,
            "unit_cost": unit_cost,
            "unit_price": unit_price,
            "supplier": fake.company(),
            "launch_date": random_date(datetime(2019, 1, 1), START_DATE).date().isoformat(),
        })

    # inject a few malformed rows (missing price -> DQ check target)
    for _ in range(15):
        r = random.choice(rows).copy()
        r["product_id"] = f"PRD-{random.randint(9000,9999)}"
        r["unit_price"] = ""
        rows.append(r)

    with open(f"{OUT_DIR}/product_catalog.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return product_ids


def build_store_master(n_stores=180):
    """Store / region reference dataset (ERP source)."""
    rows = []
    store_ids = []
    for i in range(n_stores):
        store_id = f"STR-{i:04d}"
        store_ids.append(store_id)
        region = REGIONS[i % len(REGIONS)]
        rows.append({
            "store_id": store_id,
            "store_name": f"{fake.city()} Retail Center",
            "region": region,
            "country": fake.country(),
            "store_type": random.choice(["Flagship", "Mall", "Outlet", "Express"]),
            "opened_date": random_date(datetime(2010, 1, 1), START_DATE).date().isoformat(),
        })

    with open(f"{OUT_DIR}/store_master.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return store_ids, {r["store_id"]: r["region"] for r in rows}


def build_store_sales(store_ids, product_ids, n_rows=300_000):
    """In-store POS transactions (largest source dataset)."""
    path = f"{OUT_DIR}/store_sales.csv"
    fieldnames = [
        "transaction_id", "store_id", "product_id", "quantity", "unit_price",
        "discount_pct", "total_amount", "payment_method", "transaction_ts",
        "customer_id",
    ]
    dirty_budget = int(n_rows * 0.015)  # ~1.5% intentionally dirty
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        seen_ids = set()
        for i in range(n_rows):
            txn_id = f"TXN-S-{i:07d}"
            store_id = random.choice(store_ids)
            product_id = random.choice(product_ids)
            qty = random.randint(1, 6)
            price = round(random.uniform(3, 500), 2)
            discount = random.choice([0, 0, 0, 5, 10, 15, 20])
            total = round(qty * price * (1 - discount / 100), 2)
            ts = random_date(START_DATE, END_DATE)

            row = {
                "transaction_id": txn_id,
                "store_id": store_id,
                "product_id": product_id,
                "quantity": qty,
                "unit_price": price,
                "discount_pct": discount,
                "total_amount": total,
                "payment_method": random.choice(PAYMENT_METHODS),
                "transaction_ts": ts.isoformat(sep=" "),
                "customer_id": f"CUST-{random.randint(1, 90000):06d}",
            }

            # inject dirty records at random within budget
            if dirty_budget > 0 and random.random() < 0.015:
                choice = random.random()
                if choice < 0.2:
                    row["quantity"] = -abs(qty)  # negative quantity
                elif choice < 0.4:
                    row["total_amount"] = ""  # missing amount
                elif choice < 0.6:
                    row["store_id"] = "STR-9999"  # orphan FK
                elif choice < 0.8:
                    row["transaction_ts"] = "31-13-2026"  # malformed date
                else:
                    row["transaction_id"] = f"TXN-S-{random.randint(0, i):07d}"  # dup id
                dirty_budget -= 1

            writer.writerow(row)


def build_online_sales(product_ids, n_rows=220_000):
    """E-commerce transactions (second largest source dataset)."""
    path = f"{OUT_DIR}/online_sales.csv"
    fieldnames = [
        "order_id", "product_id", "quantity", "unit_price", "shipping_region",
        "channel", "order_total", "order_ts", "customer_id", "is_returned",
    ]
    dirty_budget = int(n_rows * 0.015)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(n_rows):
            order_id = f"ORD-O-{i:07d}"
            product_id = random.choice(product_ids)
            qty = random.randint(1, 4)
            price = round(random.uniform(3, 500), 2)
            total = round(qty * price, 2)
            ts = random_date(START_DATE, END_DATE)

            row = {
                "order_id": order_id,
                "product_id": product_id,
                "quantity": qty,
                "unit_price": price,
                "shipping_region": random.choice(REGIONS),
                "channel": random.choice(CHANNELS_ONLINE),
                "order_total": total,
                "order_ts": ts.isoformat(sep=" "),
                "customer_id": f"CUST-{random.randint(1, 90000):06d}",
                "is_returned": random.choice([0, 0, 0, 0, 1]),
            }

            if dirty_budget > 0 and random.random() < 0.015:
                choice = random.random()
                if choice < 0.25:
                    row["product_id"] = "PRD-999999"  # orphan FK
                elif choice < 0.5:
                    row["quantity"] = ""  # missing qty
                elif choice < 0.75:
                    row["order_total"] = -total  # negative total
                else:
                    row["shipping_region"] = ""  # missing region
                dirty_budget -= 1

            writer.writerow(row)


if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    print("Generating product_catalog.csv ...")
    product_ids = build_product_catalog()

    print("Generating store_master.csv ...")
    store_ids, store_region_map = build_store_master()

    print("Generating store_sales.csv (300,000 rows) ...")
    build_store_sales(store_ids, product_ids, n_rows=300_000)

    print("Generating online_sales.csv (220,000 rows) ...")
    build_online_sales(product_ids, n_rows=220_000)

    print("Done. 4 source datasets written to data/raw/")
