#!/usr/bin/env python3
"""
ShopFlow MySQL synthetic data generator.
Produces: 50,000 customers, 5,000 products, 500,000 orders.

Uses chunked batch inserts (CHUNK_SIZE rows per commit) to stay within
MySQL's max_allowed_packet and keep RAM usage flat.

Usage:
    pip install faker mysql-connector-python
    python scripts/generate_mysql_data.py [--host HOST] [--port PORT]
    python scripts/generate_mysql_data.py --small   # 500/200/2000 for fast dev runs
"""

import argparse
import random
from datetime import datetime, timedelta

import mysql.connector
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULTS = {
    "host":     "127.0.0.1",
    "port":     3306,
    "user":     "root",
    "password": "MySQL@2024",
    "database": "shopflow",
}

N_CUSTOMERS = 50_000
N_PRODUCTS  = 5_000
N_ORDERS    = 500_000
CHUNK_SIZE  = 10_000   # rows per commit — keeps RAM flat, avoids packet limits

CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports", "Books",
    "Toys", "Beauty", "Automotive", "Food & Beverage", "Office",
]
SEGMENTS = ["B2B", "B2C", "VIP"]
ORDER_STATUSES = ["pending", "completed", "cancelled"]

COUNTRY_WEIGHTS = {
    "US": 30, "GB": 10, "DE": 8, "FR": 7, "CA": 6,
    "AU": 5,  "IN": 8,  "BR": 5, "JP": 5, "MX": 4,
    "SG": 3,  "NL": 3,  "SE": 2, "NO": 2, "IT": 2,
}
COUNTRY_POOL = list(COUNTRY_WEIGHTS.keys())
COUNTRY_PROBS = [COUNTRY_WEIGHTS[c] / 100.0 for c in COUNTRY_POOL]


# ── Helpers ───────────────────────────────────────────────────────────────────

def rand_date(start_days_ago: int = 730, end_days_ago: int = 0) -> datetime:
    start = datetime.now() - timedelta(days=start_days_ago)
    end   = datetime.now() - timedelta(days=end_days_ago)
    return start + (end - start) * random.random()


def weighted_country() -> str:
    return random.choices(COUNTRY_POOL, weights=COUNTRY_PROBS, k=1)[0]


# ── Generators ────────────────────────────────────────────────────────────────

def customer_rows(n: int):
    """Yield customer dicts, guaranteeing unique emails at scale."""
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
               "company.io", "corp.net", "biz.co"]
    for i in range(n):
        created = rand_date(730, 30)
        yield {
            "first_name": fake.first_name(),
            "last_name":  fake.last_name(),
            "email":      f"user_{i+1}_{fake.lexify('????')}@{random.choice(domains)}",
            "phone":      fake.phone_number()[:30],
            "segment":    random.choice(SEGMENTS),
            "country":    weighted_country(),
            "city":       fake.city(),
            "created_at": created,
            "updated_at": created + timedelta(days=random.randint(0, 30)),
        }


def product_rows(n: int):
    """Yield product dicts with unique SKUs."""
    categories_expanded = CATEGORIES * (n // len(CATEGORIES) + 1)
    random.shuffle(categories_expanded)
    for i in range(n):
        created = rand_date(730, 60)
        # price distribution: most items $10-$300, some premium up to $2000
        price = (
            round(random.uniform(10.0, 299.99), 2)
            if random.random() < 0.85
            else round(random.uniform(300.0, 1999.99), 2)
        )
        yield {
            "name":       fake.catch_phrase()[:255],
            "category":   categories_expanded[i],
            "price":      price,
            "stock_qty":  random.randint(0, 1000),
            "sku":        f"SKU-{i+1:06d}-{fake.lexify('????').upper()}",
            "created_at": created,
            "updated_at": created + timedelta(days=random.randint(0, 20)),
        }


def order_rows(n: int, customer_ids: list, product_ids: list):
    """Yield order dicts.  Revenue is correlated with segment via customer lookup."""
    # 70% of orders are in the last 12 months for recency-rich analytics
    for _ in range(n):
        recency = random.random()
        if recency < 0.70:
            order_date = rand_date(365, 0)
        elif recency < 0.90:
            order_date = rand_date(545, 365)
        else:
            order_date = rand_date(730, 545)

        # Multi-item orders: qty 1-5 most common, occasional bulk
        qty = random.choices([1, 2, 3, 4, 5, 10, 20, 50],
                             weights=[40, 25, 15, 8, 5, 4, 2, 1], k=1)[0]
        yield {
            "customer_id": random.choice(customer_ids),
            "product_id":  random.choice(product_ids),
            "amount":      round(random.uniform(10.0, 4999.99), 2),
            "quantity":    qty,
            "status":      random.choices(
                ORDER_STATUSES, weights=[15, 75, 10], k=1
            )[0],
            "order_date":  order_date,
            "updated_at":  order_date + timedelta(hours=random.randint(1, 72)),
        }


# ── Database helpers ──────────────────────────────────────────────────────────

def get_connection(cfg: dict):
    return mysql.connector.connect(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"],
        database=cfg["database"],
        allow_local_infile=True,
    )


def bulk_insert_chunked(conn, cursor, table: str, row_gen, total: int) -> int:
    """Stream rows from generator and insert CHUNK_SIZE at a time."""
    cols      = None
    inserted  = 0
    chunk     = []

    for row in row_gen:
        if cols is None:
            cols = list(row.keys())
            ph   = ", ".join(["%s"] * len(cols))
            col_list = ", ".join(cols)
            sql  = f"INSERT INTO {table} ({col_list}) VALUES ({ph})"
        chunk.append(tuple(row[c] for c in cols))
        if len(chunk) >= CHUNK_SIZE:
            cursor.executemany(sql, chunk)
            conn.commit()
            inserted += len(chunk)
            print(f"  {table}: {inserted:,} / {total:,}", end="\r", flush=True)
            chunk = []

    if chunk:
        cursor.executemany(sql, chunk)
        conn.commit()
        inserted += len(chunk)

    print(f"  {table}: {inserted:,} / {total:,}  ✓")
    return inserted


def fetch_ids(cursor, table: str, pk: str) -> list:
    cursor.execute(f"SELECT {pk} FROM {table} ORDER BY {pk}")
    return [row[0] for row in cursor.fetchall()]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate ShopFlow MySQL synthetic data")
    parser.add_argument("--host",     default=DEFAULTS["host"])
    parser.add_argument("--port",     type=int, default=DEFAULTS["port"])
    parser.add_argument("--user",     default=DEFAULTS["user"])
    parser.add_argument("--password", default=DEFAULTS["password"])
    parser.add_argument("--database", default=DEFAULTS["database"])
    parser.add_argument("--small",    action="store_true",
                        help="Generate small dataset (500/200/2000) for fast dev runs")
    args = parser.parse_args()

    n_customers = 500     if args.small else N_CUSTOMERS
    n_products  = 200     if args.small else N_PRODUCTS
    n_orders    = 2_000   if args.small else N_ORDERS

    cfg = {k: getattr(args, k, DEFAULTS[k]) for k in DEFAULTS}

    print(f"Connecting to MySQL at {cfg['host']}:{cfg['port']} / {cfg['database']} ...")
    conn   = get_connection(cfg)
    cursor = conn.cursor()

    # Disable FK checks for fast truncate
    print("Truncating existing data ...")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    for tbl in ("orders", "products", "customers"):
        cursor.execute(f"TRUNCATE TABLE {tbl}")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()

    print(f"Inserting {n_customers:,} customers ...")
    bulk_insert_chunked(conn, cursor, "customers", customer_rows(n_customers), n_customers)
    customer_ids = fetch_ids(cursor, "customers", "customer_id")
    print(f"  IDs {customer_ids[0]} – {customer_ids[-1]}")

    print(f"Inserting {n_products:,} products ...")
    bulk_insert_chunked(conn, cursor, "products", product_rows(n_products), n_products)
    product_ids = fetch_ids(cursor, "products", "product_id")
    print(f"  IDs {product_ids[0]} – {product_ids[-1]}")

    print(f"Inserting {n_orders:,} orders ...")
    bulk_insert_chunked(
        conn, cursor, "orders",
        order_rows(n_orders, customer_ids, product_ids),
        n_orders,
    )

    cursor.close()
    conn.close()
    print(f"\nDone. {n_customers:,} customers / {n_products:,} products / {n_orders:,} orders loaded.")


if __name__ == "__main__":
    main()
