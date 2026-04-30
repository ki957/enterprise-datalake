#!/usr/bin/env python3
"""
MySQL ShopFlow → MinIO Parquet (Airbyte-bypass ingestion)
===========================================================
Reads the shopflow MySQL tables and writes Parquet files to MinIO under
the same path convention Airbyte uses, so the existing bronze ClickHouse
views and dbt silver models work without modification.

Use this when Airbyte (k8s/abctl) is not available.

Usage:
    pip install mysql-connector-python pyarrow minio
    python scripts/ingest_mysql_to_minio.py [--small]

Output paths in MinIO bucket 'raw':
    airbyte/mysql/customers/YYYY_MM_DD_<epoch>_customers.parquet
    airbyte/mysql/orders/YYYY_MM_DD_<epoch>_orders.parquet
    airbyte/mysql/products/YYYY_MM_DD_<epoch>_products.parquet
"""

import argparse
import io
import sys
from datetime import datetime, timezone

import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

# ── Config ────────────────────────────────────────────────────────────────────

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "MySQL@2024"
MYSQL_DB   = "shopflow"

MINIO_ENDPOINT   = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "Minio@2024"
MINIO_BUCKET     = "raw"

NOW        = datetime.now(tz=timezone.utc)
DATE_PART  = NOW.strftime("%Y_%m_%d")
EPOCH      = int(NOW.timestamp())
EXTRACTED  = NOW.isoformat()


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_mysql():
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASS,
        database=MYSQL_DB, charset="utf8mb4",
    )


def get_minio():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    return client


def upload(client: Minio, records: list[dict], path: str) -> None:
    if not records:
        print(f"  [SKIP] no rows for {path}")
        return
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(records), buf, compression="snappy")
    buf.seek(0)
    size = buf.getbuffer().nbytes
    client.put_object(MINIO_BUCKET, path, buf, length=size,
                      content_type="application/octet-stream")
    print(f"  Uploaded {path}  ({len(records):,} rows, {size:,} bytes)")


# ── Extractors ────────────────────────────────────────────────────────────────

def extract_customers(cur, limit: int | None = None) -> list[dict]:
    sql = "SELECT * FROM customers ORDER BY customer_id"
    if limit:
        sql += f" LIMIT {limit}"
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = []
    for row in cur.fetchall():
        r = dict(zip(cols, row))
        # Add Airbyte CDC columns so bronze views work unchanged
        r["_ab_cdc_deleted_at"]     = None
        r["_ab_cdc_lsn"]            = None
        r["_airbyte_extracted_at"]  = EXTRACTED
        r["_airbyte_sync_id"]       = f"mysql-direct-{EPOCH}"
        # Serialise datetime fields to ISO strings
        for k, v in r.items():
            if isinstance(v, datetime):
                r[k] = v.isoformat()
        rows.append(r)
    return rows


def extract_orders(cur, limit: int | None = None) -> list[dict]:
    sql = "SELECT * FROM orders ORDER BY order_id"
    if limit:
        sql += f" LIMIT {limit}"
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = []
    for row in cur.fetchall():
        r = dict(zip(cols, row))
        r["_ab_cdc_deleted_at"]    = None
        r["_ab_cdc_lsn"]           = None
        r["_airbyte_extracted_at"] = EXTRACTED
        r["_airbyte_sync_id"]      = f"mysql-direct-{EPOCH}"
        for k, v in r.items():
            if isinstance(v, datetime):
                r[k] = v.isoformat()
        rows.append(r)
    return rows


def extract_products(cur, limit: int | None = None) -> list[dict]:
    sql = "SELECT * FROM products ORDER BY product_id"
    if limit:
        sql += f" LIMIT {limit}"
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = []
    for row in cur.fetchall():
        r = dict(zip(cols, row))
        r["_ab_cdc_deleted_at"]    = None
        r["_ab_cdc_lsn"]           = None
        r["_airbyte_extracted_at"] = EXTRACTED
        r["_airbyte_sync_id"]      = f"mysql-direct-{EPOCH}"
        for k, v in r.items():
            if isinstance(v, datetime):
                r[k] = v.isoformat()
        rows.append(r)
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--small", action="store_true",
                        help="Ingest only 500 customers / 2000 orders / 200 products")
    args = parser.parse_args()

    limit_customers = 500   if args.small else None
    limit_orders    = 2000  if args.small else None
    limit_products  = 200   if args.small else None

    print(f"{'[SMALL MODE] ' if args.small else ''}MySQL ShopFlow → MinIO Parquet")
    print(f"Run: {EXTRACTED}")
    print()

    conn = get_mysql()
    cur  = conn.cursor()
    minio = get_minio()

    print("Extracting customers ...")
    customers = extract_customers(cur, limit_customers)
    upload(minio, customers, f"airbyte/mysql/customers/{DATE_PART}_{EPOCH}_customers.parquet")

    print("Extracting orders ...")
    orders = extract_orders(cur, limit_orders)
    upload(minio, orders, f"airbyte/mysql/orders/{DATE_PART}_{EPOCH}_orders.parquet")

    print("Extracting products ...")
    products = extract_products(cur, limit_products)
    upload(minio, products, f"airbyte/mysql/products/{DATE_PART}_{EPOCH}_products.parquet")

    cur.close()
    conn.close()

    print(f"\nDone — {len(customers):,} customers / {len(orders):,} orders / {len(products):,} products ingested.")
    print("Next step: python scripts/setup_clickhouse_bronze.py")


if __name__ == "__main__":
    main()
