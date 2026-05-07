#!/usr/bin/env python3
"""
Creates the `bronze` schema in ClickHouse with S3 views pointing to MinIO.
These views are the entry point for the dbt silver models.

Run:
    python scripts/setup_clickhouse_bronze.py
"""

import os

import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_DEFAULT_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_DEFAULT_PASSWORD")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET   = "raw"


def ch(client, sql: str, label: str = ""):
    try:
        client.command(sql)
        if label:
            print(f"  OK  {label}")
    except Exception as e:
        print(f"  ERR {label}: {e}")


def s3(path: str) -> str:
    return f"s3('{MINIO_ENDPOINT}/{MINIO_BUCKET}/{path}', '{MINIO_ACCESS}', '{MINIO_SECRET}', 'Parquet')"


def main():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    print("Creating bronze database ...")
    ch(client, "CREATE DATABASE IF NOT EXISTS bronze", "bronze database")

    print("\nCreating MySQL bronze views ...")

    ch(client, f"""
        CREATE OR REPLACE VIEW bronze.src_mysql_customers AS
        SELECT
            toInt64(customer_id)                        AS customer_id,
            toString(first_name)                        AS first_name,
            toString(last_name)                         AS last_name,
            lower(toString(email))                      AS email,
            toString(phone)                             AS phone,
            toString(segment)                           AS segment,
            toString(country)                           AS country,
            toString(city)                              AS city,
            toDateTime64(created_at, 3, 'UTC')          AS created_at,
            toDateTime64(updated_at, 3, 'UTC')          AS updated_at,
            toString(_ab_cdc_deleted_at)                AS _ab_cdc_deleted_at,
            toDateTime64(_airbyte_extracted_at, 3, 'UTC') AS _airbyte_extracted_at
        FROM {s3('airbyte/mysql/customers/*')}
        WHERE customer_id IS NOT NULL
          AND (_ab_cdc_deleted_at IS NULL OR _ab_cdc_deleted_at = '')
    """, "src_mysql_customers")

    ch(client, f"""
        CREATE OR REPLACE VIEW bronze.src_mysql_orders AS
        SELECT
            toInt64(order_id)                           AS order_id,
            toInt64(customer_id)                        AS customer_id,
            toInt64(product_id)                         AS product_id,
            toDecimal64(amount, 2)                      AS amount,
            toInt32(quantity)                           AS quantity,
            toString(status)                            AS status,
            toDateTime64(order_date, 3, 'UTC')          AS order_date,
            toDateTime64(updated_at, 3, 'UTC')          AS updated_at,
            toString(_ab_cdc_deleted_at)                AS _ab_cdc_deleted_at,
            toDateTime64(_airbyte_extracted_at, 3, 'UTC') AS _airbyte_extracted_at
        FROM {s3('airbyte/mysql/orders/*')}
        WHERE order_id IS NOT NULL
          AND (_ab_cdc_deleted_at IS NULL OR _ab_cdc_deleted_at = '')
    """, "src_mysql_orders")

    ch(client, f"""
        CREATE OR REPLACE VIEW bronze.src_mysql_products AS
        SELECT
            toInt64(product_id)                         AS product_id,
            toString(name)                              AS name,
            toString(category)                          AS category,
            toDecimal64(price, 2)                       AS price,
            toInt32(stock_qty)                          AS stock_qty,
            toString(sku)                               AS sku,
            toDateTime64(created_at, 3, 'UTC')          AS created_at,
            toDateTime64(updated_at, 3, 'UTC')          AS updated_at,
            toString(_ab_cdc_deleted_at)                AS _ab_cdc_deleted_at,
            toDateTime64(_airbyte_extracted_at, 3, 'UTC') AS _airbyte_extracted_at
        FROM {s3('airbyte/mysql/products/*')}
        WHERE product_id IS NOT NULL
          AND (_ab_cdc_deleted_at IS NULL OR _ab_cdc_deleted_at = '')
    """, "src_mysql_products")

    print("\nCreating SAP bronze views ...")

    ch(client, f"""
        CREATE OR REPLACE VIEW bronze.src_sap_vendors AS
        SELECT
            toString(vendor_id)                         AS vendor_id,
            toString(name)                              AS name,
            toString(contact_name)                      AS contact_name,
            lower(toString(email))                      AS email,
            toString(phone)                             AS phone,
            toString(country)                           AS country,
            toString(city)                              AS city,
            toString(category)                          AS category,
            toString(payment_terms)                     AS payment_terms,
            toUInt8(is_active)                          AS is_active,
            parseDateTime64BestEffortOrNull(toString(created_at), 3, 'UTC') AS created_at,
            parseDateTime64BestEffortOrNull(toString(updated_at), 3, 'UTC') AS updated_at
        FROM {s3('airbyte/sap/vendors/*.parquet')}
        WHERE vendor_id != ''
    """, "src_sap_vendors")

    ch(client, f"""
        CREATE OR REPLACE VIEW bronze.src_sap_purchase_orders AS
        SELECT
            toString(po_id)                             AS po_id,
            toString(vendor_id)                         AS vendor_id,
            toDecimal64(amount, 2)                      AS amount,
            toString(currency)                          AS currency,
            toString(status)                            AS status,
            toInt32(line_items)                         AS line_items,
            toString(description)                       AS description,
            toString(requested_by)                      AS requested_by,
            parseDateTime64BestEffortOrNull(toString(po_date), 3, 'UTC')       AS po_date,
            parseDateTime64BestEffortOrNull(toString(delivery_date), 3, 'UTC') AS delivery_date,
            parseDateTime64BestEffortOrNull(toString(created_at), 3, 'UTC')    AS created_at,
            parseDateTime64BestEffortOrNull(toString(updated_at), 3, 'UTC')    AS updated_at
        FROM {s3('airbyte/sap/purchase_orders/*.parquet')}
        WHERE po_id != ''
    """, "src_sap_purchase_orders")

    print("\nCreating REST bronze views ...")

    ch(client, f"""
        CREATE OR REPLACE VIEW bronze.src_rest_users AS
        SELECT
            toInt64(id)                                 AS user_id,
            toString(name)                              AS name,
            lower(toString(email))                      AS email,
            toString(username)                          AS username,
            toString(phone)                             AS phone,
            toString(website)                           AS website,
            toString(address)                           AS address_json,
            toString(company)                           AS company_json
        FROM {s3('airbyte/rest/users/*.parquet')}
        WHERE id IS NOT NULL
    """, "src_rest_users")

    ch(client, f"""
        CREATE OR REPLACE VIEW bronze.src_rest_posts AS
        SELECT
            toInt64(id)                                 AS post_id,
            toInt64(userId)                             AS user_id,
            toString(title)                             AS title,
            toString(body)                              AS body
        FROM {s3('airbyte/rest/posts/*.parquet')}
        WHERE id IS NOT NULL
    """, "src_rest_posts")

    ch(client, f"""
        CREATE OR REPLACE VIEW bronze.src_rest_comments AS
        SELECT
            toInt64(id)                                 AS comment_id,
            toInt64(postId)                             AS post_id,
            toString(name)                              AS name,
            lower(toString(email))                      AS email,
            toString(body)                              AS body
        FROM {s3('airbyte/rest/comments/*.parquet')}
        WHERE id IS NOT NULL
    """, "src_rest_comments")

    print("\nVerifying row counts ...")
    views = [
        ("bronze.src_mysql_customers",     "customers"),
        ("bronze.src_mysql_orders",        "orders"),
        ("bronze.src_mysql_products",      "products"),
        ("bronze.src_sap_vendors",         "vendors"),
        ("bronze.src_sap_purchase_orders", "purchase_orders"),
        ("bronze.src_rest_users",          "users"),
        ("bronze.src_rest_posts",          "posts"),
        ("bronze.src_rest_comments",       "comments"),
    ]
    for view, label in views:
        count = client.command(f"SELECT count() FROM {view}")
        print(f"  {view:45s}  {count:>6} rows")

    print("\nBronze layer ready.")


if __name__ == "__main__":
    main()
