-- Great Expectations CI seed — creates minimal gold/staging tables in the
-- test ClickHouse instance and inserts enough rows to satisfy all expectations.
-- Run via: clickhouse-client -h localhost --port 19000 --multiquery < seed_ge_test_data.sql

-- ── Schemas ──────────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS staging;

-- ── gold.fct_orders ──────────────────────────────────────────────────────────
-- Expectations: row count 100–10M, not-null on order_id/customer_key/product_key/
-- order_date_day, status in [pending, completed, cancelled], amount 0.01–1M,
-- order_id >99% unique.
CREATE TABLE IF NOT EXISTS gold.fct_orders (
    order_id       String,
    customer_key   String,
    product_key    String,
    amount         Float64,
    revenue        Float64,
    status         String,
    order_date     DateTime,
    order_date_day Date
) ENGINE = MergeTree() ORDER BY order_id;

INSERT INTO gold.fct_orders
SELECT
    toString(number)                                    AS order_id,
    toString(number % 50)                               AS customer_key,
    toString(number % 20)                               AS product_key,
    round(toFloat64(number % 999) + 1.0, 2)            AS amount,
    round(toFloat64(number % 999) + 1.0, 2)            AS revenue,
    ['pending', 'completed', 'cancelled'][(number % 3) + 1] AS status,
    now() - toIntervalDay(number % 365)                AS order_date,
    today() - toInt32(number % 365)                    AS order_date_day
FROM numbers(150);

-- ── gold.dim_customers ───────────────────────────────────────────────────────
-- Expectations: columns exist, is_current values.
CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_key String,
    customer_id  UInt64,
    name         String,
    email        String,
    country      String,
    segment      String,
    valid_from   DateTime,
    valid_to     DateTime,
    is_current   UInt8
) ENGINE = MergeTree() ORDER BY customer_key;

INSERT INTO gold.dim_customers
SELECT
    toString(number)                                        AS customer_key,
    number                                                  AS customer_id,
    concat('Customer ', toString(number))                   AS name,
    concat('user', toString(number), '@example.com')        AS email,
    ['US', 'UK', 'DE', 'FR', 'CA'][(number % 5) + 1]      AS country,
    ['Enterprise', 'SMB', 'Consumer'][(number % 3) + 1]    AS segment,
    now() - toIntervalDay(30)                               AS valid_from,
    toDateTime('9999-12-31 00:00:00')                       AS valid_to,
    1                                                       AS is_current
FROM numbers(50);

-- ── gold.fct_procurement ─────────────────────────────────────────────────────
-- Expectations: row count, columns, amount > 0.
CREATE TABLE IF NOT EXISTS gold.fct_procurement (
    po_id       String,
    vendor_key  String,
    amount      Float64,
    status      String,
    po_date     DateTime
) ENGINE = MergeTree() ORDER BY po_id;

INSERT INTO gold.fct_procurement
SELECT
    toString(number)                                           AS po_id,
    toString(number % 10)                                      AS vendor_key,
    round(toFloat64(number % 9900) + 100.0, 2)               AS amount,
    ['pending', 'approved', 'closed'][(number % 3) + 1]      AS status,
    now() - toIntervalDay(number % 365)                        AS po_date
FROM numbers(120);

-- ── staging.stg_orders ───────────────────────────────────────────────────────
-- Expectations: row count, not-null columns, status values.
CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id    String,
    customer_id UInt64,
    product_id  UInt64,
    amount      Float64,
    status      String,
    order_date  DateTime
) ENGINE = MergeTree() ORDER BY order_id;

INSERT INTO staging.stg_orders
SELECT
    toString(number)                                        AS order_id,
    toUInt64(number % 50)                                   AS customer_id,
    toUInt64(number % 20)                                   AS product_id,
    round(toFloat64(number % 999) + 1.0, 2)               AS amount,
    ['pending', 'completed', 'cancelled'][(number % 3) + 1] AS status,
    now() - toIntervalDay(number % 365)                     AS order_date
FROM numbers(150);
