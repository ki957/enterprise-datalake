"""
Unified Customer Profile
=========================
PySpark cross-domain join: ShopFlow e-commerce customers ✕ SaaS subscription users.

The two data domains have never been connected. This job bridges them on
normalised email to answer questions like:
  - Which SaaS users also buy from the ShopFlow store?
  - What is the total revenue (orders + MRR) per customer across both domains?
  - Which high-MRR SaaS customers have never placed an e-commerce order?

Input  — reads from ClickHouse gold layer via JDBC:
  gold.dim_customers   (ShopFlow — SCD Type 2, filter is_current = 1)
  gold.dim_users       (SaaS — one row per user)
  gold.fct_orders      (ShopFlow — aggregated revenue per customer)

Output:
  gold.unified_customers in ClickHouse  (replaced on every run)
  s3a://gold/unified_customers/<date>/  Parquet snapshot for audit/lineage

customer_type values:
  cross_domain    — email found in both ShopFlow and SaaS
  shopflow_only   — e-commerce customer with no SaaS subscription
  saas_only       — SaaS subscriber who has never placed a ShopFlow order

Usage:
  spark-submit --master spark://localhost:7077 \\
    --packages org.apache.hadoop:hadoop-aws:3.3.4,\\
               com.amazonaws:aws-java-sdk-bundle:1.12.262,\\
               com.clickhouse:clickhouse-jdbc:0.4.6,\\
               org.apache.httpcomponents.client5:httpclient5:5.2.1 \\
    --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \\
    --conf spark.hadoop.fs.s3a.access.key=admin \\
    --conf spark.hadoop.fs.s3a.secret.key=<password> \\
    --conf spark.hadoop.fs.s3a.path.style.access=true \\
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
    services/spark/jobs/unified_customer_profile.py

Or via: make spark-unified
"""

import os
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Configuration ─────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "")

CH_HOST     = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CH_USER     = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_JDBC_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_HTTP_PORT}/gold"

OUTPUT_MINIO_PATH = f"s3a://gold/unified_customers/{date.today().isoformat()}/"
OUTPUT_CH_TABLE   = "gold.unified_customers"

JDBC_PROPS = {
    "driver":   "com.clickhouse.jdbc.ClickHouseDriver",
    "user":     CH_USER,
    "password": CH_PASSWORD,
}


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("UnifiedCustomerProfile")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.adaptive.enabled",            "true")
        # Broadcast hint threshold — dim tables fit in memory easily
        .config("spark.sql.autoBroadcastJoinThreshold",  "50mb")
        .getOrCreate()
    )


# ── ClickHouse DDL via HTTP ───────────────────────────────────────────────────

def ch_exec(sql: str) -> None:
    """Execute a DDL/DML statement against ClickHouse HTTP API."""
    import requests
    resp = requests.post(
        f"http://{CH_HOST}:{CH_HTTP_PORT}/",
        data=sql,
        auth=(CH_USER, CH_PASSWORD),
        timeout=60,
    )
    resp.raise_for_status()


def ensure_output_table() -> None:
    """Create gold.unified_customers if it doesn't exist."""
    ch_exec("""
        CREATE TABLE IF NOT EXISTS gold.unified_customers (
            email                   String,
            customer_id             Nullable(UInt64),
            user_id                 Nullable(UInt64),
            full_name               Nullable(String),
            shopflow_segment        Nullable(String),
            shopflow_country        Nullable(String),
            shopflow_city           Nullable(String),
            shopflow_order_count    UInt64,
            total_shopflow_revenue  Float64,
            saas_plan               Nullable(String),
            saas_mrr                Float64,
            saas_status             Nullable(String),
            saas_company            Nullable(String),
            saas_country            Nullable(String),
            is_churned              UInt8,
            customer_type           String,
            profiled_at             DateTime
        ) ENGINE = ReplacingMergeTree()
        ORDER BY email
        SETTINGS allow_nullable_key = 1
    """)
    # Clear previous run — this is a full-replace job
    ch_exec("TRUNCATE TABLE IF EXISTS gold.unified_customers")


# ── Readers ───────────────────────────────────────────────────────────────────

def read_dim_customers(spark: SparkSession):
    """Read current ShopFlow customers from ClickHouse."""
    sql = """(
        SELECT
            customer_id,
            lower(trim(email))  AS email,
            full_name,
            segment             AS shopflow_segment,
            country             AS shopflow_country,
            city                AS shopflow_city,
            customer_key
        FROM gold.dim_customers
        WHERE is_current = 1
    ) t"""
    return spark.read.jdbc(url=CH_JDBC_URL, table=sql, properties=JDBC_PROPS)


def read_dim_users(spark: SparkSession):
    """Read SaaS users from ClickHouse."""
    sql = """(
        SELECT
            user_id,
            lower(trim(email))  AS email,
            plan                AS saas_plan,
            mrr                 AS saas_mrr,
            status              AS saas_status,
            company             AS saas_company,
            country             AS saas_country,
            is_churned
        FROM gold.dim_users
    ) t"""
    return spark.read.jdbc(url=CH_JDBC_URL, table=sql, properties=JDBC_PROPS)


def read_order_aggregates(spark: SparkSession):
    """Aggregate ShopFlow orders per customer — total revenue and order count."""
    sql = """(
        SELECT
            customer_key,
            toUInt64(count())           AS shopflow_order_count,
            toFloat64(sum(revenue))     AS total_shopflow_revenue
        FROM gold.fct_orders
        GROUP BY customer_key
    ) t"""
    return spark.read.jdbc(url=CH_JDBC_URL, table=sql, properties=JDBC_PROPS)


# ── Transform ─────────────────────────────────────────────────────────────────

def build_unified_profile(customers, users, order_aggs):
    """
    Full outer join on email so we capture all three customer types:
      cross_domain  — in both ShopFlow and SaaS
      shopflow_only — e-commerce only (no SaaS account)
      saas_only     — SaaS only (never placed a ShopFlow order)
    """
    # Join order aggregates onto customers before the cross-domain join
    customers_with_orders = customers.join(
        order_aggs,
        on="customer_key",
        how="left",
    ).fillna({"shopflow_order_count": 0, "total_shopflow_revenue": 0.0})

    # Full outer join across domains on normalised email
    unified = customers_with_orders.join(users, on="email", how="full_outer")

    # Classify each row — use F.col() after the join to avoid stale DataFrame references
    unified = unified.withColumn(
        "customer_type",
        F.when(
            F.col("customer_id").isNotNull() & F.col("user_id").isNotNull(),
            F.lit("cross_domain"),
        ).when(
            F.col("customer_id").isNotNull(),
            F.lit("shopflow_only"),
        ).otherwise(
            F.lit("saas_only"),
        ),
    )

    # Coalesce nulls from the outer join for numeric columns
    unified = (
        unified
        .withColumn("saas_mrr",               F.coalesce(F.col("saas_mrr"), F.lit(0.0)))
        .withColumn("is_churned",              F.coalesce(F.col("is_churned"), F.lit(0)))
        .withColumn("shopflow_order_count",    F.coalesce(F.col("shopflow_order_count"), F.lit(0)))
        .withColumn("total_shopflow_revenue",  F.coalesce(F.col("total_shopflow_revenue"), F.lit(0.0)))
        .withColumn("profiled_at",             F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
    )

    return unified.select(
        "email",
        "customer_id",
        "user_id",
        "full_name",
        "shopflow_segment",
        "shopflow_country",
        "shopflow_city",
        "shopflow_order_count",
        "total_shopflow_revenue",
        "saas_plan",
        "saas_mrr",
        "saas_status",
        "saas_company",
        "saas_country",
        "is_churned",
        "customer_type",
        "profiled_at",
    )


# ── Writers ───────────────────────────────────────────────────────────────────

def write_to_minio(df, path: str) -> None:
    """Write Parquet snapshot to MinIO for audit and lineage."""
    df.coalesce(1).write.mode("overwrite").parquet(path)
    print(f"Parquet snapshot written to: {path}")


def write_to_clickhouse(df) -> None:
    """Write unified profile to ClickHouse via JDBC."""
    (
        df.write
        .format("jdbc")
        .option("url", CH_JDBC_URL)
        .option("dbtable", OUTPUT_CH_TABLE)
        .option("driver", JDBC_PROPS["driver"])
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        # ClickHouse JDBC batch size — larger = fewer round-trips
        .option("batchsize", "10000")
        .mode("append")   # table was already truncated in ensure_output_table()
        .save()
    )
    print(f"Unified profile written to ClickHouse: {OUTPUT_CH_TABLE}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n── Reading source tables from ClickHouse ────────────────────")
    customers  = read_dim_customers(spark)
    users      = read_dim_users(spark)
    order_aggs = read_order_aggregates(spark)

    n_customers  = customers.count()
    n_users      = users.count()
    print(f"  dim_customers (current): {n_customers:,}")
    print(f"  dim_users:               {n_users:,}")

    print("\n── Building unified profile ─────────────────────────────────")
    unified = build_unified_profile(customers, users, order_aggs)

    # Cache — we write to two sinks
    unified.cache()
    total = unified.count()

    cross    = unified.filter(F.col("customer_type") == "cross_domain").count()
    sf_only  = unified.filter(F.col("customer_type") == "shopflow_only").count()
    saas_only = unified.filter(F.col("customer_type") == "saas_only").count()

    print(f"  Total unified records:  {total:,}")
    print(f"    cross_domain:         {cross:,}")
    print(f"    shopflow_only:        {sf_only:,}")
    print(f"    saas_only:            {saas_only:,}")

    print("\n── Writing outputs ──────────────────────────────────────────")
    ensure_output_table()
    write_to_clickhouse(unified)
    write_to_minio(unified, OUTPUT_MINIO_PATH)

    print("\n── Sample: cross-domain customers ───────────────────────────")
    unified.filter(F.col("customer_type") == "cross_domain") \
           .select("email", "full_name", "shopflow_segment",
                   "saas_plan", "saas_mrr", "total_shopflow_revenue") \
           .show(10, truncate=False)

    unified.unpersist()
    spark.stop()
    print("\nUnified customer profile job complete.")


if __name__ == "__main__":
    main()
