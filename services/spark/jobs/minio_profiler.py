"""
MinIO Bronze/Silver Data Profiler
===================================
PySpark job that reads Parquet files from MinIO and produces a column-level
profiling report (row count, null rate, min/max/mean/stddev for numerics,
distinct count and top-5 values for strings).

Output is written as Parquet to MinIO: logs/profiling/YYYY-MM-DD/

Usage (from host — submit to Spark master):
    spark-submit --master spark://localhost:7077 \\
        --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \\
        --conf spark.hadoop.fs.s3a.access.key=admin \\
        --conf spark.hadoop.fs.s3a.secret.key=Minio@2024 \\
        --conf spark.hadoop.fs.s3a.path.style.access=true \\
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
        services/spark/jobs/minio_profiler.py

Or via: make spark-profile
"""

import os
import sys
from datetime import date, datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, FloatType, IntegerType, LongType, DecimalType,
    StringType, TimestampType, DateType,
)

# ── Configuration ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "Minio@2024")

OUTPUT_PATH = f"s3a://logs/profiling/{date.today().isoformat()}"

# Parquet paths to profile — (label, s3a path)
SOURCES_TO_PROFILE = [
    ("mysql_customers",    "s3a://raw/airbyte/mysql/customers/_airbyte_raw_customers/"),
    ("mysql_orders",       "s3a://raw/airbyte/mysql/orders/_airbyte_raw_orders/"),
    ("mysql_products",     "s3a://raw/airbyte/mysql/products/_airbyte_raw_products/"),
    ("sap_vendors",        "s3a://raw/airbyte/sap/vendors/"),
    ("sap_purchase_orders","s3a://raw/airbyte/sap/purchase_orders/"),
    ("rest_posts",         "s3a://raw/airbyte/rest/posts/"),
]

NUMERIC_TYPES = (DoubleType, FloatType, IntegerType, LongType, DecimalType)
STRING_TYPES  = (StringType,)


# ── Helpers ───────────────────────────────────────────────────────────────────

def profile_dataframe(label: str, df: DataFrame) -> list[dict]:
    """Return a list of per-column profile dicts for the given DataFrame."""
    row_count = df.count()
    if row_count == 0:
        return [{"source": label, "column": "_empty_", "row_count": 0}]

    results = []
    for field in df.schema.fields:
        col_name = field.name
        dtype    = field.dataType

        null_count = df.filter(F.col(col_name).isNull()).count()
        null_rate  = round(null_count / row_count, 4) if row_count else 0.0

        record = {
            "source":     label,
            "column":     col_name,
            "dtype":      str(dtype),
            "row_count":  row_count,
            "null_count": null_count,
            "null_rate":  null_rate,
            "profiled_at": datetime.utcnow().isoformat(),
        }

        if isinstance(dtype, NUMERIC_TYPES):
            stats = df.select(
                F.min(col_name).alias("min_val"),
                F.max(col_name).alias("max_val"),
                F.mean(col_name).alias("mean_val"),
                F.stddev(col_name).alias("stddev_val"),
            ).first()
            record.update({
                "min_val":    float(stats["min_val"]) if stats["min_val"] is not None else None,
                "max_val":    float(stats["max_val"]) if stats["max_val"] is not None else None,
                "mean_val":   float(stats["mean_val"]) if stats["mean_val"] is not None else None,
                "stddev_val": float(stats["stddev_val"]) if stats["stddev_val"] is not None else None,
            })

        elif isinstance(dtype, STRING_TYPES):
            distinct_count = df.select(col_name).distinct().count()
            top5 = (
                df.groupBy(col_name)
                  .count()
                  .orderBy(F.desc("count"))
                  .limit(5)
                  .select(col_name)
                  .rdd.flatMap(lambda r: [r[0]])
                  .collect()
            )
            record.update({
                "distinct_count": distinct_count,
                "top5_values":    str(top5),
            })

        results.append(record)

    return results


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MinIO-DataProfiler")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.adaptive.enabled",            "true")
        .getOrCreate()
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    all_profiles: list[dict] = []

    for label, path in SOURCES_TO_PROFILE:
        print(f"\nProfiling: {label}  ({path})")
        try:
            df = spark.read.parquet(path)
            print(f"  schema: {df.schema.simpleString()}")
            profiles = profile_dataframe(label, df)
            all_profiles.extend(profiles)
            print(f"  {len(profiles)} columns profiled, {profiles[0].get('row_count', 0):,} rows")
        except Exception as exc:
            print(f"  SKIP — {exc}")
            all_profiles.append({
                "source":     label,
                "column":     "_error_",
                "error":      str(exc),
                "profiled_at": datetime.utcnow().isoformat(),
            })

    if not all_profiles:
        print("No data profiled — exiting.")
        spark.stop()
        return

    # Write profiling results back to MinIO as Parquet
    result_df = spark.createDataFrame(all_profiles)
    result_df.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"\nProfiling report written to: {OUTPUT_PATH}")

    # Also print a summary to stdout
    print("\n── Profile Summary ──────────────────────────────────────────")
    (
        result_df
        .select("source", "column", "dtype", "row_count", "null_rate")
        .show(50, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
