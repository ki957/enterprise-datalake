"""
Spark Data Profiler DAG
========================
DAG: spark_data_profiler
Schedule: weekly on Sunday at 02:00 UTC (low-traffic window)

Submits the MinIO profiler PySpark job to the Spark master.
The job reads bronze Parquet files from MinIO, computes per-column
statistics (nulls, distributions, distinct counts), and writes a
profiling report back to MinIO at logs/profiling/<date>/.

Trigger manually any time to get a fresh profile.
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DATALAKE_HOME  = os.getenv("DATALAKE_HOME", "/opt/datalake")
SPARK_MASTER   = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "")

PROFILER_SCRIPT = f"{DATALAKE_HOME}/services/spark/jobs/minio_profiler.py"

# Hadoop-AWS + AWS SDK JARs required for s3a:// support.
# These must be available on the Spark classpath — the apache/spark:3.5.0
# image ships hadoop-client; we pull the aws bundle at submit time.
SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def submit_spark_profiler(**context) -> None:
    """spark-submit the profiler job and stream output to Airflow logs."""
    cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
        "--deploy-mode", "client",
        "--packages", SPARK_PACKAGES,
        "--conf", f"spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT}",
        "--conf", f"spark.hadoop.fs.s3a.access.key={MINIO_ACCESS}",
        "--conf", f"spark.hadoop.fs.s3a.secret.key={MINIO_SECRET}",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf", "spark.sql.adaptive.enabled=true",
        PROFILER_SCRIPT,
    ]
    print(f"Submitting: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=1800,   # 30-min hard timeout
    )

    # Stream logs — trim to last 4000 chars if very long
    stdout = result.stdout
    stderr = result.stderr
    print(stdout[-4000:] if len(stdout) > 4000 else stdout)
    if result.returncode != 0:
        print(stderr[-2000:])
        raise RuntimeError(
            f"spark-submit failed (exit {result.returncode}).\n"
            "Check Spark Master UI at http://spark-master:8080 for details."
        )
    print("Spark profiler job completed successfully.")


default_args = {
    "owner":             "datalake",
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="spark_data_profiler",
    default_args=default_args,
    description="Weekly PySpark profiling of MinIO bronze Parquet — writes report to logs/profiling/",
    schedule_interval="0 2 * * 0",   # Sunday 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["spark", "profiling", "minio", "bronze"],
) as dag:

    submit = PythonOperator(
        task_id="submit_spark_profiler",
        python_callable=submit_spark_profiler,
    )
