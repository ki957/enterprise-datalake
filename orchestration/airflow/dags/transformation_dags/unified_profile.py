"""
Unified Customer Profile DAG
==============================
DAG: unified_customer_profile
Schedule: daily at 08:00 UTC — runs after both domain pipelines complete
  shopflow_datalake_pipeline  → 06:00 UTC
  saas_data_pipeline          → 06:30 UTC
  unified_customer_profile    → 08:00 UTC  (allows ~1-1.5h for upstream to finish)

Submits the PySpark cross-domain join job that bridges:
  gold.dim_customers (ShopFlow) ✕ gold.dim_users (SaaS)

Output lands in gold.unified_customers (ClickHouse) and
s3a://gold/unified_customers/<date>/ (MinIO Parquet snapshot).
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DATALAKE_HOME   = os.getenv("DATALAKE_HOME", "/opt/datalake")
SPARK_MASTER    = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "")
CH_HOST         = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_HTTP_PORT    = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CH_USER         = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD     = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")

JOB_SCRIPT = f"{DATALAKE_HOME}/services/spark/jobs/unified_customer_profile.py"

# S3A + ClickHouse JDBC packages — resolved at submit time from Maven Central
SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "com.clickhouse:clickhouse-jdbc:0.4.6",
    "org.apache.httpcomponents.client5:httpclient5:5.2.1",
])


def submit_unified_profile(**context) -> None:
    cmd = [
        "spark-submit",
        "--master",      SPARK_MASTER,
        "--deploy-mode", "client",
        "--packages",    SPARK_PACKAGES,
        # S3A / MinIO
        "--conf", f"spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT}",
        "--conf", f"spark.hadoop.fs.s3a.access.key={MINIO_ACCESS}",
        "--conf", f"spark.hadoop.fs.s3a.secret.key={MINIO_SECRET}",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf", "spark.sql.adaptive.enabled=true",
        JOB_SCRIPT,
    ]

    env = {
        "CLICKHOUSE_HOST":     CH_HOST,
        "CLICKHOUSE_HTTP_PORT": CH_HTTP_PORT,
        "CLICKHOUSE_USER":     CH_USER,
        "CLICKHOUSE_PASSWORD": CH_PASSWORD,
        "MINIO_ENDPOINT":      MINIO_ENDPOINT,
        "MINIO_ACCESS_KEY":    MINIO_ACCESS,
        "MINIO_SECRET_KEY":    MINIO_SECRET,
    }

    print(f"Submitting Spark job: {JOB_SCRIPT}")

    import os as _os
    full_env = {**_os.environ, **env}

    result = subprocess.run(
        cmd,
        env=full_env,
        capture_output=True,
        text=True,
        timeout=3600,
    )

    stdout = result.stdout
    stderr = result.stderr
    print(stdout[-4000:] if len(stdout) > 4000 else stdout)

    if result.returncode != 0:
        print(stderr[-2000:])
        raise RuntimeError(
            f"Unified profile Spark job failed (exit {result.returncode}).\n"
            "Check Spark Master UI: http://spark-master:8080"
        )

    # Push row counts from stdout to XCom for observability
    for line in stdout.splitlines():
        if "Total unified records" in line or "cross_domain" in line:
            print(f"[METRIC] {line.strip()}")

    print("Unified customer profile job completed successfully.")


default_args = {
    "owner":             "datalake",
    "retries":           1,
    "retry_delay":       timedelta(minutes=15),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="unified_customer_profile",
    default_args=default_args,
    description="Cross-domain Spark join: ShopFlow customers × SaaS users → gold.unified_customers",
    schedule_interval="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["spark", "gold", "cross-domain", "shopflow", "saas"],
) as dag:

    submit = PythonOperator(
        task_id="submit_unified_profile",
        python_callable=submit_unified_profile,
    )
