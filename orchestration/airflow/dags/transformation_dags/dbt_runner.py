"""
dbt Standalone Runner
======================
DAG: dbt_standalone_runner
Schedule: on-demand only (no schedule)

Provides individual dbt tasks that can be triggered manually for:
- Full refresh of specific layers (silver / gold / staging / marts)
- Running only dbt tests
- Building dbt docs and publishing to the nginx container
- Source freshness check

Use this DAG when you need to rerun dbt outside the main pipeline schedule,
e.g. after a backfill, data correction, or schema change.
"""

import json
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

DATALAKE_HOME = os.getenv("DATALAKE_HOME", "/opt/datalake")
DBT_PROJECT   = f"{DATALAKE_HOME}/transformation/dbt/datalake_transforms"
DBT_PROFILES  = "/opt/airflow/dbt_profiles.yml"
DBT_BIN       = "/home/airflow/.local/bin/dbt"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def _send_slack(message: str) -> None:
    import requests as _r
    if not SLACK_WEBHOOK_URL:
        print(f"[ALERT] {message}")
        return
    try:
        _r.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as exc:
        print(f"[ALERT - send failed: {exc}] {message}")


def _run_dbt(command: list[str], task_label: str) -> tuple[str, str | None]:
    import shutil, tempfile
    tmp_dir = tempfile.mkdtemp(prefix="dbt_run_")
    project_copy = os.path.join(tmp_dir, "project")
    try:
        shutil.copytree(DBT_PROJECT, project_copy, dirs_exist_ok=True)
        dbt_candidates = [DBT_BIN, "/usr/local/bin/dbt", "/home/airflow/.local/bin/dbt"]
        dbt_exe = next((d for d in dbt_candidates if os.path.exists(d)), "dbt")
        profiles_dir = (
            os.path.dirname(DBT_PROFILES)
            if os.path.exists(DBT_PROFILES)
            else os.path.expanduser("~/.dbt")
        )
        cmd = [dbt_exe] + command + [
            "--project-dir", project_copy,
            "--profiles-dir", profiles_dir,
            "--no-version-check",
        ]
        print(f"[{task_label}] Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        print(result.stdout[-4000:] if len(result.stdout) > 4000 else result.stdout)
        if result.returncode != 0:
            print("STDERR:", result.stderr[-2000:])
            raise RuntimeError(f"dbt failed (rc={result.returncode}): {' '.join(command)}")
        target_dir = os.path.join(project_copy, "target")
        return result.stdout, target_dir if os.path.isdir(target_dir) else None
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise


def _cleanup(target_dir: str | None) -> None:
    import shutil
    if target_dir:
        shutil.rmtree(os.path.dirname(target_dir), ignore_errors=True)


def run_full_refresh_silver(**ctx):
    """Full-refresh all silver (staging) models, bypassing incremental logic."""
    stdout, td = _run_dbt(["run", "--select", "silver", "--full-refresh"], "silver_full_refresh")
    _cleanup(td)
    print("Silver full-refresh complete.")


def run_full_refresh_gold(**ctx):
    """Full-refresh all gold (dims + facts) models."""
    stdout, td = _run_dbt(["run", "--select", "gold", "--full-refresh"], "gold_full_refresh")
    _cleanup(td)
    print("Gold full-refresh complete.")


def run_full_refresh_saas(**ctx):
    """Full-refresh SaaS staging and mart models."""
    stdout, td = _run_dbt(["run", "--select", "staging marts", "--full-refresh"], "saas_full_refresh")
    _cleanup(td)
    print("SaaS full-refresh complete.")


def run_all_tests(**ctx):
    """Run all dbt tests across every layer."""
    stdout, td = _run_dbt(["test"], "dbt_all_tests")

    counts = {"pass": 0, "error": 0}
    if td:
        results_path = os.path.join(td, "run_results.json")
        if os.path.exists(results_path):
            with open(results_path) as f:
                data = json.load(f)
            for r in data.get("results", []):
                status = r.get("status", "")
                if status in ("pass", "success"):
                    counts["pass"] += 1
                elif status in ("error", "fail"):
                    counts["error"] += 1
    _cleanup(td)

    print(f"Tests: {counts['pass']} passed, {counts['error']} failed")
    if counts["error"] > 0:
        msg = f":red_circle: *dbt Tests FAILED* — {counts['error']} failures. Check Airflow logs."
        _send_slack(msg)
        raise RuntimeError(f"dbt tests: {counts['error']} failed")

    _send_slack(f":white_check_mark: *dbt Tests PASSED* — {counts['pass']} tests")


def run_source_freshness(**ctx):
    """Check source data freshness against SLA thresholds."""
    try:
        stdout, td = _run_dbt(["source", "freshness"], "dbt_freshness")
        _cleanup(td)
        print("Source freshness check passed.")
    except RuntimeError as exc:
        print(f"[WARN] Freshness issue: {exc}")
        _send_slack(f":warning: *dbt Source Freshness Warning* — {str(exc)[:300]}")


def generate_and_publish_docs(**ctx):
    """
    Generate dbt docs and copy the manifest/catalog into the dbt-docs nginx volume.
    The dbt-docs container (port 8082) serves target/ via nginx — this refreshes it.
    """
    import shutil
    stdout, td = _run_dbt(["docs", "generate"], "dbt_docs_generate")
    if td:
        nginx_target = f"{DBT_PROJECT}/target"
        os.makedirs(nginx_target, exist_ok=True)
        for fname in ["manifest.json", "catalog.json", "index.html", "graph.gpickle"]:
            src = os.path.join(td, fname)
            dst = os.path.join(nginx_target, fname)
            if os.path.exists(src):
                shutil.copy2(src, dst)
                print(f"  Copied {fname} → {nginx_target}")
        # Also copy run_results if present
        run_results = os.path.join(td, "run_results.json")
        if os.path.exists(run_results):
            shutil.copy2(run_results, os.path.join(nginx_target, "run_results.json"))
    _cleanup(td)
    print("dbt docs generated and published to :8082.")
    _send_slack(":books: *dbt Docs* refreshed — http://localhost:8082")


default_args = {
    "owner": "shopflow-datalake",
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id="dbt_standalone_runner",
    default_args=default_args,
    description="Manual dbt runs — full-refresh, tests, docs. Trigger on demand.",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["transformation", "dbt", "manual"],
) as dag:

    t_freshness = PythonOperator(
        task_id="run_source_freshness",
        python_callable=run_source_freshness,
    )

    t_silver = PythonOperator(
        task_id="full_refresh_silver",
        python_callable=run_full_refresh_silver,
        execution_timeout=timedelta(minutes=20),
    )

    t_gold = PythonOperator(
        task_id="full_refresh_gold",
        python_callable=run_full_refresh_gold,
        execution_timeout=timedelta(minutes=20),
    )

    t_saas = PythonOperator(
        task_id="full_refresh_saas",
        python_callable=run_full_refresh_saas,
        execution_timeout=timedelta(minutes=20),
    )

    t_tests = PythonOperator(
        task_id="run_all_tests",
        python_callable=run_all_tests,
        execution_timeout=timedelta(minutes=15),
        trigger_rule="all_done",
    )

    t_docs = PythonOperator(
        task_id="generate_and_publish_docs",
        python_callable=generate_and_publish_docs,
        execution_timeout=timedelta(minutes=10),
    )

    # Full pipeline: freshness → silver → gold + saas (parallel) → tests → docs
    t_freshness >> t_silver >> [t_gold, t_saas] >> t_tests >> t_docs
