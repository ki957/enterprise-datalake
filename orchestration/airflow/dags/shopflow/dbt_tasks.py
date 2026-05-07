"""dbt silver, gold, tests, and source freshness tasks."""

from common.alerting import send_slack
from common.helpers import cleanup_dbt_tmp, parse_dbt_results
from transformation_dags.dbt_runner import _run_dbt


def run_dbt_source_freshness(**ctx):
    """Run dbt source freshness checks — warns/errors if sources are stale."""
    try:
        _, target_dir = _run_dbt(["source", "freshness"], "dbt_freshness")
        cleanup_dbt_tmp(target_dir)
        print("Source freshness checks passed.")
    except RuntimeError as exc:
        # Freshness failures are warnings, not pipeline blockers.
        print(f"[WARN] Source freshness check raised an issue: {exc}")
        send_slack(f":warning: *Source Freshness Warning* — {str(exc)[:300]}")


def run_dbt_silver(**ctx):
    """Run dbt silver (staging) layer models — ShopFlow silver + SaaS staging."""
    _, target_dir = _run_dbt(["run", "--select", "silver staging"], "dbt_silver")
    counts  = parse_dbt_results(target_dir)
    cleanup_dbt_tmp(target_dir)
    summary = (
        f"silver: {counts.get('pass', 0)} ok, {counts.get('error', 0)} err"
        if counts else "silver: done"
    )
    ctx["ti"].xcom_push(key="dbt_silver_result", value=summary)
    print(f"Silver result: {summary}")


def run_dbt_gold(**ctx):
    """Run dbt gold layer models — ShopFlow gold + SaaS marts."""
    _, target_dir = _run_dbt(["run", "--select", "gold marts"], "dbt_gold")
    counts  = parse_dbt_results(target_dir)
    cleanup_dbt_tmp(target_dir)
    summary = (
        f"gold: {counts.get('pass', 0)} ok, {counts.get('error', 0)} err"
        if counts else "gold: done"
    )
    ctx["ti"].xcom_push(key="dbt_gold_result", value=summary)
    print(f"Gold result: {summary}")


def run_dbt_tests(**ctx):
    """Run dbt data quality tests on silver and gold layers."""
    stdout, target_dir = _run_dbt(["test", "--select", "silver gold"], "dbt_test")
    counts = parse_dbt_results(target_dir)
    cleanup_dbt_tmp(target_dir)

    if counts:
        passed = counts.get("pass", 0)
        failed = counts.get("error", 0)
        warned = counts.get("warn", 0)
    else:
        passed = sum(1 for ln in stdout.splitlines() if " PASS " in ln)
        failed = sum(1 for ln in stdout.splitlines() if " FAIL " in ln or " ERROR " in ln)
        warned = 0

    ctx["ti"].xcom_push(key="dbt_tests_passed", value=passed)
    ctx["ti"].xcom_push(key="dbt_tests_failed", value=failed)
    ctx["ti"].xcom_push(key="dbt_tests_warned", value=warned)

    print(f"dbt tests: {passed} passed, {warned} warnings, {failed} failed")
    if failed > 0:
        raise RuntimeError(f"dbt tests failed: {failed} error(s). Check Airflow logs for details.")
