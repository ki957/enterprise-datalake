import os
import requests
from langchain_core.tools import tool


def _base() -> str:
    return os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")


def _auth() -> tuple[str, str]:
    return (
        os.getenv("AIRFLOW_USER", "admin"),
        os.getenv("AIRFLOW_PASSWORD", ""),
    )


@tool
def get_dag_status(dag_id: str, limit: int = 5) -> str:
    """Get recent run history for an Airflow DAG, including failed task logs.
    Returns up to `limit` runs ordered by most recent first.
    Automatically fetches task logs for any failed runs — no separate log call needed."""
    try:
        resp = requests.get(
            f"{_base()}/api/v1/dags/{dag_id}/dagRuns",
            auth=_auth(),
            params={"limit": min(limit, 10), "order_by": "-start_date"},
            timeout=10,
        )
        resp.raise_for_status()
        runs = resp.json().get("dag_runs", [])
        if not runs:
            return f"No runs found for DAG '{dag_id}'."

        from datetime import datetime
        lines = [f"DAG: {dag_id} — {len(runs)} run(s)\n"]
        for r in runs:
            start = (r.get("start_date") or "n/a")[:19]
            end   = (r.get("end_date") or "running")[:19]
            duration = ""
            if r.get("start_date") and r.get("end_date"):
                try:
                    s = datetime.fromisoformat(r["start_date"].replace("Z", "+00:00"))
                    e = datetime.fromisoformat(r["end_date"].replace("Z", "+00:00"))
                    secs = int((e - s).total_seconds())
                    duration = f" ({secs//60}m {secs%60}s)"
                except Exception:
                    pass
            lines.append(
                f"  [{r['state'].upper()}] {r['dag_run_id']}\n"
                f"    Started: {start} | Ended: {end}{duration}"
            )

            # Always auto-fetch logs for failed runs — avoids multi-step tool chaining
            if r.get("state") == "failed":
                run_id = r["dag_run_id"]
                try:
                    ti_resp = requests.get(
                        f"{_base()}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
                        auth=_auth(),
                        timeout=10,
                    )
                    ti_resp.raise_for_status()
                    tasks = ti_resp.json().get("task_instances", [])
                    failed = [t for t in tasks if t["state"] == "failed"]
                    for task in failed[:2]:
                        log_resp = requests.get(
                            f"{_base()}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
                            f"/taskInstances/{task['task_id']}/logs/1",
                            auth=_auth(),
                            timeout=15,
                        )
                        lines.append(
                            f"    ↳ Failed task: {task['task_id']}\n"
                            f"      Log (last 400 chars): {log_resp.text[-400:]}"
                        )
                except Exception as log_err:
                    lines.append(f"    ↳ Could not fetch task logs: {log_err}")

        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching DAG status: {str(e)}"


@tool
def list_all_dags() -> str:
    """List all Airflow DAGs with their active/paused state.
    Use for a pipeline overview."""
    try:
        resp = requests.get(
            f"{_base()}/api/v1/dags", auth=_auth(), timeout=10
        )
        resp.raise_for_status()
        dags = resp.json().get("dags", [])
        lines = [
            f"  {d['dag_id']}: {'PAUSED' if d['is_paused'] else 'ACTIVE'}"
            for d in dags
        ]
        return "All DAGs:\n" + "\n".join(lines)
    except Exception as e:
        return f"Error listing DAGs: {str(e)}"


@tool
def trigger_dag(dag_id: str) -> str:
    """Trigger an Airflow DAG to run immediately.
    Use when pipeline is stale or needs restarting."""
    try:
        resp = requests.post(
            f"{_base()}/api/v1/dags/{dag_id}/dagRuns",
            auth=_auth(),
            json={"conf": {"triggered_by": "ai_agent"}},
            timeout=10,
        )
        resp.raise_for_status()
        run_id = resp.json().get("dag_run_id", "unknown")
        return f"DAG '{dag_id}' triggered. Run ID: {run_id}"
    except Exception as e:
        return f"Error triggering DAG: {str(e)}"


@tool
def get_failed_task_logs(dag_id: str, run_id: str) -> str:
    """Get logs from failed Airflow tasks. Provide dag_id and run_id."""
    try:
        resp = requests.get(
            f"{_base()}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
            auth=_auth(),
            timeout=10,
        )
        resp.raise_for_status()
        tasks = resp.json().get("task_instances", [])
        failed = [t for t in tasks if t["state"] == "failed"]
        if not failed:
            return "No failed tasks in this run."
        results = []
        for task in failed[:2]:
            log_resp = requests.get(
                f"{_base()}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
                f"/taskInstances/{task['task_id']}/logs/1",
                auth=_auth(),
                timeout=15,
            )
            results.append(
                f"Task: {task['task_id']}\n"
                f"Log (last 500 chars):\n{log_resp.text[-500:]}"
            )
        return "\n---\n".join(results)
    except Exception as e:
        return f"Error fetching task logs: {str(e)}"
