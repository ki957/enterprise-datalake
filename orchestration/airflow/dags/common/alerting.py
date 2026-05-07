"""Alerting helpers shared across DAGs."""

import json
import os

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def send_slack(message: str) -> None:
    """Send a Slack message via webhook. Silently skips if URL not configured."""
    import requests as _requests
    url = SLACK_WEBHOOK_URL
    if not url:
        # Try Airflow HTTP connection as fallback
        try:
            from airflow.hooks.http_hook import HttpHook
            hook = HttpHook(http_conn_id="slack_webhook", method="POST")
            hook.run("", data=json.dumps({"text": message}),
                     headers={"Content-Type": "application/json"})
        except Exception:
            print(f"[ALERT - no Slack configured] {message}")
        return
    try:
        _requests.post(url, json={"text": message}, timeout=10)
    except Exception as exc:
        print(f"[ALERT - Slack send failed: {exc}] {message}")


def notify_failure(context) -> None:
    """on_failure_callback — fires on any task failure."""
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    log_url  = context["task_instance"].log_url
    exc      = context.get("exception", "unknown error")
    msg = (
        f":red_circle: *DAG FAILURE*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Run:* `{run_id}`\n"
        f"*Error:* {str(exc)[:300]}\n"
        f"*Logs:* {log_url}"
    )
    print(msg)
    send_slack(msg)


def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """on_sla_miss callback — fires when a task misses its SLA window."""
    task_ids = ", ".join(t.task_id for t in (task_list or []))
    msg = (
        f":warning: *SLA MISS* on DAG `{dag.dag_id}`\n"
        f"Tasks that missed SLA: `{task_ids}`"
    )
    print(msg)
    send_slack(msg)
