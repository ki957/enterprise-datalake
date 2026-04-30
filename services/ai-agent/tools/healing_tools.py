"""
Write tools for the self-healing agent.

Every tool that mutates system state:
  1. Checks the GUARDRAILS dict — blocked actions are rejected outright.
  2. Executes the action.
  3. Writes an audit record via audit_store regardless of outcome.
  4. Creates a structured incident record on success.

Tools that are too risky for autonomous execution use request_approval()
to park the action in agent_pending_actions for human review.

Token note: tool docstrings are sent to Groq as part of the function schema.
Keep them short — the LLM reads every word on every call.
"""

import os
import requests
from langchain_core.tools import tool

from memory.audit_store import log_action, create_incident, queue_for_approval, get_recent_incidents

# ── Guardrail table ───────────────────────────────────────────────────────────
# auto_act=True  → agent may execute immediately
# auto_act=False → must call request_approval() instead
# blocked=True   → never execute, return an error message

_GUARDRAILS: dict[str, dict] = {
    "restart_airflow_task":   {"auto_act": True},
    "trigger_airbyte_sync":   {"auto_act": True},
    "rollback_dbt_model":     {"auto_act": False},
    "switch_fallback_source": {"auto_act": False},
    "scale_spark_executor":   {"auto_act": False},
    "drop_table":             {"auto_act": False, "blocked": True},
    "delete_data":            {"auto_act": False, "blocked": True},
}


def _is_allowed(action: str) -> tuple[bool, str]:
    """Return (allowed, reason). Checks guardrail before any write."""
    rule = _GUARDRAILS.get(action)
    if rule is None:
        return False, f"Action '{action}' is not in the guardrail allowlist."
    if rule.get("blocked"):
        return False, f"Action '{action}' is permanently blocked — requires manual intervention."
    if not rule.get("auto_act"):
        return False, f"Action '{action}' requires human approval. Use request_approval tool."
    return True, "ok"


# ── Airflow helpers ───────────────────────────────────────────────────────────

def _af_base() -> str:
    return os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")


def _af_auth() -> tuple[str, str]:
    return (
        os.getenv("AIRFLOW_USER", "admin"),
        os.getenv("AIRFLOW_PASSWORD", ""),
    )


# ── Tools ─────────────────────────────────────────────────────────────────────

@tool
def restart_airflow_task(dag_id: str, run_id: str, task_id: str) -> str:
    """Clear and retry a specific failed Airflow task without triggering a full DAG run.
    Use for transient failures: timeouts, connection refused, OOM. Do NOT use if the
    task has already failed 3+ times or if the error is a schema/permission issue."""
    action = "restart_airflow_task"
    target = {"dag_id": dag_id, "run_id": run_id, "task_id": task_id}

    allowed, reason = _is_allowed(action)
    if not allowed:
        log_action(action, target, "blocked", reasoning=reason)
        return f"🚫 Blocked: {reason}"

    try:
        resp = requests.post(
            f"{_af_base()}/api/v1/dags/{dag_id}/clearTaskInstances",
            auth=_af_auth(),
            json={
                "dry_run":    False,
                "task_ids":   [task_id],
                "dag_run_id": run_id,
            },
            timeout=10,
        )
        if resp.status_code in (200, 201):
            log_action(action, target, "success", confidence=1.0,
                       reasoning=f"Cleared task {task_id} in {dag_id}/{run_id}")
            create_incident(
                entity=dag_id,
                problem=f"Task '{task_id}' failed",
                action_taken=f"Cleared and re-queued '{task_id}' (run: {run_id})",
            )
            return (
                f"✅ Task `{task_id}` cleared and queued for retry.\n"
                f"DAG: `{dag_id}` | Run: `{run_id}`\n"
                f"Monitor at: {_af_base()}/dags/{dag_id}/grid"
            )
        else:
            err = resp.text[:200]
            log_action(action, target, "failed", error=f"HTTP {resp.status_code}: {err}")
            return f"❌ Airflow returned HTTP {resp.status_code}: {err}"

    except requests.exceptions.ConnectionError:
        err = "Airflow unreachable"
        log_action(action, target, "error", error=err)
        return f"❌ {err} — is the governance stack running? (`make governance`)"
    except Exception as e:
        log_action(action, target, "error", error=str(e))
        return f"❌ Unexpected error: {e}"


@tool
def trigger_airbyte_sync_safe(connection_id: str, reason: str) -> str:
    """Trigger an Airbyte sync for a connection. Provide the connection UUID and a brief
    reason why the sync is needed (logged to audit trail). Use when source data is stale
    and the last sync job failed or is older than 24 hours."""
    action = "trigger_airbyte_sync"
    target = {"connection_id": connection_id, "reason": reason}

    allowed, block_reason = _is_allowed(action)
    if not allowed:
        log_action(action, target, "blocked", reasoning=block_reason)
        return f"🚫 Blocked: {block_reason}"

    try:
        from tools.airbyte_tools import _base, _headers, KNOWN_CONNECTIONS
        resp = requests.post(
            f"{_base()}/api/v1/connections/sync",
            headers=_headers(),
            json={"connectionId": connection_id},
            timeout=15,
        )
        if resp.status_code in (404, 502, 503):
            msg = (
                f"Cannot trigger sync (HTTP {resp.status_code}). "
                "Airbyte may be down — use Airflow DAG 'shopflow_datalake_pipeline' instead."
            )
            log_action(action, target, "failed", error=msg)
            return f"❌ {msg}"

        resp.raise_for_status()
        job = resp.json().get("job", {})
        conn_name = KNOWN_CONNECTIONS.get(connection_id, connection_id[:8])
        log_action(action, target, "success", confidence=1.0, reasoning=reason)
        create_incident(
            entity=f"airbyte:{connection_id[:8]}",
            problem=reason,
            action_taken=f"Triggered sync job {job.get('id', 'n/a')} for {conn_name}",
        )
        return (
            f"✅ Sync triggered for **{conn_name}**\n"
            f"Job ID: `{job.get('id', 'n/a')}` | Status: `{job.get('status', 'pending')}`\n"
            f"Reason: {reason}"
        )
    except requests.exceptions.ConnectionError:
        err = "Airbyte (k8s) unreachable"
        log_action(action, target, "error", error=err)
        return f"❌ {err} — check: `kubectl get pods -n airbyte-abctl`"
    except Exception as e:
        log_action(action, target, "error", error=str(e))
        return f"❌ Unexpected error: {e}"


@tool
def request_approval(action: str, target_description: str, reasoning: str) -> str:
    """Queue a high-risk or irreversible action for human approval.
    Use for: rollback_dbt_model, switch_fallback_source, scale_spark_executor.
    Provide the action name, a plain-English description of the target, and your reasoning."""
    target = {"description": target_description}
    pending_id = queue_for_approval(action, target, reasoning)
    log_action(action, target, "queued_for_approval", reasoning=reasoning)
    return (
        f"⏳ Action **`{action}`** queued for human approval (ID: #{pending_id}).\n\n"
        f"**Target:** {target_description}\n"
        f"**Reasoning:** {reasoning}\n\n"
        "A human must approve before this action executes. "
        "Check `agent_pending_actions` table in PostgreSQL."
    )


@tool
def get_recent_incidents_summary() -> str:
    """Return the last 10 incidents detected and acted on by the self-healing agent.
    Use to understand recent pipeline health and avoid repeating the same fix."""
    incidents = get_recent_incidents(10)
    if not incidents:
        return "No incidents recorded yet — the self-healing agent hasn't acted on anything."
    lines = ["**Recent incidents (last 10):**\n"]
    for inc in incidents:
        status = "✅ Resolved" if inc["resolved"] else "🔴 Open"
        action = inc["action_taken"] or "no action taken"
        lines.append(
            f"- `{inc['entity']}` | {inc['problem']}\n"
            f"  → {action} | {status} | {inc['created_at'][:16]}"
        )
    return "\n".join(lines)
