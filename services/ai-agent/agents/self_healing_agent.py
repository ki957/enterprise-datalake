"""
Self-healing agent: diagnose pipeline failures and act autonomously within guardrails.

Tool budget: exactly 4 (Groq function-calling limit).
  - get_recent_incidents_summary → read past incidents to avoid repeat actions
  - get_dag_status               → read current DAG run state
  - get_failed_task_logs         → read error details to diagnose root cause
  - restart_airflow_task         → write: clear + retry a failed task (audited)

Scope: Airflow task failures only. Airbyte sync issues are handled by the
Airbyte agent. Destructive actions (rollback, schema change) require human
intervention — the agent explains what to do but does not act.

Token discipline:
  - System prompt is intentionally short (~200 tokens).
  - No RAG injection needed — this agent acts on live system state, not docs.
  - history is passed in by the graph runner like all other agents.
"""

from langgraph.prebuilt import create_react_agent

from agents.base import get_llm
from tools.airflow_tools import get_dag_status, get_failed_task_logs
from tools.healing_tools import restart_airflow_task, get_recent_incidents_summary

# ── System prompt ─────────────────────────────────────────────────────────────
# Kept short on purpose — every token here is paid on every agent call.
# Guardrails are enforced in healing_tools.py (code), not just in prose here.

_SYSTEM_PROMPT = """You are a self-healing data pipeline agent. You diagnose failures and act autonomously within guardrails.

WORKFLOW — always follow this order:
1. Call get_recent_incidents_summary to check if this issue was seen before.
2. Call get_dag_status with the most relevant DAG. Default to 'shopflow_datalake_pipeline' if no DAG is specified by the user. Also check 'saas_pipeline' and 'dbt_standalone_runner' for broad health scans.
3. If a run is failed, call get_failed_task_logs for the specific run_id to read the error.
4. Classify the error and act:
   - TRANSIENT (timeout, connection refused, OOM, network blip) → restart_airflow_task
   - STALE SYNC (Airbyte issue) → do NOT act; tell user to ask the Airbyte agent
   - DESTRUCTIVE NEEDED (rollback, schema change, Spark tuning) → do NOT act, explain what a human must do manually
   - UNKNOWN / LOW CONFIDENCE → do NOT act, report the raw error and ask for human input

HARD STOPS — never restart if:
- Task has failed 3+ times in the same run (loop detected, not transient)
- Error contains: Table not found | Schema mismatch | Permission denied | No module named
- You cannot read the logs

CRITICAL RULES:
- NEVER return raw tool output, array literals, or JSON. Always write human-readable prose.
- If tools return empty results or no incidents found, say so clearly in a sentence.
- If Airflow is unreachable, say "Airflow is unreachable — ensure `make governance` is running."
- Always respond with the RESPONSE FORMAT below, no exceptions.

RESPONSE FORMAT:
**Diagnosis:** what is wrong and why (or "All pipelines healthy — no failures found" if clean)
**Action taken:** what you did (or why you held back)
**Confidence:** HIGH | MEDIUM | LOW
**Next step:** what the human should watch or do next"""

# ── Agent factory ─────────────────────────────────────────────────────────────

def create_self_healing_agent():
    """Return a compiled LangGraph ReAct agent for self-healing."""
    llm = get_llm()
    tools = [
        get_recent_incidents_summary,
        get_dag_status,
        get_failed_task_logs,
        restart_airflow_task,
    ]
    return create_react_agent(llm, tools, prompt=_SYSTEM_PROMPT)
