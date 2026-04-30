"""
Anomaly detection agent — LLM-based contextual pipeline anomaly detection (#4).

Unlike Prometheus threshold alerts, this agent understands context:
  - "row count is low, but today is Sunday — that's normal"
  - "Tuesday always runs 20% faster than average — today it's 3x slower"
  - "fct_orders has been growing 5% week-over-week — sudden 40% drop is an anomaly"

Tool budget: 4 (Groq limit)
  1. get_airflow_run_history  — DAG run timeline (duration, state, frequency)
  2. get_table_row_counts     — current row counts per schema
  3. query_clickhouse         — time-series queries for trend analysis
  4. create_chart             — visualise anomalies

System prompt is kept short — the LLM's analysis quality comes from structured
data, not long prompts. We feed clean time-series and let the model reason.
"""

from langgraph.prebuilt import create_react_agent

from agents.base import get_llm
from tools.profiling_tools import get_airflow_run_history
from tools.clickhouse_tools import get_table_row_counts, query_clickhouse
from tools.chart_tools import create_chart


_SYSTEM_PROMPT = """You are a pipeline anomaly detection agent. You find contextually unusual patterns — not simple threshold breaches.

AVAILABLE TOOLS ONLY: get_airflow_run_history, get_table_row_counts, query_clickhouse, create_chart.
NEVER call list_all_dags or any other tool — it does not exist in your tool set.

KNOWN DAGS (call get_airflow_run_history directly for each):
  shopflow_datalake_pipeline | saas_data_pipeline | data_quality_suite
  unified_customer_profile   | airbyte_connection_health | spark_data_profiler

WORKFLOW:
1. Call get_airflow_run_history for the most relevant DAG(s). For "all pipelines" questions,
   call it for shopflow_datalake_pipeline and saas_data_pipeline (the two main ones).
2. Call get_table_row_counts("gold") to see current state.
3. Call query_clickhouse to get historical trend data from the ACTUAL ClickHouse tables.
   Use the gold schema tables returned by step 2. For example:
     SELECT toDate(order_date) as day, count() as orders FROM gold.fct_orders
     WHERE order_date >= today() - 30 GROUP BY day ORDER BY day
   Or for user activity:
     SELECT activity_date, active_users FROM gold.fct_daily_active_users
     WHERE activity_date >= today() - 30 ORDER BY activity_date
   Do NOT query pipeline_metadata.observability_runs — that table does not exist.
   If any query fails, note that and proceed with the data you have.
4. Identify anomalies using these rules:
   - Compare today vs same-weekday average from the last 4 weeks
   - Flag if deviation > 25% from weekday baseline
   - Flag if DAG run duration is > 2x the 7-day average
   - Flag if 3+ consecutive failures in run history
5. Call create_chart("line", ...) to visualise the trend if anomaly found.

CONTEXT RULES — do NOT flag as anomaly:
- Weekend row counts 15–30% lower than weekdays (expected pattern)
- First run of the month having more records (monthly data loads)
- A run taking longer right after a schema migration

RESPONSE FORMAT:
**Anomalies found:** list each with severity (HIGH/MEDIUM/LOW)
**Normal patterns confirmed:** what looks healthy
**Recommended action:** what to investigate or fix
**Confidence:** HIGH | MEDIUM | LOW (based on data available)"""


def create_anomaly_agent():
    """Return a compiled LangGraph ReAct agent for contextual anomaly detection."""
    llm   = get_llm()
    tools = [
        get_airflow_run_history,
        get_table_row_counts,
        query_clickhouse,
        create_chart,
    ]
    return create_react_agent(llm, tools, prompt=_SYSTEM_PROMPT)
