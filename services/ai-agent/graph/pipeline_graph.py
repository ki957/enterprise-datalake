"""
LangGraph supervisor that routes user messages to specialist agents.

Routing: keyword scoring — fast, no LLM call overhead for classification.
Each specialist is a create_react_agent compiled graph (LangGraph native).

History is passed as proper HumanMessage / AIMessage objects.
Plotly chart blocks are stripped from history before sending to agents
to keep the context window lean (raw Plotly JSON is ~5-10 KB per chart).

Failure guarantee: if all 3 Groq retries fail, a RAG-based fallback kicks
in so the user always gets a substantive answer, not just an error message.
"""

import re
import threading
import time
from typing import TypedDict

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, START, StateGraph

from agents.ingestion_agent import create_ingestion_agent
from agents.quality_agent import create_quality_agent
from agents.orchestration_agent import create_orchestration_agent
from agents.performance_agent import create_performance_agent
from agents.insight_agent import create_insight_agent
from agents.schema_agent import create_schema_agent
from agents.airbyte_agent import create_airbyte_agent
from agents.self_healing_agent import create_self_healing_agent
from agents.anomaly_agent import create_anomaly_agent
from agents.nl_dbt_agent import create_nl_dbt_agent
from agents.contract_agent import create_contract_agent


class AgentState(TypedDict):
    message:    str
    agent:      str
    response:   str
    history:    list   # last N {role, content} dicts from the session
    session_id: str    # passed through for downstream memory logging
    lc_messages: list  # LangGraph messages for tool-trace extraction


# ── Keyword routing ───────────────────────────────────────────────────────────

_AGENT_KEYWORDS: dict[str, list[str]] = {
    # ── Intent-specific agents (weighted 3×) ──────────────────────────────────
    "self_healing":  ["self heal", "self-heal", "self-healing", "auto fix",
                      "auto repair", "autonomous fix", "heal automatically",
                      "fix automatically", "auto-fix", "auto-heal",
                      "auto-diagnose", "heal the", "autonomously",
                      "apply the fix", "apply fix"],
    "anomaly":       ["anomaly detection", "unusual pattern", "detect anomaly",
                      "contextual anomaly", "pipeline trend", "pattern analysis",
                      "run history", "detect unusual", "trend analysis",
                      "duration spike", "consecutive failure",
                      "anomaly", "anomalies"],
    "nl_dbt":        ["generate model", "create dbt model", "write dbt",
                      "build metric", "new dbt model", "generate sql for",
                      "create a model", "write a model", "build a dbt",
                      "a dbt model", "generate a dbt", "build a new dbt",
                      "dbt model for", "new model for", "loyal customers",
                      "rolling revenue", "rolling window", "dim_loyal"],
    "contract":      ["data contract", "generate expectation", "auto contract",
                      "create expectation", "contract engine", "expectation suite",
                      "generate rules", "auto generate rules",
                      "great expectations suite", "ge suite", "audit-ready",
                      "governance auditor", "scd2 integrity"],
    # ── Infrastructure agents (weighted 1×) ───────────────────────────────────
    "airbyte":       ["airbyte", "connector", "sync job", "source connector",
                      "destination connector", "abctl", "airbyte connection"],
    "ingestion":     ["minio", "ingest", "landed", "parquet", "raw bucket",
                      "mysql cdc", "sap api", "rest api", "file landing",
                      "bronze layer", "s3 view", "files missing", "not landed",
                      "kafka", "streaming", "debezium", "enrichment pipeline"],
    "quality":       ["data quality", "dbt test", "freshness",
                      "great expectations", "orphan", "integrity",
                      "health check", "how healthy",
                      "null check", "null rate", "null rates", "null values",
                      "missing values", "missing data", "column null",
                      "data completeness", "highest null", "completeness check",
                      "null count", "data gaps", "stale data",
                      "row count check", "duplicate check", "uniqueness check"],
    "orchestration": ["airflow", "dag", "failed", "broken", "pipeline",
                      "task", "heal", "restart", "trigger", "scheduled",
                      "dag run", "task instance", "backfill", "last run",
                      "pipeline status", "did it run", "when did",
                      "schema evolution dag", "auto contract dag", "streaming enrichment dag"],
    "performance":   ["slow", "performance", "optimize", "archive", "query time",
                      "memory usage", "disk usage", "bottleneck", "latency",
                      "storage size", "table size", "how big", "disk space",
                      "storage", "disk", "ttl", "codec", "compression",
                      "largest table", "most storage", "reduce storage",
                      "compressed", "table bytes", "disk footprint",
                      # Idea 4 — column usage & deprecation
                      "column usage", "unused column", "deprecated column",
                      "never queried", "query traffic", "column_usage_stats",
                      "deprecation candidate", "which columns are used"],
    "schema":        ["schema", "column", "table structure", "describe",
                      "what columns", "model sql",
                      "data type", "primary key", "foreign key",
                      "what fields", "definition", "stg_", "dim_", "fct_",
                      # Idea 2 — schema evolution
                      "schema drift", "schema change", "column added",
                      "new column", "sources.yml", "schema evolution",
                      "schema snapshot", "column removed"],
    "insight":       ["revenue", "trend", "growth", "customer segment",
                      "product category", "vendor spend", "mrr", "churn",
                      "analytics", "kpi", "dashboard", "report", "forecast",
                      "orders", "customers", "products", "sales", "dau",
                      "active users", "saas", "subscription", "funnel",
                      "unified", "cross-domain",
                      # Idea 3 — enriched product analytics
                      "sentiment", "enriched", "category label", "quality tier",
                      "product enrichment", "mart_product_enrichment",
                      "positive product", "negative product", "premium product"],
}

# Intent-specific agents get a score multiplier so ONE keyword match beats
# TWO generic keyword matches from orchestration / quality / schema.
_AGENT_WEIGHT: dict[str, int] = {
    "self_healing": 3,
    "nl_dbt":       3,
    "contract":     2,
    "anomaly":      2,
}

_AGENT_PRIORITY = ["self_healing", "nl_dbt", "contract", "anomaly",
                   "airbyte", "quality", "orchestration", "performance",
                   "schema", "ingestion", "insight"]


def _route(state: AgentState) -> str:
    """Score keyword matches per agent and route to the best fit.

    Intent-specific agents (self_healing, nl_dbt, contract, anomaly) carry a
    weight multiplier so that explicit user intent (one domain keyword) reliably
    beats broad generic keywords from orchestration/quality/schema.
    """
    msg = state["message"].lower()
    scores = {
        agent: sum(1 for kw in kws if kw in msg) * _AGENT_WEIGHT.get(agent, 1)
        for agent, kws in _AGENT_KEYWORDS.items()
    }
    best = max(scores.values())
    if best == 0:
        return "insight"
    for agent in _AGENT_PRIORITY:
        if scores[agent] == best:
            return agent
    return "insight"


# ── Helpers ───────────────────────────────────────────────────────────────────

_PLOTLY_BLOCK = re.compile(r"```plotly\n.*?\n```", re.DOTALL)


def _strip_charts(text: str) -> str:
    """Replace embedded Plotly JSON blocks with a short placeholder.
    Keeps agent history context lean — raw Plotly JSON is 5-10 KB per chart.
    """
    return _PLOTLY_BLOCK.sub("[chart]", text).strip()


# ── Agent node wrappers ───────────────────────────────────────────────────────

_RECURSION_LIMIT = 16   # up to ~7 tool calls per agent turn — prevents runaway loops


def _make_runner(name: str, factory):
    """Return a LangGraph node that wraps a create_react_agent graph."""
    _agent = None
    _lock  = threading.Lock()

    def run(state: AgentState) -> AgentState:
        nonlocal _agent
        if _agent is None:
            with _lock:
                if _agent is None:
                    _agent = factory()

        # ── Build message list ─────────────────────────────────────────────
        # History as proper LangChain message objects; strip chart JSON to
        # prevent 5-10 KB Plotly blocks from eating the context window.
        messages: list = []
        history = state.get("history") or []
        for h in history[-4:]:  # last 2 exchanges
            raw = h.get("content", "")
            content = _strip_charts(raw)[:600]
            if h.get("role") == "human":
                messages.append(HumanMessage(content=content))
            else:
                messages.append(AIMessage(content=content))

        # ── RAG context — always inject top-1 relevant doc ─────────────────
        # Fine-grained docs mean even 1 result is focused and useful.
        # category_filter routes to the most relevant doc category per agent.
        user_msg = state["message"]
        _AGENT_RAG_CATEGORY: dict[str, str | None] = {
            "insight":       "sql_pattern",   # SQL patterns for analytics questions
            "schema":        "schema",         # Table schema docs
            "quality":       "schema",
            "performance":   "tool_format",   # Slow query format + size format
            "orchestration": "tool_format",   # DAG log parsing patterns
            "ingestion":     "tool_format",   # MinIO path structure
            "airbyte":       "tool_format",   # Airbyte output format + connection IDs
            "self_healing":  "tool_format",
            "anomaly":       "sql_pattern",
            "nl_dbt":        "sql_pattern",   # SQL patterns help generate correct ClickHouse SQL
            "contract":      "schema",        # Schema docs help generate relevant expectations
        }
        # Agents that benefit from seeing 2 docs (schema disambiguation, analytics patterns)
        _RAG_N: dict[str, int] = {
            "insight": 2, "schema": 2, "quality": 2,
        }
        try:
            from rag.knowledge_base import query_knowledge_graph
            category      = _AGENT_RAG_CATEGORY.get(name)
            n_rag         = _RAG_N.get(name, 1)
            multi_query   = name == "insight"   # multi-query + graph expansion for analytics
            rag_ctx = query_knowledge_graph(
                user_msg,
                n_results=n_rag,
                category_filter=category,
                use_multi_query=multi_query,
            )
            # Category-scoped miss → fall back to global graph search
            if not rag_ctx and category:
                rag_ctx = query_knowledge_graph(user_msg, n_results=1)
            if rag_ctx:
                user_msg = (
                    f"[Relevant context]\n{rag_ctx}\n\n"
                    f"[Question]\n{user_msg}"
                )
        except Exception:
            pass

        messages.append(HumanMessage(content=user_msg))

        # ── Invoke with retries ────────────────────────────────────────────
        # Retry strategy on "Failed to call a function" (Groq 400):
        #   attempt 0 — full context (RAG prefix + history)
        #   attempt 1 — no RAG, no history; just the user message (lighter context)
        #   attempt 2 — bare question only, no sleep; LLM still has tools
        # Rate-limit (429): sleep exponentially, keep same messages.
        # No sleep on func_fail — we're sending a simpler message, not retrying identical input.
        last_err = "max retries exceeded"
        for attempt in range(3):
            try:
                result = _agent.invoke(
                    {"messages": messages},
                    config={"recursion_limit": _RECURSION_LIMIT},
                )
                response = result["messages"][-1].content
                return {
                    **state,
                    "agent":       name,
                    "response":    response,
                    "lc_messages": result.get("messages", []),
                }

            except Exception as e:
                err_str = str(e)
                is_rate_limit = "429" in err_str or "rate limit" in err_str.lower()
                is_tpd        = "tokens per day" in err_str.lower() or "tpd" in err_str.lower()
                is_func_fail  = "Failed to call a function" in err_str

                if attempt < 2 and is_rate_limit and not is_tpd:
                    # RPM limit only — short wait, retry with same messages.
                    # TPD (daily quota) is NOT retried — sleeping 9 minutes inside the
                    # server's 75s timeout would always trigger the timeout message.
                    import re as _re
                    m = _re.search(r"try again in (\d+)m(\d+(?:\.\d+)?)s", err_str)
                    if m:
                        wait = int(m.group(1)) * 60 + float(m.group(2)) + 2
                    else:
                        wait = 5 ** (attempt + 1)   # 5s, 25s fallback
                    wait = min(wait, 60)             # cap at 60s — stay under server 75s timeout
                    time.sleep(wait)
                    continue

                if attempt < 2 and is_func_fail:
                    # Progressively strip context — no sleep needed since input changes
                    if attempt == 0:
                        # Drop RAG prefix and history, keep just the user message
                        messages = [HumanMessage(content=state["message"])]
                    else:
                        # Final simplification: bare question, no punctuation tricks
                        bare = state["message"].split("?")[0] if "?" in state["message"] else state["message"][:150]
                        messages = [HumanMessage(content=bare.strip())]
                    continue

                last_err = err_str
                break

        # ── Structured fallback — always return a clear, human-readable response ──
        import re as _re
        is_tpd_limit = "429" in last_err and ("tokens per day" in last_err.lower() or "tpd" in last_err.lower())
        is_func_fail = "Failed to call a function" in last_err
        _wait_m = _re.search(r"try again in (\d+m[\d.]+s)", last_err)
        wait_hint = f"Try again in **{_wait_m.group(1)}**." if _wait_m else "Try again in a moment."

        if is_tpd_limit:
            response = (
                "## Groq Daily Quota Reached\n\n"
                f"- **Reason:** Groq free-tier daily token limit hit\n"
                f"- **Action:** {wait_hint}\n\n"
                "> **Bottom line:** Live data is temporarily unavailable — quota resets at midnight UTC."
            )
        elif is_func_fail:
            response = (
                "## Agent Couldn't Complete the Request\n\n"
                "The AI model failed to generate a valid tool call after 3 attempts.\n\n"
                "**What to try:**\n\n"
                "1. **Rephrase** — break your question into one specific thing, e.g. _\"Show total revenue by month for the last 6 months\"_\n"
                "2. **Be explicit** — name the table if you know it, e.g. _\"query gold.fct_orders\"_\n"
                "3. **Switch agent** — use the agent selector to pick **Insight** manually\n\n"
                f"- **Technical error:** `{last_err[:180]}`\n\n"
                "> **Bottom line:** Rephrase the question more specifically and try again."
            )
        else:
            response = (
                f"## {name.title()} Agent Unavailable\n\n"
                f"- **Error:** `{last_err[:200]}`\n\n"
                "**Check that these services are running:**\n\n"
                "- ClickHouse → `http://localhost:8123`\n"
                "- Airflow → `http://localhost:8080`\n"
                "- MinIO → `http://localhost:9001`\n\n"
                "> **Bottom line:** Verify the relevant service is up, then retry."
            )

        return {
            **state,
            "agent":       name,
            "lc_messages": [],
            "response":    response,
        }

    run.__name__ = f"run_{name}"
    return run


# ── Graph assembly ────────────────────────────────────────────────────────────

def create_supervisor_graph():
    """Build and compile the LangGraph supervisor."""
    graph = StateGraph(AgentState)

    graph.add_node("ingestion",     _make_runner("ingestion",     create_ingestion_agent))
    graph.add_node("quality",       _make_runner("quality",       create_quality_agent))
    graph.add_node("orchestration", _make_runner("orchestration", create_orchestration_agent))
    graph.add_node("performance",   _make_runner("performance",   create_performance_agent))
    graph.add_node("insight",       _make_runner("insight",       create_insight_agent))
    graph.add_node("schema",        _make_runner("schema",        create_schema_agent))
    graph.add_node("airbyte",       _make_runner("airbyte",       create_airbyte_agent))
    graph.add_node("self_healing",  _make_runner("self_healing",  create_self_healing_agent))
    graph.add_node("anomaly",       _make_runner("anomaly",       create_anomaly_agent))
    graph.add_node("nl_dbt",        _make_runner("nl_dbt",        create_nl_dbt_agent))
    graph.add_node("contract",      _make_runner("contract",      create_contract_agent))

    graph.add_conditional_edges(
        START,
        _route,
        {
            "ingestion":     "ingestion",
            "quality":       "quality",
            "orchestration": "orchestration",
            "performance":   "performance",
            "insight":       "insight",
            "schema":        "schema",
            "airbyte":       "airbyte",
            "self_healing":  "self_healing",
            "anomaly":       "anomaly",
            "nl_dbt":        "nl_dbt",
            "contract":      "contract",
        },
    )

    for node in ("ingestion", "quality", "orchestration", "performance",
                 "insight", "schema", "airbyte", "self_healing",
                 "anomaly", "nl_dbt", "contract"):
        graph.add_edge(node, END)

    return graph.compile()
