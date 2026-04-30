"""
ShopFlow Data Lake AI Agent — Chat-first Streamlit UI.
"""

import sys
import uuid

sys.path.insert(0, "/app")

import json
import re

import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def render_response(text: str) -> None:
    """Render an agent response that may contain embedded Plotly chart blocks.

    Splits on  ```plotly\\n...\\n```  fences.  Text segments are rendered as
    markdown; chart segments are parsed as Plotly JSON and displayed with
    st.plotly_chart().  Falls back to a code block if parsing fails.
    """
    parts = re.split(r"```plotly\n(.*?)\n```", text, flags=re.DOTALL)
    for i, part in enumerate(parts):
        if i % 2 == 0:          # text segment
            if part.strip():
                st.markdown(part)
        else:                    # plotly JSON segment
            try:
                fig = go.Figure(json.loads(part))
                st.plotly_chart(fig, use_container_width=True)
            except Exception:
                st.code(part, language="json")

from graph.pipeline_graph import create_supervisor_graph, _route, AgentState
from memory.postgres_memory import save_message, clear_session, get_history, cleanup_old_sessions
from rag.knowledge_base import maybe_reload_knowledge_base

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Enterprise Data Lake Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Wider chat area */
.main .block-container { max-width: 900px; padding-top: 1rem; }

/* Agent chip buttons */
div[data-testid="stHorizontalBlock"] .stButton > button {
    border-radius: 20px;
    font-size: 0.78rem;
    padding: 0.3rem 0.8rem;
    white-space: nowrap;
    font-weight: 500;
}

/* Active agent chip highlight */
.active-agent button {
    border: 2px solid #ff4b4b !important;
    background-color: #fff5f5 !important;
}

/* Chat message spacing */
.stChatMessage { margin-bottom: 0.25rem; }

/* Agent label inside message */
.agent-label {
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    opacity: 0.7;
    margin-bottom: 4px;
}

/* Suggestion chips */
.suggestion-chip {
    display: inline-block;
    background: #f0f2f6;
    border-radius: 16px;
    padding: 0.3rem 0.75rem;
    margin: 0.2rem;
    font-size: 0.82rem;
    cursor: pointer;
}
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
AGENTS = {
    "auto":         {"icon": "🤖", "label": "Auto",         "color": "gray"},
    "insight":      {"icon": "💬", "label": "Insight",       "color": "green"},
    "quality":      {"icon": "🏥", "label": "Quality",       "color": "blue"},
    "ingestion":    {"icon": "🔄", "label": "Ingestion",     "color": "green"},
    "orchestration":{"icon": "⚙️", "label": "Orchestration", "color": "orange"},
    "performance":  {"icon": "⚡", "label": "Performance",   "color": "red"},
    "schema":       {"icon": "🗂️", "label": "Schema",        "color": "blue"},
    "airbyte":      {"icon": "🔌", "label": "Airbyte",       "color": "orange"},
}

QUICK_ACTIONS = {
    "Data Engineer": [
        ("📋 Pipeline SLA check",
         "Did the shopflow_datalake_pipeline DAG complete within 30 minutes today? Show start and end time."),
        ("🔍 Data freshness audit",
         "Show row counts for all gold tables and the min and max order_date available in fct_orders. Flag any table with zero rows."),
        ("🔗 Airbyte sync health",
         "List all Airbyte connections, show their status and the result of the latest sync job for each."),
        ("🧪 Gold layer quality check",
         "Check data quality on gold tables: count nulls in fct_orders customer_key and product_key, check for duplicate order_ids, and count active dim_customers where is_current=1."),
        ("📉 Row count anomaly check",
         "Show current row counts for all gold tables. Flag any table with fewer than 50 rows as critical and any below 200 rows as a warning."),
        ("🗄️ MinIO storage audit",
         "List all MinIO buckets and show the total file count and size for each bucket. Flag any bucket over 100MB."),
    ],
    "Data Scientist": [
        ("📈 Revenue trend + growth",
         "Show monthly revenue for the last 6 months with month-over-month growth rate as a percentage."),
        ("🎯 Customer segment analysis",
         "Break down total revenue and order count by customer segment. Which segment is most valuable?"),
        ("🛒 Product category performance",
         "Which product categories generate the most revenue? Show top 5 with average order value."),
        ("🌍 Geographic revenue map",
         "Show total revenue by country, top 15. Include average order value per country."),
        ("💰 Vendor spend analysis",
         "Who are the top 10 vendors by procurement spend? Show total amount and number of purchase orders."),
        ("📊 SaaS MRR & churn",
         "Show MRR trend and churn rate from the SaaS gold layer. Is revenue growing or shrinking?"),
    ],
    "Operations": [
        ("🩺 Full stack health check",
         "Run a complete health check: check the Airflow DAG status, gold table row counts, and MinIO file counts."),
        ("🐛 Debug last pipeline failure",
         "Check if the shopflow_datalake_pipeline had any failures recently. If yes, show the error logs."),
        ("⚡ Slow query report",
         "List all ClickHouse queries that took more than 1 second today. Suggest optimizations."),
        ("🔌 Airbyte connection audit",
         "Show all Airbyte connections. For each active one, show the last sync status and record count."),
        ("📦 Schema inspection",
         "Describe all tables in the gold schema — show columns, types, and row counts in one report."),
        ("🔁 Trigger full pipeline",
         "Trigger the shopflow_datalake_pipeline DAG to run now and confirm it started."),
    ],
}

# ── Session init ──────────────────────────────────────────────────────────────
if "session_id" not in st.session_state:
    # Full UUID — 128-bit random session identifier
    st.session_state.session_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    # Reload conversation history from PostgreSQL so chat survives browser refreshes
    try:
        rows = get_history(st.session_state.session_id, limit=100)
        st.session_state.messages = [
            {"role": r["role"], "content": r["content"], "agent": r.get("agent", "")}
            for r in rows
        ]
    except Exception:
        st.session_state.messages = []
if "locked_agent" not in st.session_state:
    st.session_state.locked_agent = "auto"
if "graph" not in st.session_state:
    with st.spinner("Warming up agents…"):
        try:
            # Seed or reload docs — upserts changed content, skips unchanged
            maybe_reload_knowledge_base()
        except Exception:
            pass
        st.session_state.graph = create_supervisor_graph()
if "rag_last_check" not in st.session_state:
    st.session_state.rag_last_check = 0
if "cleanup_last_run" not in st.session_state:
    # Prune messages older than 30 days — runs once per session on startup
    try:
        cleanup_old_sessions(days=30)
    except Exception:
        pass
    st.session_state.cleanup_last_run = True

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("## 🤖 Enterprise Data Lake Agent")
st.caption("Powered by LangGraph · Groq Llama-4-Scout · ChromaDB RAG · PostgreSQL Memory")

# ── Interactive Agent Selector ────────────────────────────────────────────────
st.markdown("**Route to agent:**")
chip_cols = st.columns(len(AGENTS))
for col, (key, info) in zip(chip_cols, AGENTS.items()):
    with col:
        is_active = st.session_state.locked_agent == key
        label = f"{'✓ ' if is_active else ''}{info['icon']} {info['label']}"
        if st.button(label, key=f"chip_{key}", use_container_width=True,
                     type="primary" if is_active else "secondary"):
            st.session_state.locked_agent = key
            st.rerun()

# Show current routing mode
locked = st.session_state.locked_agent
if locked == "auto":
    st.caption("🤖 **Auto-routing** — supervisor picks the best agent for each question")
else:
    info = AGENTS[locked]
    st.caption(f"{info['icon']} **Locked to {info['label']} Agent** — click Auto to release")

st.divider()

# ── Chat history ──────────────────────────────────────────────────────────────
for msg in st.session_state.messages:
    role = msg["role"]
    with st.chat_message(role):
        if role == "assistant":
            agent_key = msg.get("agent", "")
            if agent_key and agent_key in AGENTS:
                a = AGENTS[agent_key]
                st.markdown(
                    f"<div class='agent-label'>{a['icon']} {a['label']} Agent</div>",
                    unsafe_allow_html=True,
                )
        render_response(msg["content"])

# ── Empty state: show suggestion tabs ────────────────────────────────────────
if not st.session_state.messages:
    st.markdown("#### 💡 What would you like to know?")
    tabs = st.tabs(list(QUICK_ACTIONS.keys()))
    for tab, (category, actions) in zip(tabs, QUICK_ACTIONS.items()):
        with tab:
            cols = st.columns(2)
            for i, (label, question) in enumerate(actions):
                with cols[i % 2]:
                    if st.button(label, key=f"qa_{category}_{i}", use_container_width=True):
                        st.session_state.pending_q = question
                        st.rerun()
    st.divider()

# ── Input ─────────────────────────────────────────────────────────────────────
# Dynamic placeholder based on locked agent
placeholders = {
    "auto":          "Ask anything — e.g. 'Which customers drove the most revenue last month?'",
    "insight":       "Ask a business question — e.g. 'Top 10 countries by revenue this quarter'",
    "quality":       "Ask about data quality — e.g. 'Are there nulls in fct_orders customer_key?'",
    "ingestion":     "Ask about data ingestion — e.g. 'Did all Airbyte files land in MinIO today?'",
    "orchestration": "Ask about pipelines — e.g. 'Why did the DAG fail last night?'",
    "performance":   "Ask about performance — e.g. 'What queries are slowing down ClickHouse?'",
    "schema":        "Ask about schema — e.g. 'What columns does dim_customers have?'",
    "airbyte":       "Ask about Airbyte — e.g. 'Show all connections and last sync status'",
}

prompt = st.session_state.pop("pending_q", None) or st.chat_input(
    placeholders.get(locked, "Ask anything about your data lake…")
)

# ── Quick-action bar (visible once chat starts) ───────────────────────────────
if st.session_state.messages:
    with st.expander("💡 Quick actions", expanded=False):
        q_tabs = st.tabs(list(QUICK_ACTIONS.keys()))
        for q_tab, (category, actions) in zip(q_tabs, QUICK_ACTIONS.items()):
            with q_tab:
                q_cols = st.columns(3)
                for i, (label, question) in enumerate(actions):
                    with q_cols[i % 3]:
                        if st.button(label, key=f"qa2_{category}_{i}", use_container_width=True):
                            st.session_state.pending_q = question
                            st.rerun()

# ── Process input ─────────────────────────────────────────────────────────────
# Reload RAG knowledge base at most once every 6 hours (non-blocking)
import time as _time
if _time.time() - st.session_state.rag_last_check > 21600:
    try:
        maybe_reload_knowledge_base(max_age_seconds=21600)
        st.session_state.rag_last_check = _time.time()
    except Exception:
        pass

if prompt:
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    try:
        save_message(st.session_state.session_id, "user", prompt)
    except Exception:
        pass

    # Determine target agent
    if locked != "auto":
        target_agent = locked
    else:
        target_agent = None  # graph supervisor decides

    with st.chat_message("assistant"):
        # Show which agent is being invoked while waiting for the LLM
        agent_status_msgs = {
            "insight":       "📊 Querying ClickHouse analytics layer…",
            "quality":       "🏥 Running data quality checks…",
            "ingestion":     "🔄 Checking MinIO / source ingestion…",
            "orchestration": "⚙️ Inspecting Airflow DAGs…",
            "performance":   "⚡ Analysing slow queries & storage…",
            "schema":        "🗂️ Fetching schema & dbt models…",
            "airbyte":       "🔌 Querying Airbyte connections…",
        }
        # Determine which agent will likely handle this (pre-route for status)
        from graph.pipeline_graph import _route, AgentState
        _preview_state: AgentState = {"message": prompt, "agent": "", "response": "", "history": [], "session_id": ""}
        _predicted_agent = _route(_preview_state) if locked == "auto" else locked
        _status_msg = agent_status_msgs.get(_predicted_agent, "🤖 Thinking…")

        with st.spinner(_status_msg):
            try:
                # If locked to a specific agent, hint the graph via message prefix
                message = prompt
                if locked != "auto":
                    # Inject a keyword that guarantees routing to the locked agent
                    routing_hint = {
                        "airbyte":       "airbyte connection: ",
                        "ingestion":     "minio sync files: ",
                        "quality":       "data quality test rows: ",
                        "orchestration": "airflow dag pipeline: ",
                        "performance":   "slow query performance: ",
                        "schema":        "schema describe columns: ",
                        "insight":       "",
                    }
                    message = routing_hint.get(locked, "") + prompt

                # Pass recent conversation turns so agents handle follow-ups
                recent_history = [
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state.messages[-6:]  # last 3 exchanges
                ]
                result = st.session_state.graph.invoke({
                    "message":    message,
                    "agent":      "",
                    "response":   "",
                    "history":    recent_history,
                    "session_id": st.session_state.session_id,
                })
                agent_used = result.get("agent", "insight")
                response = result.get("response", "No response.")

                # Show agent label
                if agent_used in AGENTS:
                    a = AGENTS[agent_used]
                    st.markdown(
                        f"<div class='agent-label'>{a['icon']} {a['label']} Agent</div>",
                        unsafe_allow_html=True,
                    )

                render_response(response)

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response,
                    "agent": agent_used,
                })

                try:
                    save_message(
                        st.session_state.session_id, "assistant",
                        response, agent=agent_used,
                    )
                except Exception:
                    pass

            except Exception as e:
                err = f"❌ Error: {str(e)}"
                st.error(err)
                st.session_state.messages.append({"role": "assistant", "content": err})

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📊 Session")
    st.markdown(f"**ID:** `{st.session_state.session_id}`")
    st.markdown(f"**Messages:** {len(st.session_state.messages)}")
    st.markdown(f"**Mode:** {'Auto-route' if locked == 'auto' else AGENTS[locked]['label'] + ' locked'}")

    st.divider()

    st.markdown("### 🗺️ Agent Map")
    st.markdown("""
| Agent | Best for |
|---|---|
| 🤖 Auto | Let AI decide |
| 💬 Insight | Revenue, trends, KPIs |
| 🏥 Quality | Nulls, tests, row counts |
| 🔄 Ingestion | MinIO files, source syncs |
| ⚙️ Orchestration | DAG failures, retries |
| ⚡ Performance | Slow queries, storage |
| 🗂️ Schema | Columns, types, dbt SQL |
| 🔌 Airbyte | Connections, sync jobs |
    """)

    st.divider()

    st.markdown("### 🔗 Services")
    st.markdown("""
[🌀 Airflow](http://localhost:8080) &nbsp;|&nbsp;
[🔌 Airbyte](http://localhost:8000)

[📦 MinIO](http://localhost:9001) &nbsp;|&nbsp;
[🏠 ClickHouse](http://localhost:8123/play)

[📊 Grafana](http://localhost:3000) &nbsp;|&nbsp;
[📈 Superset](http://localhost:8088)

[📚 dbt Docs](http://localhost:8082)
    """)

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("🗑️ Clear", use_container_width=True):
            try:
                clear_session(st.session_state.session_id)
            except Exception:
                pass
            st.session_state.messages = []
            st.session_state.locked_agent = "auto"
            st.rerun()

    st.divider()
    st.caption("LangGraph 0.2.76 · Groq Llama-4-Scout-17b · ChromaDB RAG · PostgreSQL Memory")
