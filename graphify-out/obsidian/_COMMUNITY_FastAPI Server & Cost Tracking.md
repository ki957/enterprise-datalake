---
type: community
cohesion: 0.06
members: 56
---

# FastAPI Server & Cost Tracking

**Cohesion:** 0.06 - loosely connected
**Members:** 56 nodes

## Members
- [[Agent cost tracking — logs token usage and computes Groq API spend per call.  Ta]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[AgentState]] - code - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[BaseModel]] - code
- [[ChatRequest]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[Context manager that borrows a connection from the pool and returns it.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[Create memory table + index the first time this module is used.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[Daily spend aggregated by date and agent for the last N days.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[Daily spend by date + agent (for the time-series chart).]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[Delete messages older than `days` days.     Call periodically to prevent unbound]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[FastAPI backend for DataLake AI v2 — replaces the Streamlit app.py.  Runs on por]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[Force an immediate ChromaDB re-seed + clear response cache.     Safe to call at]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[Insert one row into agent_cost_log. Silently swallows DB errors.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[Most recent N individual calls.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[Most recent individual calls for the call log table.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[Overall + per-agent tokencost summary for the last N days.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[Overall totals + per-agent breakdown for the last N days.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[PostgreSQL-backed conversation memory. Reuses the existing Airflow PostgreSQL in]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[Return cost breakdown for a single call.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[Return the last `limit` messages for a session, oldest first.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[Stream response in 25-char chunks at 6ms cadence.     300-word response (~1 500]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[TypedDict]] - code
- [[_cache_key()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[_conn()_1]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[_conn()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[_ensure_table()_1]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[_ensure_table()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[_get_cached()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[_get_pool()_1]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[_get_pool()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[_log_cost()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[_persist_memory()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[_ping()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[_set_cached()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[chat()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[chunk_stream()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[cleanup_old_sessions()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[clear_session()_1]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[clear_session()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[compute_cost()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[cost_tracker.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[costs_daily()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[costs_recent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[costs_summary()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[get_daily_costs()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[get_history()_1]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[get_history()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[get_recent_calls()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[get_summary()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[health()_1]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[lifespan()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[log_call()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/cost_tracker.py
- [[postgres_memory.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[rag_reload()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[save_message()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/postgres_memory.py
- [[search_history()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[server.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/FastAPI_Server_&_Cost_Tracking
SORT file.name ASC
```

## Connections to other communities
- 5 edges to [[_COMMUNITY_ChromaDB RAG Knowledge Base]]
- 2 edges to [[_COMMUNITY_SAP OData Flask API]]
- 1 edge to [[_COMMUNITY_Audit & Governance Store]]

## Top bridge nodes
- [[AgentState]] - degree 12, connects to 2 communities
- [[server.py]] - degree 21, connects to 1 community
- [[chat()]] - degree 4, connects to 1 community
- [[search_history()]] - degree 3, connects to 1 community