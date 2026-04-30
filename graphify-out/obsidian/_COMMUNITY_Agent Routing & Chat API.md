---
type: community
cohesion: 0.07
members: 40
---

# Agent Routing & Chat API

**Cohesion:** 0.07 - loosely connected
**Members:** 40 nodes

## Members
- [[AgentState TypedDict]] - code - services/ai-agent/graph/pipeline_graph.py
- [[Airflow PostgreSQL DB — shared by memory, cost, audit]] - code - services/ai-agent/memory/postgres_memory.py
- [[ChromaDB collection shopflow_knowledge_v3]] - code - services/ai-agent/rag/knowledge_base.py
- [[GET apicosts — cost analytics endpoints]] - code - services/ai-agent/server.py
- [[Groq retry logic — 3 attempts with context stripping]] - code - services/ai-agent/graph/pipeline_graph.py
- [[POST apichat — SSE streaming chat endpoint]] - code - services/ai-agent/server.py
- [[POST apiragreload — force ChromaDB re-seed]] - code - services/ai-agent/server.py
- [[PostgreSQL table agent_cost_log]] - code - services/ai-agent/memory/cost_tracker.py
- [[PostgreSQL table ai_agent_memory]] - code - services/ai-agent/memory/postgres_memory.py
- [[_AGENT_KEYWORDS — routing keyword table]] - code - services/ai-agent/graph/pipeline_graph.py
- [[_SCHEMA_GRAPH — FK join graph for RAG expansion]] - code - services/ai-agent/rag/knowledge_base.py
- [[_make_runner() — lazy agent node factory]] - code - services/ai-agent/graph/pipeline_graph.py
- [[_resp_cache — in-memory 5-min response cache]] - code - services/ai-agent/server.py
- [[_route() — keyword-score router]] - code - services/ai-agent/graph/pipeline_graph.py
- [[_sync_live_schemas() — live ClickHouse schema sync]] - code - services/ai-agent/rag/knowledge_base.py
- [[app.py — Streamlit Chat UI (legacy)]] - code - services/ai-agent/app.py
- [[chart_tools.py — Plotly chart generation tool]] - code - services/ai-agent/tools/chart_tools.py
- [[check_slow_queries() @tool]] - code - services/ai-agent/tools/clickhouse_tools.py
- [[chromadb 0.5.23 — vector store for RAG]] - document - services/ai-agent/requirements.txt
- [[clickhouse-driver 0.2.9 — ClickHouse native protocol client]] - document - services/ai-agent/requirements.txt
- [[clickhouse_tools.py — ClickHouse query tools]] - code - services/ai-agent/tools/clickhouse_tools.py
- [[cost_tracker.py — Groq token cost logger]] - code - services/ai-agent/memory/cost_tracker.py
- [[create_chart() @tool — Plotly chart generation]] - code - services/ai-agent/tools/chart_tools.py
- [[create_supervisor_graph() — LangGraph compiler]] - code - services/ai-agent/graph/pipeline_graph.py
- [[describe_table() @tool]] - code - services/ai-agent/tools/clickhouse_tools.py
- [[knowledge_base.py — ChromaDB GraphRAG knowledge base]] - code - services/ai-agent/rag/knowledge_base.py
- [[langchain 0.3.25 — LLM abstraction layer]] - document - services/ai-agent/requirements.txt
- [[langchain-groq 0.3.2 — Groq LLM provider]] - document - services/ai-agent/requirements.txt
- [[langgraph 0.2.76 — agent graph orchestration]] - document - services/ai-agent/requirements.txt
- [[minio 7.2.0 — MinIO S3 client]] - document - services/ai-agent/requirements.txt
- [[pipeline_graph.py — LangGraph Supervisor]] - code - services/ai-agent/graph/pipeline_graph.py
- [[plotly — interactive chart rendering]] - document - services/ai-agent/requirements.txt
- [[postgres_memory.py — conversation memory (PostgreSQL)]] - code - services/ai-agent/memory/postgres_memory.py
- [[psycopg2-binary 2.9.9 — PostgreSQL driver]] - document - services/ai-agent/requirements.txt
- [[query_clickhouse() @tool]] - code - services/ai-agent/tools/clickhouse_tools.py
- [[query_knowledge_graph() — GraphRAG-lite retrieval]] - code - services/ai-agent/rag/knowledge_base.py
- [[requirements.txt — Python dependencies]] - document - services/ai-agent/requirements.txt
- [[server.py — FastAPI backend (active UI)]] - code - services/ai-agent/server.py
- [[streamlit 1.44.0 — legacy chat UI]] - document - services/ai-agent/requirements.txt
- [[test_agents.py — smoke test for nl_dbt and contract agents]] - code - services/ai-agent/test_agents.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Agent_Routing_&_Chat_API
SORT file.name ASC
```

## Connections to other communities
- 3 edges to [[_COMMUNITY_Service Integration & Audit Core]]

## Top bridge nodes
- [[postgres_memory.py — conversation memory (PostgreSQL)]] - degree 7, connects to 1 community
- [[psycopg2-binary 2.9.9 — PostgreSQL driver]] - degree 4, connects to 1 community
- [[Airflow PostgreSQL DB — shared by memory, cost, audit]] - degree 3, connects to 1 community