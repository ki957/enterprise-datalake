---
type: community
cohesion: 0.09
members: 31
---

# ChromaDB RAG Knowledge Base

**Cohesion:** 0.09 - loosely connected
**Members:** 31 nodes

## Members
- [[Build and compile the LangGraph supervisor.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[ChromaDB knowledge base — GraphRAG-lite with relationship traversal.  Architectu]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[Fetch ChromaDB docs for join partners of a table (1-hop graph expansion).]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[Force an immediate full reload. Called by POST apiragreload.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[GraphRAG-lite retrieval vector search + graph expansion + optional multi-query.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[LangGraph supervisor that routes user messages to specialist agents.  Routing k]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[Legacy single-query search. Still used as a building block.     Returns formatte]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[Pre-create all singletons that add cold-start latency on first request.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[Query ClickHouse for live schema info across gold, raw, staging schemas.     Ups]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[Reload if never loaded, doc content changed, or cache is stale (default 1h).]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[Replace embedded Plotly JSON blocks with a short placeholder.     Keeps agent hi]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[Return a LangGraph node that wraps a create_react_agent graph.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[Score keyword matches per agent and route to the best fit.      Intent-specific]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[Upsert all docs into ChromaDB. Safe to call multiple times.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[_docs_hash()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[_get_join_partner_docs()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[_make_runner()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[_prewarm()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[_route()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[_strip_charts()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[_sync_live_schemas()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[create_supervisor_graph()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[force_reload()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[get_collection()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[get_graph()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/server.py
- [[knowledge_base.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[maybe_reload_knowledge_base()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[pipeline_graph.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/graph/pipeline_graph.py
- [[query_knowledge()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[query_knowledge_graph()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py
- [[seed_knowledge_base()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/rag/knowledge_base.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/ChromaDB_RAG_Knowledge_Base
SORT file.name ASC
```

## Connections to other communities
- 5 edges to [[_COMMUNITY_FastAPI Server & Cost Tracking]]
- 1 edge to [[_COMMUNITY_AI Agent Tool Integration Layer]]

## Top bridge nodes
- [[_prewarm()]] - degree 5, connects to 2 communities
- [[pipeline_graph.py]] - degree 6, connects to 1 community
- [[get_graph()]] - degree 4, connects to 1 community
- [[_route()]] - degree 3, connects to 1 community
- [[Pre-create all singletons that add cold-start latency on first request.]] - degree 2, connects to 1 community