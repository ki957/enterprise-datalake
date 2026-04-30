---
type: community
cohesion: 0.40
members: 6
---

# RAG Document Pipelines

**Cohesion:** 0.40 - moderately connected
**Members:** 6 nodes

## Members
- [[PIPELINE_DOCS — architecture and service URL docs]] - code - services/ai-agent/rag/knowledge_base.py
- [[RELATIONSHIP_DOCS — FK join pattern docs]] - code - services/ai-agent/rag/knowledge_base.py
- [[SQL_PATTERN_DOCS — ClickHouse SQL pattern docs]] - code - services/ai-agent/rag/knowledge_base.py
- [[TABLE_DOCS — static schema doc set for RAG]] - code - services/ai-agent/rag/knowledge_base.py
- [[TOOL_FORMAT_DOCS — tool output format docs]] - code - services/ai-agent/rag/knowledge_base.py
- [[seed_knowledge_base() — ChromaDB upsert]] - code - services/ai-agent/rag/knowledge_base.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/RAG_Document_Pipelines
SORT file.name ASC
```
