---
source_file: "services/ai-agent/app.py"
type: "code"
community: "Agent Routing & Orchestration"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Agent_Routing_&_Orchestration
---

# app.py — Streamlit Chat UI (legacy)

## Connections
- [[_route() — keyword-score router]] - `calls` [EXTRACTED]
- [[create_chart() @tool — Plotly chart generation]] - `shares_data_with` [EXTRACTED]
- [[create_supervisor_graph() — LangGraph compiler]] - `calls` [EXTRACTED]
- [[knowledge_base.py — ChromaDB GraphRAG knowledge base]] - `calls` [EXTRACTED]
- [[postgres_memory.py — conversation memory (PostgreSQL)]] - `calls` [EXTRACTED]
- [[server.py — FastAPI backend (active UI)]] - `semantically_similar_to` [INFERRED]
- [[streamlit 1.44.0 — legacy chat UI]] - `references` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Agent_Routing_&_Orchestration