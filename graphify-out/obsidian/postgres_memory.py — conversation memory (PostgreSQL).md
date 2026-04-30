---
source_file: "services/ai-agent/memory/postgres_memory.py"
type: "code"
community: "Agent Routing & Orchestration"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Agent_Routing_&_Orchestration
---

# postgres_memory.py — conversation memory (PostgreSQL)

## Connections
- [[Airflow PostgreSQL DB — shared by memory, cost, audit]] - `shares_data_with` [EXTRACTED]
- [[PostgreSQL table ai_agent_memory]] - `implements` [EXTRACTED]
- [[app.py — Streamlit Chat UI (legacy)]] - `calls` [EXTRACTED]
- [[audit_store.py — self-healing action audit log]] - `semantically_similar_to` [INFERRED]
- [[cost_tracker.py — Groq token cost logger]] - `semantically_similar_to` [INFERRED]
- [[psycopg2-binary 2.9.9 — PostgreSQL driver]] - `references` [EXTRACTED]
- [[server.py — FastAPI backend (active UI)]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Agent_Routing_&_Orchestration