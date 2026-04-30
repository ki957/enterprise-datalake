---
source_file: "services/ai-agent/memory/cost_tracker.py"
type: "code"
community: "Agent Routing & Orchestration"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Agent_Routing_&_Orchestration
---

# cost_tracker.py — Groq token cost logger

## Connections
- [[Airflow PostgreSQL DB — shared by memory, cost, audit]] - `shares_data_with` [EXTRACTED]
- [[GET apicosts — cost analytics endpoints]] - `calls` [EXTRACTED]
- [[PostgreSQL table agent_cost_log]] - `implements` [EXTRACTED]
- [[postgres_memory.py — conversation memory (PostgreSQL)]] - `semantically_similar_to` [INFERRED]
- [[psycopg2-binary 2.9.9 — PostgreSQL driver]] - `references` [EXTRACTED]
- [[server.py — FastAPI backend (active UI)]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Agent_Routing_&_Orchestration