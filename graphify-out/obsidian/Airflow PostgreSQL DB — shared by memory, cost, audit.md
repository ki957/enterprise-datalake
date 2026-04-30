---
source_file: "services/ai-agent/memory/postgres_memory.py"
type: "code"
community: "Agent Routing & Orchestration"
location: "line 38"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Agent_Routing_&_Orchestration
---

# Airflow PostgreSQL DB — shared by memory, cost, audit

## Connections
- [[audit_store.py — self-healing action audit log]] - `shares_data_with` [EXTRACTED]
- [[cost_tracker.py — Groq token cost logger]] - `shares_data_with` [EXTRACTED]
- [[postgres_memory.py — conversation memory (PostgreSQL)]] - `shares_data_with` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Agent_Routing_&_Orchestration