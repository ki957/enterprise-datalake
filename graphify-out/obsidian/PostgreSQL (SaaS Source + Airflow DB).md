---
source_file: "CLAUDE.md"
type: "document"
community: "AI Agent Memory & Cost System"
tags:
  - graphify/document
  - graphify/EXTRACTED
  - community/AI_Agent_Memory_&_Cost_System
---

# PostgreSQL (SaaS Source + Airflow DB)

## Connections
- [[Raw Layer (SaaS Extracts)]] - `references` [EXTRACTED]
- [[SaaS Domain (Subscriptions)]] - `references` [EXTRACTED]
- [[agent_cost_log Table (PostgreSQL)]] - `references` [EXTRACTED]
- [[ai_agent_memory Table (PostgreSQL)]] - `references` [EXTRACTED]
- [[audit_store Table (PostgreSQL)]] - `references` [EXTRACTED]
- [[psycopg2-binary==2.9.9]] - `references` [EXTRACTED]
- [[saas_pipeline.py DAG]] - `references` [EXTRACTED]

#graphify/document #graphify/EXTRACTED #community/AI_Agent_Memory_&_Cost_System