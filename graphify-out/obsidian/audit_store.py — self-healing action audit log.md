---
source_file: "services/ai-agent/memory/audit_store.py"
type: "code"
community: "Airbyte Data Ingestion"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Airbyte_Data_Ingestion
---

# audit_store.py — self-healing action audit log

## Connections
- [[Airflow PostgreSQL DB — shared by memory, cost, audit]] - `shares_data_with` [EXTRACTED]
- [[PostgreSQL table agent_audit_log]] - `implements` [EXTRACTED]
- [[PostgreSQL table agent_incidents]] - `implements` [EXTRACTED]
- [[PostgreSQL table agent_pending_actions]] - `implements` [EXTRACTED]
- [[contract_tools.py_1]] - `calls` [EXTRACTED]
- [[create_dbt_model (tool)]] - `calls` [EXTRACTED]
- [[dbt_write_tools.py_1]] - `calls` [EXTRACTED]
- [[get_recent_incidents_summary (tool)]] - `calls` [EXTRACTED]
- [[healing_tools.py_1]] - `calls` [EXTRACTED]
- [[postgres_memory.py — conversation memory (PostgreSQL)]] - `semantically_similar_to` [INFERRED]
- [[psycopg2-binary 2.9.9 — PostgreSQL driver]] - `references` [EXTRACTED]
- [[request_approval (tool)]] - `calls` [EXTRACTED]
- [[restart_airflow_task (tool)]] - `calls` [EXTRACTED]
- [[trigger_airbyte_sync_safe (tool)]] - `calls` [EXTRACTED]
- [[write_expectations (tool)]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Airbyte_Data_Ingestion