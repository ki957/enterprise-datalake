---
type: community
cohesion: 0.21
members: 14
---

# Schema Evolution DAG

**Cohesion:** 0.21 - loosely connected
**Members:** 14 nodes

## Members
- [[Compare MySQL information_schema.columns against Postgres schema_snapshots.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[Create schema_snapshots table in Postgres if it doesn't exist.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[For each new column detected, append it to the appropriate sources.yml.     Uses]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[POST to the AI Agent FastAPI server to force ChromaDB re-seed.     Runs only whe]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[Run dbt compile to verify sources.yml is valid.     Rolls back sources.yml if co]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[Schema Evolution DAG ===================== DAG schema_evolution Schedule daily]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[_ensure_snapshot_table()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[_pg_conn()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[_send_slack()_6]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[reload_agent_rag()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[schema_evolution_dag.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[snapshot_mysql_schema()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[update_sources_yml()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[validate_dbt_compile()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/schema_evolution_dag.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Schema_Evolution_DAG
SORT file.name ASC
```
