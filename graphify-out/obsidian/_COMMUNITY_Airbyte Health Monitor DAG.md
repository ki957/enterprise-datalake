---
type: community
cohesion: 0.19
members: 14
---

# Airbyte Health Monitor DAG

**Cohesion:** 0.19 - loosely connected
**Members:** 14 nodes

## Members
- [[Airbyte Connection Health Monitor ================================== DAG airbyt]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[List the last 10 jobs across all connections for audit trail.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[Query each Airbyte connection for its latest job status and report.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[_airbyte_wait_for_job() — polls with token refresh]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[_get_airbyte_token()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[_get_airbyte_token() — OAuth2 client credentials_1]] - code - orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[_get_airbyte_token() — OAuth2 client credentials]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[_send_slack()_5]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[airbyte_connection_health DAG]] - code - orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[airbyte_health.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[check_airbyte_connections()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[check_airbyte_connections() — latest job status check]] - code - orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[list_recent_jobs()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[trigger_airbyte_mysql_sync()_1]] - code - orchestration/airflow/dags/shopflow_pipeline.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Airbyte_Health_Monitor_DAG
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_Airbyte Data Ingestion]]

## Top bridge nodes
- [[trigger_airbyte_mysql_sync()_1]] - degree 3, connects to 1 community