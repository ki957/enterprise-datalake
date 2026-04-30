---
type: community
cohesion: 0.23
members: 17
---

# dbt Docs & Run DAG

**Cohesion:** 0.23 - loosely connected
**Members:** 17 nodes

## Members
- [[Check source data freshness against SLA thresholds.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Full-refresh SaaS staging and mart models.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Full-refresh all gold (dims + facts) models.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Full-refresh all silver (staging) models, bypassing incremental logic.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Generate dbt docs and copy the manifestcatalog into the dbt-docs nginx volume.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Run all dbt tests across every layer.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[_cleanup()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[_run_dbt()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[_send_slack()_2]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[dbt Standalone Runner ====================== DAG dbt_standalone_runner Schedule]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[dbt_runner.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[generate_and_publish_docs()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[run_all_tests()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[run_full_refresh_gold()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[run_full_refresh_saas()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[run_full_refresh_silver()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[run_source_freshness()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/dbt_Docs_&_Run_DAG
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_dbt Pipeline & RAG Reload]]
- 1 edge to [[_COMMUNITY_Airbyte Health Monitoring]]

## Top bridge nodes
- [[_run_dbt()_1]] - degree 9, connects to 2 communities