---
type: community
cohesion: 0.17
members: 12
---

# Auto Contract Logic

**Cohesion:** 0.17 - loosely connected
**Members:** 12 nodes

## Members
- [[_generate_table_contract() — GE expectation suite builder]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[auto_contract DAG]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[collect_column_usage() — system.query_log analysis]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[data_quality_suite DAG]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[flag_deprecations() — zero-usage column report]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[generate_contracts() — GE suite writer for hot tables]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[governancegreat_expectationsexpectations (GE suites dir)]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[governancegreat_expectationsrun_checkpoint.py]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[null_rate_check() — 5% threshold on critical columns]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[raw.column_usage_stats (ClickHouse table)]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[row_count_anomaly_check() — 7-day rolling average]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[run_great_expectations() — calls run_checkpoint.py]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Auto_Contract_Logic
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_Backend API & Storage Layer]]
- 1 edge to [[_COMMUNITY_Airbyte Data Ingestion]]

## Top bridge nodes
- [[data_quality_suite DAG]] - degree 5, connects to 2 communities