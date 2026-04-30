---
type: community
cohesion: 0.26
members: 12
---

# Auto Contract DAG

**Cohesion:** 0.26 - loosely connected
**Members:** 12 nodes

## Members
- [[Auto-Contract DAG ================== DAG auto_contract Schedule daily at 0900]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[For each gold table with at least one hot column (query_count = HOT_THRESHOLD),]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[Parse system.query_log for the last 24h.     For each known column in gold., co]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[Profile a table and write a rule-based GE expectation suite.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[Report columns with zero queries in last 24h as deprecation candidates.     Only]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[_ch()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[_generate_table_contract()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[_send_slack()_3]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[auto_contract_dag.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[collect_column_usage()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[flag_deprecations()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[generate_contracts()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/auto_contract_dag.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Auto_Contract_DAG
SORT file.name ASC
```
