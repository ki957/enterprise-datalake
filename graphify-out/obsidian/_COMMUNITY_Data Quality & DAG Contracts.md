---
type: community
cohesion: 0.05
members: 52
---

# Data Quality & DAG Contracts

**Cohesion:** 0.05 - loosely connected
**Members:** 52 nodes

## Members
- [[._call_insert()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_datetime_formatted_without_microseconds()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_decimal_serialised_as_float_string()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_empty_rows_skips_http_call()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_multiple_rows_each_on_separate_line()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_null_values_serialised_as_backslash_N()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_tab_in_value_is_replaced()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[Airflow XCom Push Contract (usersevents counts)]] - code - tests/integration/test_saas_pipeline_e2e.py
- [[DAG Integrity Tests]] - code - tests/test_dag_integrity.py
- [[DAG Retries Required Invariant]] - code - tests/test_dag_integrity.py
- [[DAG catchup=False Invariant]] - code - tests/test_dag_integrity.py
- [[Data Generator Unit Tests]] - code - tests/test_data_generators.py
- [[Development & Testing Dependencies (requirements-dev.txt)]] - code - requirements-dev.txt
- [[Diff gold SaaS mart row counts against previous run.     Writes results to pipel]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[Helper call _ch_insert and return the body sent to ClickHouse.]] - rationale - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[Pipeline Extract Idempotency Invariant]] - code - tests/integration/test_saas_pipeline_e2e.py
- [[Rationale Custom Dockerfile.airflow bakes dbtpyarrowminio for reproducibility]] - document - docs/PROJECT_DOCUMENTATION.md
- [[SaaS Data Pipeline =================== DAG saas_data_pipeline Schedule daily a]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[SaaS Pipeline End-to-End Integration Tests]] - code - tests/integration/test_saas_pipeline_e2e.py
- [[SaaS Pipeline Logic Unit Tests]] - code - tests/test_saas_pipeline_logic.py
- [[Slack webhook alerting (_send_slack pattern)]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[TSV Serialisation Invariants (null as N, datetime strip microseconds, Decimal as float)]] - code - tests/test_saas_pipeline_logic.py
- [[TestChInsert]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[Tests for the _ch_insert TSV serialisation helper.]] - rationale - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[Vault KV2 API Contract (data envelope, X-Vault-Token header)]] - code - tests/test_data_generators.py
- [[_ch()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[_ch_insert TSV Serialisation Helper]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[_ch_insert()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[_send_slack()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[data_quality_check Function]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[data_quality_check()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[extract_postgres_to_clickhouse Function]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[extract_postgres_to_clickhouse()_1]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[generate_mysql_data Script]] - code - scripts/generate_mysql_data.py
- [[generate_saas_data Script]] - code - scripts/generate_saas_data.py
- [[notify_failure()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[notify_success()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[observability_check()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[observability_check() — gold table row count diff]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[pipeline_metadata.observability_runs (ClickHouse table)]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[raw.saas_events (ClickHouse table)]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[raw.saas_subscriptions (ClickHouse table)]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[raw.saas_users (ClickHouse table)]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[run_dbt_saas_marts()_1]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[run_dbt_saas_marts()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[run_dbt_saas_staging()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[run_dbt_saas_staging()_1]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[saas_data_pipeline DAG]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[saas_pipeline.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[setup_vault_secrets Script]] - code - scripts/setup_vault_secrets.py
- [[transformationdbtdatalake_transforms (dbt project)]] - code - orchestration/airflow/dags/saas_pipeline.py
- [[vault_write Function]] - code - scripts/setup_vault_secrets.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Data_Quality_&_DAG_Contracts
SORT file.name ASC
```

## Connections to other communities
- 3 edges to [[_COMMUNITY_Integration Test Infrastructure]]
- 3 edges to [[_COMMUNITY_Airbyte Health Monitoring]]
- 2 edges to [[_COMMUNITY_dbt Pipeline & RAG Reload]]
- 1 edge to [[_COMMUNITY_ClickHouse Bronze View Creator]]
- 1 edge to [[_COMMUNITY_Architecture Decisions & Constraints]]

## Top bridge nodes
- [[saas_pipeline.py]] - degree 11, connects to 1 community
- [[saas_data_pipeline DAG]] - degree 11, connects to 1 community
- [[TestChInsert]] - degree 9, connects to 1 community
- [[SaaS Pipeline End-to-End Integration Tests]] - degree 7, connects to 1 community
- [[_ch_insert()]] - degree 5, connects to 1 community