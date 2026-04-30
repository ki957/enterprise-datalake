---
type: community
cohesion: 0.05
members: 42
---

# Airbyte Health Monitoring

**Cohesion:** 0.05 - loosely connected
**Members:** 42 nodes

## Members
- [[Airbyte Connection Health Monitor ================================== DAG airbyt]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[List the last 10 jobs across all connections for audit trail.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[Query each Airbyte connection for its latest job status and report.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[_airbyte_wait_for_job() — polls with token refresh]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[_generate_table_contract() — GE expectation suite builder]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[_get_airbyte_token()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[_get_airbyte_token() — OAuth2 client credentials_1]] - code - orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[_get_airbyte_token() — OAuth2 client credentials]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[_run_dbt() — wraps dbt subprocess with temp dir copy]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[_send_slack()_5]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[airbyte_connection_health DAG]] - code - orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[airbyte_health.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[auto_contract DAG]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[check_airbyte_connections()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[check_airbyte_connections() — latest job status check]] - code - orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[collect_column_usage() — system.query_log analysis]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[cross_table_consistency_check() — FK referential integrity]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[data_quality_suite DAG]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[extract_saas_to_clickhouse() — parallel SaaS PG extract]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[flag_deprecations() — zero-usage column report]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[generate_contracts() — GE suite writer for hot tables]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[gold.dim_customers (ClickHouse dimension table)]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[gold.dim_users (ClickHouse SaaS user dimension)]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[gold.unified_customers (ClickHouse table)]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[governancegreat_expectationsexpectations (GE suites dir)]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[governancegreat_expectationsrun_checkpoint.py]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[list_recent_jobs()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/ingestion_dags/airbyte_health.py
- [[null_rate_check() — 5% threshold on critical columns]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[raw.column_usage_stats (ClickHouse table)]] - code - orchestration/airflow/dags/quality_dags/auto_contract_dag.py
- [[refresh_superset_dashboard() — 6 dataset IDs]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[row_count_anomaly_check() — 7-day rolling average]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[run_dbt_gold() — gold + marts layer]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[run_dbt_silver() — silver + staging layer]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[run_great_expectations() — calls run_checkpoint.py]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[shopflow_datalake_pipeline DAG]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[submit_unified_profile() — spark-submit cross-domain join]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[trigger_airbyte_mysql_sync()_1]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[trigger_airbyte_rest_sync() — JSONPlaceholder → MinIO]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[trigger_airbyte_sap_sync() — SAP OData → MinIO Parquet]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[unified_customer_profile DAG]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[unified_customer_profile.py (PySpark job)]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[wait_for_minio_files() — verifies Parquet prefixes]] - code - orchestration/airflow/dags/shopflow_pipeline.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Airbyte_Health_Monitoring
SORT file.name ASC
```

## Connections to other communities
- 3 edges to [[_COMMUNITY_Data Quality & DAG Contracts]]
- 2 edges to [[_COMMUNITY_Service Integration & Audit Core]]
- 1 edge to [[_COMMUNITY_dbt Docs & Run DAG]]

## Top bridge nodes
- [[shopflow_datalake_pipeline DAG]] - degree 11, connects to 1 community
- [[data_quality_suite DAG]] - degree 5, connects to 1 community
- [[_run_dbt() — wraps dbt subprocess with temp dir copy]] - degree 3, connects to 1 community
- [[trigger_airbyte_rest_sync() — JSONPlaceholder → MinIO]] - degree 2, connects to 1 community
- [[trigger_airbyte_sap_sync() — SAP OData → MinIO Parquet]] - degree 2, connects to 1 community