---
type: community
cohesion: 0.07
members: 45
---

# MySQL to MinIO Bronze Ingest

**Cohesion:** 0.07 - loosely connected
**Members:** 45 nodes

## Members
- [[Ingest JSONPlaceholder REST API → MinIO Parquet (users, posts, comments).]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Ingest SAP OData API → MinIO Parquet (vendors, purchase_orders, cost_centers).]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Log pipeline completion summary and send Slack success notification.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Parse dbt run_results.json for structured passfailerror counts.     Falls back]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Refresh all 6 Superset datasets so charts reflect latest Gold data.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Remove the dbt temp directory (parent of target_dir).]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Run a dbt command using the project's dbt installation.      Returns         (s]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Run dbt data quality tests on silver and gold layers.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Run dbt gold layer models — ShopFlow gold + SaaS marts.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Run dbt silver (staging) layer models — ShopFlow silver + SaaS staging.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Run dbt source freshness checks — warnserrors if sources are stale.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Send a Slack message via webhook. Silently skips if URL not configured.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[ShopFlow Enterprise Data Lake Pipeline ====================================== DA]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Trigger Airbyte MySQL → MinIO CDC sync and wait for completion.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Verify all expected Parquet prefixes exist in MinIO raw bucket.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[_airbyte_trigger_sync()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[_airbyte_wait_for_job()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[_cleanup_dbt_tmp()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[_get_airbyte_token()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[_parse_run_results()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[_run_dbt()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[_send_slack()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[extract_customers()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_mysql_to_minio.py
- [[extract_orders()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_mysql_to_minio.py
- [[extract_products()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_mysql_to_minio.py
- [[get_minio()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_mysql_to_minio.py
- [[get_mysql()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_mysql_to_minio.py
- [[ingest_mysql_to_minio.py]] - code - /home/kishore/enterprise-datalake/scripts/ingest_mysql_to_minio.py
- [[main()_2]] - code - /home/kishore/enterprise-datalake/scripts/ingest_mysql_to_minio.py
- [[notify_failure()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[notify_sla_miss()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[notify_success()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[on_failure_callback — fires on any task failure.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[on_sla_miss callback — fires when a task misses its SLA window.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[refresh_superset_dashboard()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[run_dbt_gold()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[run_dbt_silver()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[run_dbt_source_freshness()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[run_dbt_tests()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[shopflow_pipeline.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[trigger_airbyte_mysql_sync()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[trigger_airbyte_rest_sync()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[trigger_airbyte_sap_sync()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[upload()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_mysql_to_minio.py
- [[wait_for_minio_files()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/MySQL_to_MinIO_Bronze_Ingest
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_Spark Unified Customer Profile]]
- 1 edge to [[_COMMUNITY_dbt Pipeline & RAG Reload]]
- 1 edge to [[_COMMUNITY_ClickHouse Bronze View Creator]]

## Top bridge nodes
- [[shopflow_pipeline.py]] - degree 23, connects to 3 communities