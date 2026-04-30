---
type: community
cohesion: 0.04
members: 68
---

# Airbyte Data Ingestion

**Cohesion:** 0.04 - loosely connected
**Members:** 68 nodes

## Members
- [[Airbyte MySQL CDC → MinIO Parquet connection]] - code - scripts/setup_airbyte_phase2.py
- [[Airbyte OAuth2 token cache (55-min TTL)]] - code - services/ai-agent/tools/airbyte_tools.py
- [[Airflow REST API]] - document - services/ai-agent/tools/healing_tools.py
- [[ClickHouse bronze schema (S3 views over MinIO)]] - code - scripts/setup_clickhouse_bronze.py
- [[ClickHouse gold schema]] - document - transformation/dbt/datalake_transforms/models/gold
- [[GE Suite Template (_SUITE_TEMPLATE)]] - code - services/ai-agent/tools/contract_tools.py
- [[Great Expectations expectations directory]] - document - governance/great_expectations/expectations
- [[MinIO logsprofiling output (Parquet)]] - document - services/spark/jobs/minio_profiler.py
- [[MinIO raw bucket — Parquet landing zone]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[PostgreSQL table agent_audit_log]] - code - services/ai-agent/memory/audit_store.py
- [[PostgreSQL table agent_incidents]] - code - services/ai-agent/memory/audit_store.py
- [[PostgreSQL table agent_pending_actions]] - code - services/ai-agent/memory/audit_store.py
- [[SAP vendorPOcost-center synthetic data]] - document - services/sap-api/data_generator.py
- [[Self-Healing Guardrail Table (_GUARDRAILS)]] - code - services/ai-agent/tools/healing_tools.py
- [[_normalise_sql (SQL sanitizer function)]] - code - services/ai-agent/tools/dbt_write_tools.py
- [[_run_dbt() — wraps dbt subprocess with temp dir copy]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[airbyte_tools.py — Airbyte OAuth2 API tools]] - code - services/ai-agent/tools/airbyte_tools.py
- [[airflow_tools.py — Airflow REST API tools]] - code - services/ai-agent/tools/airflow_tools.py
- [[audit_store.py — self-healing action audit log]] - code - services/ai-agent/memory/audit_store.py
- [[contract_tools.py_1]] - code - services/ai-agent/tools/contract_tools.py
- [[create_dbt_model (tool)]] - code - services/ai-agent/tools/dbt_write_tools.py
- [[cross_table_consistency_check() — FK referential integrity]] - code - orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[dbt modelsmarts directory]] - document - transformation/dbt/datalake_transforms/models/marts
- [[dbt_tools.py_1]] - code - services/ai-agent/tools/dbt_tools.py
- [[dbt_write_tools.py_1]] - code - services/ai-agent/tools/dbt_write_tools.py
- [[extract_saas_to_clickhouse() — parallel SaaS PG extract]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[generate_mysql_data.py (ShopFlow synthetic data generator)]] - code - scripts/generate_mysql_data.py
- [[generate_saas_data.py (SaaS PostgreSQL synthetic data generator)]] - code - scripts/generate_saas_data.py
- [[get_airflow_run_history (tool)]] - code - services/ai-agent/tools/profiling_tools.py
- [[get_dag_status() @tool]] - code - services/ai-agent/tools/airflow_tools.py
- [[get_recent_incidents_summary (tool)]] - code - services/ai-agent/tools/healing_tools.py
- [[gold.dim_customers (ClickHouse dimension table)]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[gold.dim_users (ClickHouse SaaS user dimension)]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[gold.unified_customers (ClickHouse table)]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[gold.unified_customers (ClickHouse table)_1]] - document - services/spark/jobs/unified_customer_profile.py
- [[healing_tools.py_1]] - code - services/ai-agent/tools/healing_tools.py
- [[ingest_http_to_minio.py (SAP API + JSONPlaceholder → MinIO Parquet)]] - code - scripts/ingest_http_to_minio.py
- [[ingest_mysql_to_minio.py (MySQL ShopFlow → MinIO Parquet bypass)]] - code - scripts/ingest_mysql_to_minio.py
- [[list_airbyte_connections() @tool]] - code - services/ai-agent/tools/airbyte_tools.py
- [[list_minio_files (tool)]] - code - services/ai-agent/tools/minio_tools.py
- [[minio_profiler.py (PySpark profiling job)]] - code - orchestration/airflow/dags/transformation_dags/spark_profiler.py
- [[minio_profiler.py (Spark job)]] - code - services/spark/jobs/minio_profiler.py
- [[minio_tools.py_1]] - code - services/ai-agent/tools/minio_tools.py
- [[profile_table_stats (tool)]] - code - services/ai-agent/tools/profiling_tools.py
- [[profiling_tools.py_1]] - code - services/ai-agent/tools/profiling_tools.py
- [[refresh_superset_dashboard() — 6 dataset IDs]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[request_approval (tool)]] - code - services/ai-agent/tools/healing_tools.py
- [[restart_airflow_task (tool)]] - code - services/ai-agent/tools/healing_tools.py
- [[run_dbt_gold() — gold + marts layer]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[run_dbt_silver() — silver + staging layer]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[sap-apiapp.py]] - code - services/sap-api/app.py
- [[sap-apidata_generator.py]] - code - services/sap-api/data_generator.py
- [[setup_airbyte_http_sources.py (configure SAP + REST Airbyte sources)]] - code - scripts/setup_airbyte_http_sources.py
- [[setup_airbyte_phase2.py (configure Airbyte MySQL CDC → MinIO)]] - code - scripts/setup_airbyte_phase2.py
- [[setup_clickhouse_bronze.py (creates bronze S3 views in ClickHouse)]] - code - scripts/setup_clickhouse_bronze.py
- [[shopflow_datalake_pipeline DAG]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[spark_data_profiler DAG]] - code - orchestration/airflow/dags/transformation_dags/spark_profiler.py
- [[submit_spark_profiler() — spark-submit minio_profiler.py]] - code - orchestration/airflow/dags/transformation_dags/spark_profiler.py
- [[submit_unified_profile() — spark-submit cross-domain join]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[trigger_airbyte_rest_sync() — JSONPlaceholder → MinIO]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[trigger_airbyte_sap_sync() — SAP OData → MinIO Parquet]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[trigger_airbyte_sync_safe (tool)]] - code - services/ai-agent/tools/healing_tools.py
- [[trigger_dag() @tool]] - code - services/ai-agent/tools/airflow_tools.py
- [[unified_customer_profile DAG]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[unified_customer_profile.py (PySpark job)]] - code - orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[unified_customer_profile.py (Spark job)]] - code - services/spark/jobs/unified_customer_profile.py
- [[wait_for_minio_files() — verifies Parquet prefixes]] - code - orchestration/airflow/dags/shopflow_pipeline.py
- [[write_expectations (tool)]] - code - services/ai-agent/tools/contract_tools.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Airbyte_Data_Ingestion
SORT file.name ASC
```

## Connections to other communities
- 3 edges to [[_COMMUNITY_Agent Routing & Orchestration]]
- 2 edges to [[_COMMUNITY_Backend API & Storage Layer]]
- 1 edge to [[_COMMUNITY_dbt Transformation Runner]]
- 1 edge to [[_COMMUNITY_Airbyte Health Monitor DAG]]
- 1 edge to [[_COMMUNITY_Auto Contract Logic]]

## Top bridge nodes
- [[shopflow_datalake_pipeline DAG]] - degree 11, connects to 2 communities
- [[audit_store.py — self-healing action audit log]] - degree 15, connects to 1 community
- [[_run_dbt() — wraps dbt subprocess with temp dir copy]] - degree 3, connects to 1 community
- [[cross_table_consistency_check() — FK referential integrity]] - degree 2, connects to 1 community