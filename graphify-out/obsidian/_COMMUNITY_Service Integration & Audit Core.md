---
type: community
cohesion: 0.05
members: 52
---

# Service Integration & Audit Core

**Cohesion:** 0.05 - loosely connected
**Members:** 52 nodes

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
- [[airbyte_tools.py — Airbyte OAuth2 API tools]] - code - services/ai-agent/tools/airbyte_tools.py
- [[airflow_tools.py — Airflow REST API tools]] - code - services/ai-agent/tools/airflow_tools.py
- [[audit_store.py — self-healing action audit log]] - code - services/ai-agent/memory/audit_store.py
- [[contract_tools.py_1]] - code - services/ai-agent/tools/contract_tools.py
- [[create_dbt_model (tool)]] - code - services/ai-agent/tools/dbt_write_tools.py
- [[dbt modelsmarts directory]] - document - transformation/dbt/datalake_transforms/models/marts
- [[dbt_tools.py_1]] - code - services/ai-agent/tools/dbt_tools.py
- [[dbt_write_tools.py_1]] - code - services/ai-agent/tools/dbt_write_tools.py
- [[generate_mysql_data.py (ShopFlow synthetic data generator)]] - code - scripts/generate_mysql_data.py
- [[generate_saas_data.py (SaaS PostgreSQL synthetic data generator)]] - code - scripts/generate_saas_data.py
- [[get_airflow_run_history (tool)]] - code - services/ai-agent/tools/profiling_tools.py
- [[get_dag_status() @tool]] - code - services/ai-agent/tools/airflow_tools.py
- [[get_recent_incidents_summary (tool)]] - code - services/ai-agent/tools/healing_tools.py
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
- [[request_approval (tool)]] - code - services/ai-agent/tools/healing_tools.py
- [[restart_airflow_task (tool)]] - code - services/ai-agent/tools/healing_tools.py
- [[sap-apiapp.py]] - code - services/sap-api/app.py
- [[sap-apidata_generator.py]] - code - services/sap-api/data_generator.py
- [[setup_airbyte_http_sources.py (configure SAP + REST Airbyte sources)]] - code - scripts/setup_airbyte_http_sources.py
- [[setup_airbyte_phase2.py (configure Airbyte MySQL CDC → MinIO)]] - code - scripts/setup_airbyte_phase2.py
- [[setup_clickhouse_bronze.py (creates bronze S3 views in ClickHouse)]] - code - scripts/setup_clickhouse_bronze.py
- [[spark_data_profiler DAG]] - code - orchestration/airflow/dags/transformation_dags/spark_profiler.py
- [[submit_spark_profiler() — spark-submit minio_profiler.py]] - code - orchestration/airflow/dags/transformation_dags/spark_profiler.py
- [[trigger_airbyte_sync_safe (tool)]] - code - services/ai-agent/tools/healing_tools.py
- [[trigger_dag() @tool]] - code - services/ai-agent/tools/airflow_tools.py
- [[unified_customer_profile.py (Spark job)]] - code - services/spark/jobs/unified_customer_profile.py
- [[write_expectations (tool)]] - code - services/ai-agent/tools/contract_tools.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Service_Integration_&_Audit_Core
SORT file.name ASC
```

## Connections to other communities
- 3 edges to [[_COMMUNITY_Agent Routing & Chat API]]
- 2 edges to [[_COMMUNITY_Airbyte Health Monitoring]]

## Top bridge nodes
- [[audit_store.py — self-healing action audit log]] - degree 15, connects to 1 community
- [[MinIO raw bucket — Parquet landing zone]] - degree 15, connects to 1 community