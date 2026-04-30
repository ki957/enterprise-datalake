---
source_file: "orchestration/airflow/dags/shopflow_pipeline.py"
type: "code"
community: "Airbyte Data Ingestion"
location: "line 862"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Airbyte_Data_Ingestion
---

# shopflow_datalake_pipeline DAG

## Connections
- [[extract_saas_to_clickhouse() — parallel SaaS PG extract]] - `calls` [EXTRACTED]
- [[observability_check() — gold table row count diff]] - `calls` [EXTRACTED]
- [[refresh_superset_dashboard() — 6 dataset IDs]] - `calls` [EXTRACTED]
- [[run_dbt_gold() — gold + marts layer]] - `calls` [EXTRACTED]
- [[run_dbt_silver() — silver + staging layer]] - `calls` [EXTRACTED]
- [[saas_data_pipeline DAG]] - `conceptually_related_to` [INFERRED]
- [[trigger_airbyte_mysql_sync()_1]] - `calls` [EXTRACTED]
- [[trigger_airbyte_rest_sync() — JSONPlaceholder → MinIO]] - `calls` [EXTRACTED]
- [[trigger_airbyte_sap_sync() — SAP OData → MinIO Parquet]] - `calls` [EXTRACTED]
- [[unified_customer_profile DAG]] - `conceptually_related_to` [INFERRED]
- [[wait_for_minio_files() — verifies Parquet prefixes]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Airbyte_Data_Ingestion