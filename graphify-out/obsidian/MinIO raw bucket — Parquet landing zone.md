---
source_file: "orchestration/airflow/dags/shopflow_pipeline.py"
type: "code"
community: "Airbyte Data Ingestion"
location: "line 49"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Airbyte_Data_Ingestion
---

# MinIO raw bucket — Parquet landing zone

## Connections
- [[generate_mysql_data.py (ShopFlow synthetic data generator)]] - `shares_data_with` [INFERRED]
- [[generate_saas_data.py (SaaS PostgreSQL synthetic data generator)]] - `conceptually_related_to` [INFERRED]
- [[ingest_http_to_minio.py (SAP API + JSONPlaceholder → MinIO Parquet)]] - `shares_data_with` [EXTRACTED]
- [[ingest_mysql_to_minio.py (MySQL ShopFlow → MinIO Parquet bypass)]] - `shares_data_with` [EXTRACTED]
- [[list_minio_files (tool)]] - `references` [EXTRACTED]
- [[minio_profiler.py (Spark job)]] - `references` [EXTRACTED]
- [[minio_tools.py_1]] - `references` [EXTRACTED]
- [[sap-apidata_generator.py]] - `shares_data_with` [INFERRED]
- [[setup_airbyte_http_sources.py (configure SAP + REST Airbyte sources)]] - `references` [EXTRACTED]
- [[setup_airbyte_phase2.py (configure Airbyte MySQL CDC → MinIO)]] - `references` [EXTRACTED]
- [[setup_clickhouse_bronze.py (creates bronze S3 views in ClickHouse)]] - `references` [EXTRACTED]
- [[submit_spark_profiler() — spark-submit minio_profiler.py]] - `shares_data_with` [EXTRACTED]
- [[trigger_airbyte_rest_sync() — JSONPlaceholder → MinIO]] - `shares_data_with` [EXTRACTED]
- [[trigger_airbyte_sap_sync() — SAP OData → MinIO Parquet]] - `shares_data_with` [EXTRACTED]
- [[unified_customer_profile.py (Spark job)]] - `shares_data_with` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Airbyte_Data_Ingestion