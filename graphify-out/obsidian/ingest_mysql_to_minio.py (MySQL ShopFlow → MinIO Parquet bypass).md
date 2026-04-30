---
source_file: "scripts/ingest_mysql_to_minio.py"
type: "code"
community: "Airbyte Data Ingestion"
location: "line 1-185"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Airbyte_Data_Ingestion
---

# ingest_mysql_to_minio.py (MySQL ShopFlow → MinIO Parquet bypass)

## Connections
- [[MinIO raw bucket — Parquet landing zone]] - `shares_data_with` [EXTRACTED]
- [[ingest_http_to_minio.py (SAP API + JSONPlaceholder → MinIO Parquet)]] - `semantically_similar_to` [INFERRED]
- [[setup_clickhouse_bronze.py (creates bronze S3 views in ClickHouse)]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Airbyte_Data_Ingestion