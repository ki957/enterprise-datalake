---
source_file: "scripts/setup_clickhouse_bronze.py"
type: "code"
community: "Airbyte Data Ingestion"
location: "line 1-204"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Airbyte_Data_Ingestion
---

# setup_clickhouse_bronze.py (creates bronze S3 views in ClickHouse)

## Connections
- [[ClickHouse bronze schema (S3 views over MinIO)]] - `implements` [EXTRACTED]
- [[MinIO raw bucket — Parquet landing zone]] - `references` [EXTRACTED]
- [[ingest_mysql_to_minio.py (MySQL ShopFlow → MinIO Parquet bypass)]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Airbyte_Data_Ingestion