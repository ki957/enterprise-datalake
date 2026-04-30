---
source_file: "services/spark/jobs/unified_customer_profile.py"
type: "code"
community: "Airbyte Data Ingestion"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Airbyte_Data_Ingestion
---

# unified_customer_profile.py (Spark job)

## Connections
- [[ClickHouse gold schema]] - `references` [EXTRACTED]
- [[MinIO raw bucket — Parquet landing zone]] - `shares_data_with` [EXTRACTED]
- [[gold.unified_customers (ClickHouse table)_1]] - `shares_data_with` [EXTRACTED]
- [[minio_profiler.py (Spark job)]] - `semantically_similar_to` [INFERRED]

#graphify/code #graphify/EXTRACTED #community/Airbyte_Data_Ingestion