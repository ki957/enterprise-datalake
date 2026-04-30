---
source_file: "/home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py"
type: "rationale"
community: "ShopFlow Ingestion Pipeline"
location: "L282"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/ShopFlow_Ingestion_Pipeline
---

# Ingest JSONPlaceholder REST API → MinIO Parquet (users, posts, comments).

## Connections
- [[trigger_airbyte_rest_sync()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/EXTRACTED #community/ShopFlow_Ingestion_Pipeline