---
source_file: "/home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py"
type: "rationale"
community: "ShopFlow Ingestion Pipeline"
location: "L343"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/ShopFlow_Ingestion_Pipeline
---

# Verify all expected Parquet prefixes exist in MinIO raw bucket.

## Connections
- [[wait_for_minio_files()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/EXTRACTED #community/ShopFlow_Ingestion_Pipeline