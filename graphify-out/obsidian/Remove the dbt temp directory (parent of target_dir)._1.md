---
source_file: "/home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py"
type: "rationale"
community: "ShopFlow Ingestion Pipeline"
location: "L383"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/ShopFlow_Ingestion_Pipeline
---

# Remove the dbt temp directory (parent of target_dir).

## Connections
- [[_cleanup_dbt_tmp()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/EXTRACTED #community/ShopFlow_Ingestion_Pipeline