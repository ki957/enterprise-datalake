---
source_file: "/home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py"
type: "rationale"
community: "ShopFlow Ingestion Pipeline"
location: "L218"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/ShopFlow_Ingestion_Pipeline
---

# Ingest SAP OData API → MinIO Parquet (vendors, purchase_orders, cost_centers).

## Connections
- [[trigger_airbyte_sap_sync()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/EXTRACTED #community/ShopFlow_Ingestion_Pipeline