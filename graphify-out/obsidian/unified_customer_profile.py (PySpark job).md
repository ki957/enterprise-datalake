---
source_file: "orchestration/airflow/dags/transformation_dags/unified_profile.py"
type: "code"
community: "Airbyte Data Ingestion"
location: "line 34"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Airbyte_Data_Ingestion
---

# unified_customer_profile.py (PySpark job)

## Connections
- [[gold.dim_customers (ClickHouse dimension table)]] - `shares_data_with` [EXTRACTED]
- [[gold.dim_users (ClickHouse SaaS user dimension)]] - `shares_data_with` [EXTRACTED]
- [[gold.unified_customers (ClickHouse table)]] - `shares_data_with` [EXTRACTED]
- [[submit_unified_profile() — spark-submit cross-domain join]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Airbyte_Data_Ingestion