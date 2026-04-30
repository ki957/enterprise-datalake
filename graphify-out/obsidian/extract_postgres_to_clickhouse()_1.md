---
source_file: "orchestration/airflow/dags/saas_pipeline.py"
type: "code"
community: "Backend API & Storage Layer"
location: "line 119"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Backend_API_&_Storage_Layer
---

# extract_postgres_to_clickhouse()

## Connections
- [[_ch_insert()]] - `calls` [EXTRACTED]
- [[raw.saas_events (ClickHouse table)]] - `shares_data_with` [EXTRACTED]
- [[raw.saas_subscriptions (ClickHouse table)]] - `shares_data_with` [EXTRACTED]
- [[raw.saas_users (ClickHouse table)]] - `shares_data_with` [EXTRACTED]
- [[saas_data_pipeline DAG]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Backend_API_&_Storage_Layer