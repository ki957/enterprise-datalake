---
source_file: "/home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py"
type: "rationale"
community: "Kafka Streaming Enrichment"
location: "L62"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/Kafka_Streaming_Enrichment
---

# Create raw.shopflow_products_enriched if it doesn't exist.

## Connections
- [[_ensure_raw_table()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/EXTRACTED #community/Kafka_Streaming_Enrichment