---
source_file: "orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py"
type: "code"
community: "Backend API & Storage Layer"
location: "line 149"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Backend_API_&_Storage_Layer
---

# consume_and_enrich() — Kafka poll + Groq batch enrichment

## Connections
- [[_enrich_batch_with_groq() — llama-4-scout product enrichment]] - `calls` [EXTRACTED]
- [[dbz.shopflow.products (Kafka topic, Debezium CDC)]] - `shares_data_with` [EXTRACTED]
- [[raw.shopflow_products_enriched (ClickHouse table)]] - `shares_data_with` [EXTRACTED]
- [[streaming_enrichment DAG]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Backend_API_&_Storage_Layer