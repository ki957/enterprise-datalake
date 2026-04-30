---
source_file: "/home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py"
type: "rationale"
community: "Kafka Streaming Enrichment"
location: "L150"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/Kafka_Streaming_Enrichment
---

# Poll Kafka for product CDC events, enrich via Groq, write to ClickHouse.     Tra

## Connections
- [[consume_and_enrich()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/EXTRACTED #community/Kafka_Streaming_Enrichment