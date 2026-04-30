---
type: community
cohesion: 0.21
members: 13
---

# Kafka Streaming Enrichment

**Cohesion:** 0.21 - loosely connected
**Members:** 13 nodes

## Members
- [[Call Groq to enrich a batch of products.     Returns list of enrichment dicts {]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[Create raw.shopflow_products_enriched if it doesn't exist.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[POST apiragreload so the Insight Agent immediately knows about     gold.mart_]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[Poll Kafka for product CDC events, enrich via Groq, write to ClickHouse.     Tra]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[Run dbt mart_product_enrichment model to land enriched data in gold.     Only ru]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[Streaming Enrichment DAG ======================== DAG streaming_enrichment Sche]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[_ch()_3]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[_enrich_batch_with_groq()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[_ensure_raw_table()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[consume_and_enrich()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[reload_rag()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[run_dbt_mart()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[streaming_enrichment_dag.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Kafka_Streaming_Enrichment
SORT file.name ASC
```
