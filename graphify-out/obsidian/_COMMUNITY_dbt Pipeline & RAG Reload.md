---
type: community
cohesion: 0.07
members: 35
---

# dbt Pipeline & RAG Reload

**Cohesion:** 0.07 - loosely connected
**Members:** 35 nodes

## Members
- [[Compare expected columns (from dbt manifest.json) against actual ClickHouse]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[Compare gold layer row counts against the previous run.     Writes a structured]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[Metadata Sync & Governance DAG ================================ DAG metadata_sy]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[POST apiragreload — AI Agent ChromaDB re-seed]] - code - orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[Run dbt docs generate and copy artifacts to the nginx-served target directory.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[Write pipeline run metadata to ClickHouse governance.pipeline_runs table.     Cr]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[_ch()_4]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[_enrich_batch_with_groq() — llama-4-scout product enrichment]] - code - orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[_send_slack()_7]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[consume_and_enrich() — Kafka poll + Groq batch enrichment]] - code - orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[dbt_standalone_runner DAG]] - code - orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[dbz.shopflow.products (Kafka topic, Debezium CDC)]] - code - orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[generate_and_publish_docs() — dbt docs → nginx target]] - code - orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[governance.pipeline_runs (ClickHouse audit table)]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[log_pipeline_metadata()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[log_pipeline_metadata() → governance.pipeline_runs]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[metadata_sync DAG]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[metadata_sync.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[modelsbronzesources.yml — dbt source definitions]] - code - orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[observability_check()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[publish_dbt_docs()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[publish_dbt_docs() — dbt docs generate → nginx target]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[raw.shopflow_products_enriched (ClickHouse table)]] - code - orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[reload_agent_rag() — POST apiragreload conditional]] - code - orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[reload_rag() — POST apiragreload to AI Agent]] - code - orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[run_dbt_mart() — mart_product_enrichment conditional refresh]] - code - orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[schema_drift_alerts (PostgreSQL table)]] - code - orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[schema_drift_check()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[schema_evolution DAG]] - code - orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[schema_snapshots (PostgreSQL table)]] - code - orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[snapshot_mysql_schema() — information_schema diff]] - code - orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[streaming_enrichment DAG]] - code - orchestration/airflow/dags/streaming_dags/streaming_enrichment_dag.py
- [[targetmanifest.json — dbt compiled manifest]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[update_sources_yml() — ruamel.yaml safe append]] - code - orchestration/airflow/dags/governance_dags/schema_evolution_dag.py
- [[validate_dbt_compile() — dbt parse + rollback on failure]] - code - orchestration/airflow/dags/governance_dags/schema_evolution_dag.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/dbt_Pipeline_&_RAG_Reload
SORT file.name ASC
```

## Connections to other communities
- 2 edges to [[_COMMUNITY_Data Quality & DAG Contracts]]
- 1 edge to [[_COMMUNITY_MySQL to MinIO Bronze Ingest]]
- 1 edge to [[_COMMUNITY_dbt Docs & Run DAG]]

## Top bridge nodes
- [[validate_dbt_compile() — dbt parse + rollback on failure]] - degree 3, connects to 1 community
- [[observability_check()_1]] - degree 3, connects to 1 community
- [[dbt_standalone_runner DAG]] - degree 2, connects to 1 community
- [[run_dbt_mart() — mart_product_enrichment conditional refresh]] - degree 2, connects to 1 community