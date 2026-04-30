---
type: community
cohesion: 0.09
members: 34
---

# dbt Transformation Runner

**Cohesion:** 0.09 - loosely connected
**Members:** 34 nodes

## Members
- [[Check source data freshness against SLA thresholds.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Compare expected columns (from dbt manifest.json) against actual ClickHouse]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[Full-refresh SaaS staging and mart models.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Full-refresh all gold (dims + facts) models.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Full-refresh all silver (staging) models, bypassing incremental logic.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Generate dbt docs and copy the manifestcatalog into the dbt-docs nginx volume.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Metadata Sync & Governance DAG ================================ DAG metadata_sy]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[Run all dbt tests across every layer.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[Run dbt docs generate and copy artifacts to the nginx-served target directory.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[Write pipeline run metadata to ClickHouse governance.pipeline_runs table.     Cr]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[_ch()_4]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[_cleanup()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[_run_dbt()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[_send_slack()_2]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[_send_slack()_7]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[dbt Standalone Runner ====================== DAG dbt_standalone_runner Schedule]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[dbt_runner.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[dbt_standalone_runner DAG]] - code - orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[generate_and_publish_docs()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[generate_and_publish_docs() — dbt docs → nginx target]] - code - orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[governance.pipeline_runs (ClickHouse audit table)]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[log_pipeline_metadata()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[log_pipeline_metadata() → governance.pipeline_runs]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[metadata_sync DAG]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[metadata_sync.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[publish_dbt_docs()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[publish_dbt_docs() — dbt docs generate → nginx target]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[run_all_tests()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[run_full_refresh_gold()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[run_full_refresh_saas()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[run_full_refresh_silver()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[run_source_freshness()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/dbt_runner.py
- [[schema_drift_check()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/governance_dags/metadata_sync.py
- [[targetmanifest.json — dbt compiled manifest]] - code - orchestration/airflow/dags/governance_dags/metadata_sync.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/dbt_Transformation_Runner
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_ShopFlow Ingestion Pipeline]]
- 1 edge to [[_COMMUNITY_Airbyte Data Ingestion]]
- 1 edge to [[_COMMUNITY_Backend API & Storage Layer]]

## Top bridge nodes
- [[_run_dbt()_1]] - degree 9, connects to 1 community
- [[schema_drift_check()]] - degree 7, connects to 1 community
- [[_ch()_4]] - degree 4, connects to 1 community