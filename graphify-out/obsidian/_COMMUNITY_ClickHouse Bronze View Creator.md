---
type: community
cohesion: 0.21
members: 13
---

# ClickHouse Bronze View Creator

**Cohesion:** 0.21 - loosely connected
**Members:** 13 nodes

## Members
- [[.test_passes_when_both_counts_nonzero()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_raises_on_zero_events()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_raises_on_zero_users()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[Row-count and freshness checks across all Gold tables.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[SaaS Pipeline Logic Tests ========================== Tests the pure-Python helpe]] - rationale - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[TestQualityCheck]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[Tests for data_quality_check — validates assertion logic without DB.]] - rationale - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[ch()]] - code - /home/kishore/enterprise-datalake/scripts/setup_clickhouse_bronze.py
- [[data_quality_check()_1]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
- [[main()_3]] - code - /home/kishore/enterprise-datalake/scripts/setup_clickhouse_bronze.py
- [[s3()]] - code - /home/kishore/enterprise-datalake/scripts/setup_clickhouse_bronze.py
- [[setup_clickhouse_bronze.py]] - code - /home/kishore/enterprise-datalake/scripts/setup_clickhouse_bronze.py
- [[test_saas_pipeline_logic.py]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/ClickHouse_Bronze_View_Creator
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_MySQL to MinIO Bronze Ingest]]
- 1 edge to [[_COMMUNITY_Integration Test Infrastructure]]
- 1 edge to [[_COMMUNITY_Data Quality & DAG Contracts]]

## Top bridge nodes
- [[data_quality_check()_1]] - degree 7, connects to 2 communities
- [[test_saas_pipeline_logic.py]] - degree 3, connects to 1 community