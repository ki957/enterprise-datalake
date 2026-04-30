---
type: community
cohesion: 0.19
members: 14
---

# ClickHouse Bronze Setup

**Cohesion:** 0.19 - loosely connected
**Members:** 14 nodes

## Members
- [[.test_passes_when_both_counts_nonzero()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_raises_on_zero_events()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[.test_raises_on_zero_users()]] - code - /home/kishore/enterprise-datalake/tests/test_saas_pipeline_logic.py
- [[Row-count and freshness checks across all Gold tables._1]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/shopflow_pipeline.py
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
TABLE source_file, type FROM #community/ClickHouse_Bronze_Setup
SORT file.name ASC
```

## Connections to other communities
- 2 edges to [[_COMMUNITY_ShopFlow Ingestion Pipeline]]
- 1 edge to [[_COMMUNITY_Integration Tests (ClickHouse)]]
- 1 edge to [[_COMMUNITY_Backend API & Storage Layer]]

## Top bridge nodes
- [[data_quality_check()_1]] - degree 9, connects to 2 communities
- [[test_saas_pipeline_logic.py]] - degree 3, connects to 1 community