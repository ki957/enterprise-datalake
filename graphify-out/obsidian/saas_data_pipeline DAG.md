---
source_file: "orchestration/airflow/dags/saas_pipeline.py"
type: "code"
community: "Backend API & Storage Layer"
location: "line 289"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Backend_API_&_Storage_Layer
---

# saas_data_pipeline DAG

## Connections
- [[DAG Integrity Tests]] - `references` [INFERRED]
- [[Rationale Custom Dockerfile.airflow bakes dbtpyarrowminio for reproducibility]] - `rationale_for` [INFERRED]
- [[SaaS Pipeline End-to-End Integration Tests]] - `references` [EXTRACTED]
- [[SaaS Pipeline Logic Unit Tests]] - `references` [EXTRACTED]
- [[data_quality_check()]] - `calls` [EXTRACTED]
- [[extract_postgres_to_clickhouse()_1]] - `calls` [EXTRACTED]
- [[notify_failure()]] - `calls` [EXTRACTED]
- [[observability_check()]] - `calls` [EXTRACTED]
- [[run_dbt_saas_marts()_1]] - `calls` [EXTRACTED]
- [[run_dbt_saas_staging()_1]] - `calls` [EXTRACTED]
- [[shopflow_datalake_pipeline DAG]] - `conceptually_related_to` [INFERRED]

#graphify/code #graphify/EXTRACTED #community/Backend_API_&_Storage_Layer