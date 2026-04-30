---
source_file: "orchestration/airflow/dags/quality_dags/data_quality_suite.py"
type: "code"
community: "Auto Contract Logic"
location: "line 250"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Auto_Contract_Logic
---

# data_quality_suite DAG

## Connections
- [[cross_table_consistency_check() — FK referential integrity]] - `calls` [EXTRACTED]
- [[data_quality_check()]] - `semantically_similar_to` [INFERRED]
- [[null_rate_check() — 5% threshold on critical columns]] - `calls` [EXTRACTED]
- [[row_count_anomaly_check() — 7-day rolling average]] - `calls` [EXTRACTED]
- [[run_great_expectations() — calls run_checkpoint.py]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Auto_Contract_Logic