---
type: community
cohesion: 0.50
members: 4
---

# Spark Profiler DAG

**Cohesion:** 0.50 - moderately connected
**Members:** 4 nodes

## Members
- [[Spark Data Profiler DAG ======================== DAG spark_data_profiler Schedu]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/spark_profiler.py
- [[spark-submit the profiler job and stream output to Airflow logs.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/spark_profiler.py
- [[spark_profiler.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/spark_profiler.py
- [[submit_spark_profiler()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/spark_profiler.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Spark_Profiler_DAG
SORT file.name ASC
```
