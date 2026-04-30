---
type: community
cohesion: 0.47
members: 6
---

# MinIO Spark Profiler

**Cohesion:** 0.47 - moderately connected
**Members:** 6 nodes

## Members
- [[MinIO BronzeSilver Data Profiler =================================== PySpark jo]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/minio_profiler.py
- [[Return a list of per-column profile dicts for the given DataFrame.]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/minio_profiler.py
- [[build_spark_session()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/minio_profiler.py
- [[main()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/minio_profiler.py
- [[minio_profiler.py]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/minio_profiler.py
- [[profile_dataframe()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/minio_profiler.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/MinIO_Spark_Profiler
SORT file.name ASC
```
