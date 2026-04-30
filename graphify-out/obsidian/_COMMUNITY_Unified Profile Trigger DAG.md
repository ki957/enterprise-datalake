---
type: community
cohesion: 0.67
members: 3
---

# Unified Profile Trigger DAG

**Cohesion:** 0.67 - moderately connected
**Members:** 3 nodes

## Members
- [[Unified Customer Profile DAG ============================== DAG unified_custome]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[submit_unified_profile()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/unified_profile.py
- [[unified_profile.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/transformation_dags/unified_profile.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Unified_Profile_Trigger_DAG
SORT file.name ASC
```
