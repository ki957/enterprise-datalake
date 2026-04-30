---
type: community
cohesion: 0.31
members: 9
---

# Profiling Tools

**Cohesion:** 0.31 - loosely connected
**Members:** 9 nodes

## Members
- [[Fetch the last N DAG run records from Airflow state, start time, duration.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/profiling_tools.py
- [[Profile a ClickHouse table null rate, distinct cardinality, and numeric     per]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/profiling_tools.py
- [[Profiling tools shared by the anomaly detection and data contract agents.  profi]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/profiling_tools.py
- [[_af_auth()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/profiling_tools.py
- [[_af_base()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/profiling_tools.py
- [[_ch_client()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/profiling_tools.py
- [[get_airflow_run_history()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/profiling_tools.py
- [[profile_table_stats()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/profiling_tools.py
- [[profiling_tools.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/profiling_tools.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Profiling_Tools
SORT file.name ASC
```
