---
type: community
cohesion: 0.23
members: 16
---

# Superset Setup & Dashboards

**Cohesion:** 0.23 - loosely connected
**Members:** 16 nodes

## Members
- [[Create key charts. Returns list of chart ids.]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[Create the Business Intelligence dashboard.]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[Export dashboard as ZIP and save to infrastructuresupersetdashboards.]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[Register ClickHouse as a Superset database connection. Returns DB id.]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[Register gold. tables as Superset datasets. Returns {table_name dataset_id}.]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[_api()]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[_get_csrf_token()]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[_get_token()_2]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[create_charts()]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[create_clickhouse_database()]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[create_dashboard()]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[create_datasets()]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[export_dashboard()]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[main()_10]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[setup_superset.py]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py
- [[wait_for_superset()_1]] - code - /home/kishore/enterprise-datalake/scripts/setup_superset.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Superset_Setup_&_Dashboards
SORT file.name ASC
```
