---
type: community
cohesion: 0.52
members: 7
---

# Superset Dashboard Import

**Cohesion:** 0.52 - moderately connected
**Members:** 7 nodes

## Members
- [[Import a dashboard ZIP via the Superset REST API.]] - rationale - /home/kishore/enterprise-datalake/scripts/import_superset_dashboards.py
- [[_get_csrf()]] - code - /home/kishore/enterprise-datalake/scripts/import_superset_dashboards.py
- [[_get_token()_1]] - code - /home/kishore/enterprise-datalake/scripts/import_superset_dashboards.py
- [[import_superset_dashboards.py]] - code - /home/kishore/enterprise-datalake/scripts/import_superset_dashboards.py
- [[import_zip()]] - code - /home/kishore/enterprise-datalake/scripts/import_superset_dashboards.py
- [[main()_6]] - code - /home/kishore/enterprise-datalake/scripts/import_superset_dashboards.py
- [[wait_for_superset()]] - code - /home/kishore/enterprise-datalake/scripts/import_superset_dashboards.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Superset_Dashboard_Import
SORT file.name ASC
```
