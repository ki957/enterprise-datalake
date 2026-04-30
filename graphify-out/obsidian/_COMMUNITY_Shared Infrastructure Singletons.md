---
type: community
cohesion: 0.40
members: 6
---

# Shared Infrastructure Singletons

**Cohesion:** 0.40 - moderately connected
**Members:** 6 nodes

## Members
- [[Keycloak datalake realm (TOTP MFA + OIDC clients)]] - code - scripts/setup_keycloak.py
- [[Superset ClickHouse gold layer datasource]] - code - scripts/setup_superset.py
- [[import_superset_dashboards.py (restores dashboard ZIPs to Superset)]] - code - scripts/import_superset_dashboards.py
- [[run_checkpoint.py (executes Great Expectations datalake checkpoint)]] - code - governance/great_expectations/run_checkpoint.py
- [[setup_keycloak.py (creates datalake realm + GrafanaSuperset OIDC clients)]] - code - scripts/setup_keycloak.py
- [[setup_superset.py (configures ClickHouse datasets + dashboard + export)]] - code - scripts/setup_superset.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Shared_Infrastructure_Singletons
SORT file.name ASC
```
