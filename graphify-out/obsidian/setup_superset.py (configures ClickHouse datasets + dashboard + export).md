---
source_file: "scripts/setup_superset.py"
type: "code"
community: "Governance Scripts"
location: "line 1-308"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Governance_Scripts
---

# setup_superset.py (configures ClickHouse datasets + dashboard + export)

## Connections
- [[Superset ClickHouse gold layer datasource]] - `implements` [EXTRACTED]
- [[import_superset_dashboards.py (restores dashboard ZIPs to Superset)]] - `shares_data_with` [EXTRACTED]
- [[run_checkpoint.py (executes Great Expectations datalake checkpoint)]] - `conceptually_related_to` [INFERRED]
- [[setup_keycloak.py (creates datalake realm + GrafanaSuperset OIDC clients)]] - `semantically_similar_to` [INFERRED]

#graphify/code #graphify/EXTRACTED #community/Governance_Scripts