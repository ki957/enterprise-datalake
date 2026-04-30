---
type: community
cohesion: 0.25
members: 14
---

# Keycloak SSO Setup

**Cohesion:** 0.25 - loosely connected
**Members:** 14 nodes

## Members
- [[Create a viewer test user for manual SSO login verification.]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[Create an OIDC client for Grafana (Authorization Code + fixed secret).]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[Create an OIDC client for Superset.]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[Fetch a short-lived admin-cli token from the master realm.]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[Keycloak Setup Script ====================== Creates the 'datalake' realm with a]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[_admin_token()]] - code - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[_http()]] - code - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[_wait_for_keycloak()]] - code - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[create_grafana_client()]] - code - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[create_realm()]] - code - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[create_superset_client()]] - code - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[create_test_user()]] - code - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[main()_9]] - code - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py
- [[setup_keycloak.py]] - code - /home/kishore/enterprise-datalake/scripts/setup_keycloak.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Keycloak_SSO_Setup
SORT file.name ASC
```
