---
type: community
cohesion: 0.21
members: 20
---

# Airbyte OAuth2 API Client

**Cohesion:** 0.21 - loosely connected
**Members:** 20 nodes

## Members
- [[Convert a Unix epoch int from the Airbyte API to a readable UTC string.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[Exchange client credentials for a Bearer token.      Caching strategy     - Tok]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[Extract synced record count from an Airbyte attempt — field varies by API versio]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[Get detailed status of one Airbyte connection by its UUID.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[Get the most recent sync job result for an Airbyte connection (records synced, d]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[List all Airbyte connections with status AND latest sync job details (records, d]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[List all configured Airbyte sources in the workspace.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[Manually trigger an Airbyte sync for a given connection UUID.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[_base()_1]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[_get_token()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[_headers()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[_records()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[_ts()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[_unreachable_msg()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[airbyte_tools.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[get_airbyte_connection_status()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[get_airbyte_source_info()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[get_latest_sync_job()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[list_airbyte_connections()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py
- [[trigger_airbyte_sync()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airbyte_tools.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Airbyte_OAuth2_API_Client
SORT file.name ASC
```

## Connections to other communities
- 2 edges to [[_COMMUNITY_PostgreSQL Audit Store]]

## Top bridge nodes
- [[_base()_1]] - degree 8, connects to 1 community
- [[_headers()]] - degree 8, connects to 1 community