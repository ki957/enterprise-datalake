---
type: community
cohesion: 0.30
members: 15
---

# Airbyte HTTP Source Setup

**Cohesion:** 0.30 - loosely connected
**Members:** 15 nodes

## Members
- [[Declarative manifest for JSONPlaceholder REST API.     Response shape direct ar]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[Declarative manifest for the SAP Flask API.     Response shape {results ...]] - rationale - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[api()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[api_public()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[check_source()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[create_connection()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[create_source()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[discover_catalog()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[get_stream_stats()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[get_token()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[main()_8]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[rest_manifest()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[sap_manifest()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[setup_airbyte_http_sources.py]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py
- [[trigger_and_wait()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_http_sources.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Airbyte_HTTP_Source_Setup
SORT file.name ASC
```
