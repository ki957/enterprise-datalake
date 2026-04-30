---
type: community
cohesion: 0.35
members: 11
---

# Airbyte MySQL CDC Setup

**Cohesion:** 0.35 - loosely connected
**Members:** 11 nodes

## Members
- [[api()_1]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[api_public()_1]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[create_connection()_1]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[create_minio_destination()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[create_mysql_source()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[discover_catalog()_1]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[get_token()_1]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[main()_11]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[setup_airbyte_phase2.py]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[trigger_sync()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py
- [[wait_for_job()]] - code - /home/kishore/enterprise-datalake/scripts/setup_airbyte_phase2.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Airbyte_MySQL_CDC_Setup
SORT file.name ASC
```
