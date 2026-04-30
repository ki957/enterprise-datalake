---
type: community
cohesion: 0.31
members: 9
---

# MinIO Storage Tools

**Cohesion:** 0.31 - loosely connected
**Members:** 9 nodes

## Members
- [[Get the total file count and size for a MinIO bucket.     Use for storage capaci]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/minio_tools.py
- [[List all MinIO buckets. Use to confirm bucket structure is correct.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/minio_tools.py
- [[List files in the MinIO 'raw' bucket under the given prefix.     Use to verify A]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/minio_tools.py
- [[MinIO tools for the AI agent.  Singleton client a single module-level Minio ins]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/minio_tools.py
- [[_client()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/minio_tools.py
- [[check_minio_bucket_size()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/minio_tools.py
- [[list_minio_buckets()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/minio_tools.py
- [[list_minio_files()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/minio_tools.py
- [[minio_tools.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/minio_tools.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/MinIO_Storage_Tools
SORT file.name ASC
```
