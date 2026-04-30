---
type: community
cohesion: 0.35
members: 11
---

# HTTP to MinIO Ingestion

**Cohesion:** 0.35 - loosely connected
**Members:** 11 nodes

## Members
- [[Convert records list to Parquet and upload to MinIO.]] - rationale - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[Paginate through a SAP API endpoint.]] - rationale - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[ensure_bucket()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[fetch_all_pages()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[get_minio_client()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[http_get()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[ingest_http_to_minio.py]] - code - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[ingest_jsonplaceholder()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[ingest_sap()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[main()_7]] - code - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py
- [[upload_parquet()]] - code - /home/kishore/enterprise-datalake/scripts/ingest_http_to_minio.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/HTTP_to_MinIO_Ingestion
SORT file.name ASC
```
