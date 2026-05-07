"""SAP OData API and JSONPlaceholder REST → MinIO Parquet ingestion tasks."""

import io
import os
from datetime import datetime, timezone

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET   = "raw"

SAP_BASE_URL  = os.getenv("SAP_BASE_URL", "http://sap-api:5001")
JSON_BASE_URL = os.getenv("JSON_BASE_URL", "https://jsonplaceholder.typicode.com")


def trigger_airbyte_sap_sync(**ctx):
    """Ingest SAP OData API → MinIO Parquet (vendors, purchase_orders, cost_centers)."""
    import requests
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        from minio import Minio
    except ImportError as e:
        raise RuntimeError(f"Missing package: {e}. Ensure _PIP_ADDITIONAL_REQUIREMENTS is set.") from e

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    now    = datetime.now(tz=timezone.utc)

    def purge_prefix(minio_prefix: str):
        from minio.deleteobjects import DeleteObject
        objects = client.list_objects(MINIO_BUCKET, prefix=minio_prefix, recursive=True)
        delete_list = [DeleteObject(o.object_name) for o in objects]
        if delete_list:
            list(client.remove_objects(MINIO_BUCKET, delete_list))
            print(f"  Purged {len(delete_list)} objects from {MINIO_BUCKET}/{minio_prefix}")

    def upload(records: list, prefix: str):
        if not records:
            print(f"  [SKIP] No records for {prefix}")
            return
        minio_prefix = f"airbyte/sap/{prefix}/"
        purge_prefix(minio_prefix)
        table = pa.Table.from_pylist(records)
        buf   = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        path  = f"airbyte/sap/{prefix}/{prefix}.parquet"
        client.put_object(MINIO_BUCKET, path, buf, length=buf.getbuffer().nbytes,
                          content_type="application/octet-stream")
        print(f"  Uploaded {MINIO_BUCKET}/{path} ({len(records)} records)")

    def fetch_all(api_path: str) -> list:
        all_records, page = [], 1
        while True:
            data    = requests.get(f"{SAP_BASE_URL}{api_path}?page={page}&page_size=100",
                                   timeout=30).json()
            results = data.get("results", [])
            all_records.extend(results)
            if not data.get("next_page"):
                break
            page += 1
        return all_records

    for path, name in [("/api/vendors", "vendors"),
                       ("/api/purchase-orders", "purchase_orders"),
                       ("/api/cost-centers", "cost_centers")]:
        print(f"  Fetching {path} ...")
        records = fetch_all(path)
        for r in records:
            r["_airbyte_extracted_at"] = now.isoformat()
        upload(records, name)

    print("SAP sync complete.")


def trigger_airbyte_rest_sync(**ctx):
    """Ingest JSONPlaceholder REST API → MinIO Parquet (users, posts, comments)."""
    import json as _json
    import requests
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        from minio import Minio
    except ImportError as e:
        raise RuntimeError(f"Missing package: {e}") from e

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    now    = datetime.now(tz=timezone.utc)

    def purge_prefix(minio_prefix: str):
        from minio.deleteobjects import DeleteObject
        objects = client.list_objects(MINIO_BUCKET, prefix=minio_prefix, recursive=True)
        delete_list = [DeleteObject(o.object_name) for o in objects]
        if delete_list:
            list(client.remove_objects(MINIO_BUCKET, delete_list))
            print(f"  Purged {len(delete_list)} objects from {MINIO_BUCKET}/{minio_prefix}")

    def upload(records: list, prefix: str):
        flat = []
        for r in records:
            f = {k: (_json.dumps(v) if isinstance(v, dict) else v) for k, v in r.items()}
            f["_airbyte_extracted_at"] = now.isoformat()
            flat.append(f)
        minio_prefix = f"airbyte/rest/{prefix}/"
        purge_prefix(minio_prefix)
        table = pa.Table.from_pylist(flat)
        buf   = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        path  = f"airbyte/rest/{prefix}/{prefix}.parquet"
        client.put_object(MINIO_BUCKET, path, buf, length=buf.getbuffer().nbytes,
                          content_type="application/octet-stream")
        print(f"  Uploaded {MINIO_BUCKET}/{path} ({len(flat)} records)")

    skipped = []
    for endpoint, name in [("/users", "users"), ("/posts", "posts"), ("/comments", "comments")]:
        print(f"  Fetching {JSON_BASE_URL}{endpoint} ...")
        try:
            records = requests.get(f"{JSON_BASE_URL}{endpoint}", timeout=30).json()
        except Exception as exc:
            print(f"  [WARN] JSONPlaceholder unreachable ({exc}) — skipping {name}")
            skipped.append(name)
            continue
        if not isinstance(records, list):
            records = [records]
        upload(records, name)

    if skipped:
        print(f"  REST sync partial — skipped offline: {skipped}")
    else:
        print("REST sync complete.")
