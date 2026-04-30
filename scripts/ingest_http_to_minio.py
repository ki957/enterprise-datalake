#!/usr/bin/env python3
"""
HTTP source ingestion: SAP API + JSONPlaceholder → MinIO Parquet.

Writes data to MinIO under paths that mirror Airbyte's output convention:
    raw/airbyte/sap/vendors/
    raw/airbyte/sap/purchase_orders/
    raw/airbyte/sap/cost_centers/
    raw/airbyte/rest/users/
    raw/airbyte/rest/posts/
    raw/airbyte/rest/comments/

Usage:
    pip install minio pyarrow requests
    python scripts/ingest_http_to_minio.py
"""

import io
import json
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

SAP_BASE_URL = "http://localhost:5001"
JSON_BASE_URL = "https://jsonplaceholder.typicode.com"

MINIO_ENDPOINT   = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "Minio@2024"
MINIO_BUCKET     = "raw"

NOW = datetime.now(tz=timezone.utc)
DATE_PART = NOW.strftime("%Y_%m_%d")
EPOCH = int(NOW.timestamp())


# ── MinIO client ──────────────────────────────────────────────────────────────

def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"  Created bucket: {bucket}")


def upload_parquet(client: Minio, records: list[dict], minio_path: str):
    """Convert records list to Parquet and upload to MinIO."""
    if not records:
        print(f"  [SKIP] No records for {minio_path}")
        return

    table = pa.Table.from_pylist(records)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    size = buf.getbuffer().nbytes

    client.put_object(
        MINIO_BUCKET,
        minio_path,
        buf,
        length=size,
        content_type="application/octet-stream",
    )
    print(f"  Uploaded {minio_path} ({size:,} bytes, {len(records)} records)")


# ── HTTP fetch helpers ────────────────────────────────────────────────────────

def http_get(url: str, retries: int = 3) -> dict | list:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def fetch_all_pages(base_url: str, path: str, page_size: int = 100) -> list[dict]:
    """Paginate through a SAP API endpoint."""
    all_records = []
    page = 1
    while True:
        url = f"{base_url}{path}?page={page}&page_size={page_size}"
        data = http_get(url)
        results = data.get("results", [])
        all_records.extend(results)
        if data.get("next_page") is None:
            break
        page += 1
    return all_records


# ── SAP API ingestion ─────────────────────────────────────────────────────────

def ingest_sap(client: Minio):
    print("\n── SAP API ──────────────────────────────────────────")

    streams = [
        ("/api/vendors",         "sap/vendors"),
        ("/api/purchase-orders", "sap/purchase_orders"),
        ("/api/cost-centers",    "sap/cost_centers"),
    ]

    for api_path, prefix in streams:
        stream_name = prefix.split("/")[-1]
        print(f"  Fetching {api_path} ...")
        records = fetch_all_pages(SAP_BASE_URL, api_path)

        # Add Airbyte-style metadata columns
        for r in records:
            r["_airbyte_extracted_at"] = NOW.isoformat()
            r["_airbyte_sync_id"] = f"http-ingest-{EPOCH}"

        minio_path = f"airbyte/{prefix}/{DATE_PART}_{EPOCH}_{stream_name}.parquet"
        upload_parquet(client, records, minio_path)


# ── JSONPlaceholder ingestion ─────────────────────────────────────────────────

def ingest_jsonplaceholder(client: Minio):
    print("\n── JSONPlaceholder REST API ──────────────────────────")

    streams = [
        ("/users",    "rest/users"),
        ("/posts",    "rest/posts"),
        ("/comments", "rest/comments"),
    ]

    for api_path, prefix in streams:
        stream_name = prefix.split("/")[-1]
        print(f"  Fetching {JSON_BASE_URL}{api_path} ...")
        records = http_get(f"{JSON_BASE_URL}{api_path}")
        if not isinstance(records, list):
            records = [records]

        # Flatten nested objects (address, company, geo) to strings for Parquet
        flat_records = []
        for r in records:
            flat = {}
            for k, v in r.items():
                flat[k] = json.dumps(v) if isinstance(v, dict) else v
            flat["_airbyte_extracted_at"] = NOW.isoformat()
            flat["_airbyte_sync_id"] = f"http-ingest-{EPOCH}"
            flat_records.append(flat)

        minio_path = f"airbyte/{prefix}/{DATE_PART}_{EPOCH}_{stream_name}.parquet"
        upload_parquet(client, flat_records, minio_path)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("HTTP Sources → MinIO Parquet Ingestion")
    print(f"Run timestamp: {NOW.isoformat()}")
    print("=" * 60)

    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    ingest_sap(client)
    ingest_jsonplaceholder(client)

    print("\n── Verifying MinIO paths ─────────────────────────────")
    paths_to_check = [
        "airbyte/sap/vendors/",
        "airbyte/sap/purchase_orders/",
        "airbyte/sap/cost_centers/",
        "airbyte/rest/users/",
        "airbyte/rest/posts/",
        "airbyte/rest/comments/",
    ]
    for prefix in paths_to_check:
        objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
        if objects:
            obj = objects[0]
            print(f"  OK  raw/{prefix}  ({obj.size:,} bytes)")
        else:
            print(f"  MISSING  raw/{prefix}")

    print("\nDone.")


if __name__ == "__main__":
    main()
