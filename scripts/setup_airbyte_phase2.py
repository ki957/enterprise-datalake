#!/usr/bin/env python3
"""
Phase 2: Configure Airbyte → MinIO pipeline.

What this script does:
  1. Authenticates with Airbyte (abctl OAuth2)
  2. Creates S3/MinIO destination
  3. Creates MySQL CDC source
  4. Creates a connection: MySQL → MinIO
  5. Triggers an initial sync

Run:
    python scripts/setup_airbyte_phase2.py
"""

import sys
import time
import json
import urllib.request
import urllib.error

AIRBYTE_URL = "http://localhost:8000"
CLIENT_ID   = "4ef6ae1b-2abb-479c-8476-f46a3d4e1895"
CLIENT_SECRET = "DqSQF98hSB9kvixIJPIfwC6KeQHr5IEY"
WORKSPACE_ID  = "80c3e872-96c3-451f-9f63-74e33001a0a6"

# Connector definition IDs (verified from live Airbyte catalog)
MYSQL_SOURCE_DEF_ID  = "435bb9a5-7887-4809-aa58-28c27df0d7ad"
S3_DEST_DEF_ID       = "4816b78f-1489-44c1-9060-4b19d5fa9362"

# MinIO config
MINIO_ENDPOINT   = "http://10.167.23.146:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "Minio@2024"
MINIO_BUCKET     = "raw"
MINIO_PATH_PREFIX = "airbyte"

# MySQL config
MYSQL_HOST     = "10.167.23.146"
MYSQL_PORT     = 3306
MYSQL_DATABASE = "shopflow"
MYSQL_USER     = "root"
MYSQL_PASSWORD = "MySQL@2024"


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def get_token() -> str:
    data = json.dumps({
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }).encode()
    req = urllib.request.Request(
        f"{AIRBYTE_URL}/api/public/v1/applications/token",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)["access_token"]


def api(path: str, payload: dict, token: str, method: str = "POST") -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{AIRBYTE_URL}/api/v1/{path}",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  HTTP {e.code} on {path}: {body[:300]}")
        raise


# ── Step 1: S3/MinIO destination ──────────────────────────────────────────────

def create_minio_destination(token: str) -> str:
    print("Creating S3/MinIO destination ...")
    payload = {
        "workspaceId": WORKSPACE_ID,
        "name": "MinIO Raw Bucket (Parquet)",
        "destinationDefinitionId": S3_DEST_DEF_ID,
        "connectionConfiguration": {
            "s3_bucket_name": MINIO_BUCKET,
            "s3_bucket_path": MINIO_PATH_PREFIX,
            "s3_bucket_region": "us-east-1",
            "access_key_id": MINIO_ACCESS_KEY,
            "secret_access_key": MINIO_SECRET_KEY,
            "s3_endpoint": MINIO_ENDPOINT,
            "s3_path_format": "${NAMESPACE}/${STREAM_NAME}/${YEAR}_${MONTH}_${DAY}_${EPOCH}_",
            "file_name_pattern": "{date}",
            "format": {
                "format_type": "Parquet",
                "compression_codec": "UNCOMPRESSED",
                "block_size_mb": 128,
                "max_padding_size_mb": 8,
                "page_size_kb": 1024,
                "dictionary_page_size_kb": 1024,
                "dictionary_encoding": True,
            },
        },
    }
    result = api("destinations/create", payload, token)
    dest_id = result["destinationId"]
    print(f"  -> destination created: {dest_id}")
    return dest_id


# ── Step 2: MySQL CDC source ──────────────────────────────────────────────────

def create_mysql_source(token: str) -> str:
    print("Creating MySQL CDC source ...")
    payload = {
        "workspaceId": WORKSPACE_ID,
        "name": "ShopFlow MySQL CDC",
        "sourceDefinitionId": MYSQL_SOURCE_DEF_ID,
        "connectionConfiguration": {
            "host": MYSQL_HOST,
            "port": MYSQL_PORT,
            "database": MYSQL_DATABASE,
            "username": MYSQL_USER,
            "password": MYSQL_PASSWORD,
            "replication_method": {
                "method": "CDC",
                "initial_waiting_seconds": 120,
                "server_time_zone": "UTC",
            },
            "ssl_mode": {"mode": "preferred"},
            "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
        },
    }
    result = api("sources/create", payload, token)
    src_id = result["sourceId"]
    print(f"  -> source created: {src_id}")
    return src_id


# ── Step 3: Discover schema + create connection ───────────────────────────────

def discover_catalog(source_id: str, token: str) -> str:
    print("Discovering MySQL schema (this may take ~30s) ...")
    result = api("sources/discover_schema", {"sourceId": source_id}, token)
    catalog_id = result.get("catalogId") or result.get("catalog", {}).get("catalogId")
    # Fall back: extract from the catalog object
    if not catalog_id:
        catalog_id = result["catalog"]["catalogId"]
    print(f"  -> catalogId: {catalog_id}")
    return catalog_id, result["catalog"]


def create_connection(source_id: str, dest_id: str, catalog: dict,
                      catalog_id: str, token: str) -> str:
    print("Creating MySQL → MinIO connection ...")

    pk_map = {
        "customers": [["customer_id"]],
        "products":  [["product_id"]],
        "orders":    [["order_id"]],
    }

    # Pass back the full stream object (including jsonSchema) from discovery
    streams = []
    for entry in catalog.get("streams", []):
        s = entry["stream"]
        name = s["name"]
        streams.append({
            "stream": s,          # full object with jsonSchema
            "config": {
                "syncMode": "incremental",
                "destinationSyncMode": "append_dedup",
                "cursorField": ["updated_at"],
                "primaryKey": pk_map.get(name, [["id"]]),
                "selected": True,
                "aliasName": name,
                "destinationNamespace": "mysql",
            },
        })

    payload = {
        "sourceId": source_id,
        "destinationId": dest_id,
        "name": "MySQL CDC → MinIO Parquet",
        "namespaceDefinition": "customformat",
        "namespaceFormat": "mysql",
        "prefix": "",
        "syncCatalog": {"streams": streams},
        "sourceDiscoveryCatalogId": catalog_id,
        "scheduleType": "manual",
        "status": "active",
    }
    result = api("connections/create", payload, token)
    conn_id = result["connectionId"]
    print(f"  -> connection created: {conn_id}")
    return conn_id


# ── Step 4: Trigger sync ──────────────────────────────────────────────────────

def api_public(path: str, payload: dict, token: str, method: str = "POST") -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{AIRBYTE_URL}/api/public/v1/{path}",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  HTTP {e.code} on public/{path}: {body[:300]}")
        raise


def trigger_sync(connection_id: str, token: str) -> str:
    print(f"Triggering initial sync for connection {connection_id} ...")
    result = api_public("jobs", {
        "connectionId": connection_id,
        "jobType": "sync",
    }, token)
    job_id = result.get("jobId") or result.get("id")
    print(f"  -> job started: {job_id}")
    return str(job_id)


def wait_for_job(job_id: str, token: str, timeout: int = 300) -> str:
    print(f"Waiting for job {job_id} to complete (timeout {timeout}s) ...")
    start = time.time()
    while time.time() - start < timeout:
        result = api_public(f"jobs/{job_id}", {}, token, method="GET")
        status = result.get("status")
        print(f"  status: {status}")
        if status in ("succeeded", "failed", "cancelled"):
            return status
        time.sleep(10)
    return "timeout"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Phase 2: Airbyte → MinIO Setup")
    print("=" * 60)

    token = get_token()
    print("Authenticated. Token acquired.")

    # Reuse existing IDs if already created (idempotent re-run)
    dest_id = "02f98fd0-ac68-44a1-af0a-f4911fabc90a"
    src_id  = "6a0b6357-4dec-4fd3-839a-734793ae04a6"
    print(f"Using existing destination: {dest_id}")
    print(f"Using existing source:      {src_id}")

    # Reuse existing connection if already created
    conn_id = "9993f6c9-040d-47bb-830b-31de9137a477"
    print(f"Using existing connection: {conn_id}")

    # Trigger sync
    token = get_token()
    job_id = trigger_sync(conn_id, token)

    # Wait for completion
    token = get_token()
    final_status = wait_for_job(job_id, token, timeout=300)

    print()
    print(f"MySQL sync job finished with status: {final_status}")
    print()
    print("IDs summary (save these for Airflow DAG):")
    print(f"  AIRBYTE_WORKSPACE_ID  = {WORKSPACE_ID}")
    print(f"  AIRBYTE_DEST_ID       = {dest_id}")
    print(f"  AIRBYTE_MYSQL_SRC_ID  = {src_id}")
    print(f"  AIRBYTE_MYSQL_CONN_ID = {conn_id}")
    print()

    # Write IDs to a JSON file for later use
    ids = {
        "workspace_id": WORKSPACE_ID,
        "destination_id": dest_id,
        "mysql_source_id": src_id,
        "mysql_connection_id": conn_id,
        "mysql_sync_job_id": job_id,
        "mysql_sync_status": final_status,
    }
    with open("ingestion/airbyte/connection-configs/airbyte_ids.json", "w") as f:
        json.dump(ids, f, indent=2)
    print("IDs saved to ingestion/airbyte/connection-configs/airbyte_ids.json")


if __name__ == "__main__":
    main()
