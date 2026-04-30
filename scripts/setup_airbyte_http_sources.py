#!/usr/bin/env python3
"""
Fix 2 & 3: Create declarative Airbyte sources for SAP API and JSONPlaceholder REST API,
connect each to the MinIO Raw Bucket destination, trigger syncs, and verify.

SAP API   → raw/airbyte/sap/{vendors,purchase_orders,cost_centers}/
REST API  → raw/airbyte/rest/{users,posts,comments}/
"""

import json
import time
import urllib.request
import urllib.error

AIRBYTE_URL   = "http://localhost:8000"
CLIENT_ID     = "4ef6ae1b-2abb-479c-8476-f46a3d4e1895"
CLIENT_SECRET = "DqSQF98hSB9kvixIJPIfwC6KeQHr5IEY"
WORKSPACE_ID  = "80c3e872-96c3-451f-9f63-74e33001a0a6"

# Existing MinIO destination (already fixed to 172.17.0.1)
MINIO_DEST_ID = "02f98fd0-ac68-44a1-af0a-f4911fabc90a"

# SAP API is accessible from k8s pods via Docker bridge
SAP_BASE_URL  = "http://172.17.0.1:5001"
REST_BASE_URL = "https://jsonplaceholder.typicode.com"


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_token() -> str:
    data = json.dumps({"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}).encode()
    req = urllib.request.Request(
        f"{AIRBYTE_URL}/api/public/v1/applications/token", data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)["access_token"]


def api(path: str, payload: dict, token: str, method: str = "POST") -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{AIRBYTE_URL}/api/v1/{path}", data=data,
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {token}"},
        method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  HTTP {e.code} on {path}: {body[:400]}")
        raise


def api_public(path: str, payload: dict, token: str, method: str = "POST") -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{AIRBYTE_URL}/api/public/v1/{path}", data=data,
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {token}"},
        method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  HTTP {e.code} on public/{path}: {body[:400]}")
        raise


# ── Declarative manifests ─────────────────────────────────────────────────────

def sap_manifest() -> dict:
    """
    Declarative manifest for the SAP Flask API.
    Response shape: {"results": [...], "next_page": <int|null>, "count": N}
    Streams: vendors, purchase_orders, cost_centers
    """
    def stream(name: str, path: str) -> dict:
        return {
            "type": "DeclarativeStream",
            "name": name,
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": SAP_BASE_URL,
                    "path": path,
                    "http_method": "GET",
                    "request_parameters": {},
                    "request_headers": {},
                    "authenticator": {"type": "NoAuth"},
                    "error_handler": {"type": "DefaultErrorHandler"},
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {
                        "type": "DpathExtractor",
                        "field_path": ["results"],
                    },
                },
                "paginator": {
                    "type": "DefaultPaginator",
                    "pagination_strategy": {
                        "type": "PageIncrement",
                        "start_from_page": 1,
                        "page_size": 100,
                    },
                    "page_token_option": {
                        "type": "RequestOption",
                        "inject_into": "request_parameter",
                        "field_name": "page",
                    },
                    "page_size_option": {
                        "type": "RequestOption",
                        "inject_into": "request_parameter",
                        "field_name": "page_size",
                    },
                    "stop_condition": {
                        "type": "WaitUntilTimeFromHeader",
                    },
                },
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object",
                           "additionalProperties": True, "properties": {}},
            },
        }

    return {
        "version": "0.79.1",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["vendors"]},
        "streams": [
            stream("vendors",          "/api/vendors"),
            stream("purchase_orders",  "/api/purchase-orders"),
            stream("cost_centers",     "/api/cost-centers"),
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": SAP_BASE_URL,
            "connection_specification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "ShopFlow SAP API",
                "type": "object",
                "required": [],
                "additionalProperties": True,
                "properties": {},
            },
        },
    }


def rest_manifest() -> dict:
    """
    Declarative manifest for JSONPlaceholder REST API.
    Response shape: direct array [...]
    Streams: users, posts, comments
    """
    def stream(name: str, path: str) -> dict:
        return {
            "type": "DeclarativeStream",
            "name": name,
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": REST_BASE_URL,
                    "path": path,
                    "http_method": "GET",
                    "request_parameters": {},
                    "request_headers": {},
                    "authenticator": {"type": "NoAuth"},
                    "error_handler": {"type": "DefaultErrorHandler"},
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {
                        "type": "DpathExtractor",
                        "field_path": [],  # Root array response
                    },
                },
                "paginator": {"type": "NoPagination"},
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object",
                           "additionalProperties": True, "properties": {}},
            },
        }

    return {
        "version": "0.79.1",
        "type": "DeclarativeSource",
        "check": {"type": "CheckStream", "stream_names": ["users"]},
        "streams": [
            stream("users",    "/users"),
            stream("posts",    "/posts"),
            stream("comments", "/comments"),
        ],
        "spec": {
            "type": "Spec",
            "documentation_url": REST_BASE_URL,
            "connection_specification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "JSONPlaceholder REST API",
                "type": "object",
                "required": [],
                "additionalProperties": True,
                "properties": {},
            },
        },
    }


# ── Source creation ───────────────────────────────────────────────────────────

def create_source(name: str, source_def_id: str, manifest: dict, token: str) -> str:
    print(f"Creating source: {name} ...")
    payload = {
        "workspaceId": WORKSPACE_ID,
        "name": name,
        "sourceDefinitionId": source_def_id,
        "connectionConfiguration": {
            "__injected_declarative_manifest": manifest,
        },
    }
    result = api("sources/create", payload, token)
    src_id = result["sourceId"]
    print(f"  -> source created: {src_id}")
    return src_id


def check_source(source_id: str, token: str) -> bool:
    print(f"  Checking source connection {source_id} ...")
    try:
        result = api("sources/check_connection", {"sourceId": source_id}, token)
        status = result.get("status")
        msg = result.get("message", "")
        print(f"  -> status: {status}" + (f" | {msg[:150]}" if msg else ""))
        return status == "succeeded"
    except Exception as e:
        print(f"  -> check failed: {e}")
        return False


def discover_catalog(source_id: str, token: str) -> tuple:
    print(f"  Discovering schema for {source_id} (may take ~30s) ...")
    result = api("sources/discover_schema", {"sourceId": source_id}, token)
    catalog = result.get("catalog", {})
    catalog_id = result.get("catalogId") or catalog.get("catalogId")
    streams = catalog.get("streams", [])
    print(f"  -> catalogId: {catalog_id} | streams: {[e['stream']['name'] for e in streams]}")
    return catalog_id, catalog


def create_connection(name: str, source_id: str, dest_id: str,
                      catalog: dict, catalog_id: str,
                      namespace_prefix: str, token: str) -> str:
    print(f"Creating connection: {name} ...")

    streams = []
    for entry in catalog.get("streams", []):
        s = entry["stream"]
        stream_name = s["name"]
        streams.append({
            "stream": s,
            "config": {
                "syncMode": "full_refresh",
                "destinationSyncMode": "overwrite",
                "selected": True,
                "aliasName": stream_name,
                "destinationNamespace": f"{namespace_prefix}/{stream_name}",
            },
        })

    payload = {
        "sourceId": source_id,
        "destinationId": dest_id,
        "name": name,
        "namespaceDefinition": "customformat",
        "namespaceFormat": namespace_prefix,
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


def trigger_and_wait(conn_id: str, token: str, label: str, timeout: int = 300) -> dict:
    print(f"Triggering sync for {label} ...")
    result = api_public("jobs", {"connectionId": conn_id, "jobType": "sync"}, token)
    job_id = result.get("jobId") or result.get("id")
    print(f"  -> job started: {job_id}")

    start = time.time()
    while time.time() - start < timeout:
        time.sleep(10)
        token = get_token()
        try:
            res = api_public(f"jobs/{job_id}", {}, token, method="GET")
            status = res.get("status")
            print(f"  [{int(time.time()-start)}s] status={status}")
            if status in ("succeeded", "failed", "cancelled"):
                return res
        except Exception as e:
            print(f"  poll error: {e}")

    return {"status": "timeout"}


def get_stream_stats(job_id: int, token: str) -> list:
    try:
        result = api("jobs/get", {"id": job_id}, token)
        stats = []
        for att in result.get("attempts", []):
            a = att.get("attempt", {})
            if a.get("status") == "succeeded":
                for ss in a.get("streamStats", []):
                    s = ss.get("stats", {})
                    stats.append({
                        "stream": ss.get("streamName"),
                        "emitted": s.get("recordsEmitted", 0),
                        "committed": s.get("recordsCommitted", 0),
                    })
        return stats
    except Exception:
        return []


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Fix 2 & 3: SAP API + REST API → Airbyte → MinIO")
    print("=" * 60)

    token = get_token()
    print("Authenticated.\n")

    # ── FIX 2: SAP API ────────────────────────────────────────────────────────
    print("─" * 50)
    print("FIX 2: SAP API (ShopFlow) → MinIO")
    print("─" * 50)

    # Existing SAP source def (created in background task)
    sap_def_id = "1d4a10d7-c66b-484b-8747-74e63f95be1f"

    token = get_token()
    sap_src_id = create_source("ShopFlow SAP API", sap_def_id, sap_manifest(), token)

    token = get_token()
    sap_ok = check_source(sap_src_id, token)

    if not sap_ok:
        print("  WARNING: source check did not succeed — attempting discover anyway")

    token = get_token()
    sap_catalog_id, sap_catalog = discover_catalog(sap_src_id, token)

    token = get_token()
    sap_conn_id = create_connection(
        "ShopFlow SAP API → MinIO Parquet",
        sap_src_id, MINIO_DEST_ID,
        sap_catalog, sap_catalog_id,
        "sap", token,
    )

    token = get_token()
    sap_result = trigger_and_wait(sap_conn_id, token, "SAP API", timeout=300)
    sap_job_id = sap_result.get("jobId") or sap_result.get("id")

    print(f"\nSAP sync final status: {sap_result.get('status')}")
    if sap_job_id:
        token = get_token()
        for s in get_stream_stats(int(sap_job_id), token):
            print(f"  {s['stream']}: emitted={s['emitted']} committed={s['committed']}")

    print()

    # ── FIX 3: REST API (JSONPlaceholder) ────────────────────────────────────
    print("─" * 50)
    print("FIX 3: JSONPlaceholder REST API → MinIO")
    print("─" * 50)

    # Create a new source definition for JSONPlaceholder
    token = get_token()
    print("Creating source definition for JSONPlaceholder REST API ...")
    rest_def_result = api("source_definitions/create_custom", {
        "workspaceId": WORKSPACE_ID,
        "sourceDefinition": {
            "name": "JSONPlaceholder REST API",
            "dockerRepository": "airbyte/source-declarative-manifest",
            "dockerImageTag": "latest",
            "documentationUrl": REST_BASE_URL,
        },
    }, token)
    rest_def_id = rest_def_result["sourceDefinitionId"]
    print(f"  -> source definition: {rest_def_id}")

    token = get_token()
    rest_src_id = create_source("JSONPlaceholder REST API", rest_def_id, rest_manifest(), token)

    token = get_token()
    rest_ok = check_source(rest_src_id, token)

    if not rest_ok:
        print("  WARNING: source check did not succeed — attempting discover anyway")

    token = get_token()
    rest_catalog_id, rest_catalog = discover_catalog(rest_src_id, token)

    token = get_token()
    rest_conn_id = create_connection(
        "JSONPlaceholder REST API → MinIO Parquet",
        rest_src_id, MINIO_DEST_ID,
        rest_catalog, rest_catalog_id,
        "rest", token,
    )

    token = get_token()
    rest_result = trigger_and_wait(rest_conn_id, token, "REST API", timeout=300)
    rest_job_id = rest_result.get("jobId") or rest_result.get("id")

    print(f"\nREST sync final status: {rest_result.get('status')}")
    if rest_job_id:
        token = get_token()
        for s in get_stream_stats(int(rest_job_id), token):
            print(f"  {s['stream']}: emitted={s['emitted']} committed={s['committed']}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    ids = {
        "sap_source_def_id":  sap_def_id,
        "sap_source_id":      sap_src_id,
        "sap_connection_id":  sap_conn_id,
        "rest_source_def_id": rest_def_id,
        "rest_source_id":     rest_src_id,
        "rest_connection_id": rest_conn_id,
    }
    for k, v in ids.items():
        print(f"  {k}: {v}")

    with open("ingestion/airbyte/connection-configs/airbyte_http_ids.json", "w") as f:
        json.dump(ids, f, indent=2)
    print("\nIDs saved to ingestion/airbyte/connection-configs/airbyte_http_ids.json")


if __name__ == "__main__":
    main()
