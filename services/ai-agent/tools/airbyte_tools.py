import os
import time
import requests
from datetime import datetime, timezone
from langchain_core.tools import tool


def _ts(unix: int | None) -> str:
    """Convert a Unix epoch int from the Airbyte API to a readable UTC string."""
    if not unix:
        return "n/a"
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _records(attempt: dict) -> str:
    """Extract synced record count from an Airbyte attempt — field varies by API version."""
    stats = attempt.get("totalStats") or attempt.get("streamStats") or {}
    if isinstance(stats, dict):
        for key in ("recordsCommitted", "recordsEmitted", "recordsSynced"):
            if key in stats:
                return str(stats[key])
    # flat field used by older API versions
    return str(attempt.get("recordsSynced", attempt.get("records_synced", "n/a")))

# Known IDs from ingestion/airbyte/connection-configs/
WORKSPACE_ID  = "80c3e872-96c3-451f-9f63-74e33001a0a6"
MYSQL_CONN_ID = "9993f6c9-040d-47bb-830b-31de9137a477"
MINIO_DEST_ID = "02f98fd0-ac68-44a1-af0a-f4911fabc90a"

KNOWN_CONNECTIONS = {
    "9993f6c9-040d-47bb-830b-31de9137a477": "MySQL CDC → MinIO",
    "66c7e612-8749-4eb5-bcd8-828d3ce323c2": "JSONPlaceholder REST API → MinIO",
    "fecf7237-1234-4eb5-bcd8-828d3ce323c2": "SAP OData API → MinIO",
    "d37118e9-abcd-47bb-830b-31de9137a477": "PostgreSQL → MinIO (inactive)",
}

_token_cache: dict = {}
_token_lock = __import__("threading").Lock()


def _base() -> str:
    return os.getenv("AIRBYTE_URL", "http://airbyte-abctl-control-plane:80")


def _get_token() -> str:
    """
    Exchange client credentials for a Bearer token.

    Caching strategy:
    - Token is cached with a 55-minute TTL (Airbyte tokens expire at 60 min)
    - A 60-second buffer ensures we never use a token that's about to expire
    - Thread-safe via a module-level lock to avoid thundering herd on refresh
    """
    now = time.time()
    with _token_lock:
        if _token_cache.get("token") and _token_cache.get("expires_at", 0) > now + 60:
            return _token_cache["token"]

        client_id     = os.getenv("AIRBYTE_CLIENT_ID", "")
        client_secret = os.getenv("AIRBYTE_CLIENT_SECRET", "")

        if not client_id or not client_secret:
            raise ValueError(
                "Airbyte credentials not configured.\n"
                "Set these in your .env file:\n"
                "  AIRBYTE_CLIENT_ID=<id from Airbyte UI → Settings → Applications>\n"
                "  AIRBYTE_CLIENT_SECRET=<secret>\n"
                "Then restart the AI agent server.\n"
                "Alternative: trigger syncs via Airflow DAG 'shopflow_datalake_pipeline'."
            )

        resp = requests.post(
            f"{_base()}/api/v1/applications/token",
            json={"client_id": client_id, "client_secret": client_secret},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data  = resp.json()
        token = data["access_token"]

        # Use server-provided expiry if available; otherwise assume 55 min
        expires_in = data.get("expires_in", 3300)
        _token_cache["token"]      = token
        _token_cache["expires_at"] = now + int(expires_in) - 60  # 60s safety buffer
        return token


def _headers() -> dict:
    token = _get_token()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }


def _unreachable_msg() -> str:
    return (
        "⚠️ Airbyte is unreachable. It runs in Kubernetes via abctl.\n"
        "Check pods: kubectl get pods -n airbyte-abctl\n"
        "Fallback: trigger syncs via Airflow DAG 'shopflow_datalake_pipeline'."
    )


def _no_credentials_msg() -> str:
    return (
        "STATUS: credentials_not_configured\n\n"
        "SETUP: Add to services/ai-agent/.env then restart server.py:\n"
        "  AIRBYTE_CLIENT_ID=<from Airbyte UI → Settings → Applications>\n"
        "  AIRBYTE_CLIENT_SECRET=<secret>\n\n"
        "KNOWN_CONNECTIONS (4 total, from design-time config):\n"
        "  conn_id=9993f6c9 | name=MySQL CDC → MinIO | source=shopflow MySQL | "
        "destination=raw/airbyte/mysql/ | tables=customers,orders,products | status=active\n"
        "  conn_id=66c7e612 | name=JSONPlaceholder REST → MinIO | source=JSONPlaceholder API | "
        "destination=raw/airbyte/rest/ | tables=posts,comments,users | status=active\n"
        "  conn_id=fecf7237 | name=SAP OData API → MinIO | source=SAP Flask :5001 | "
        "destination=raw/airbyte/sap/ | tables=vendors,purchase_orders,cost_centers | status=active\n"
        "  conn_id=d37118e9 | name=PostgreSQL → MinIO | source=postgres saas_users | "
        "destination=raw/airbyte/postgres/ | tables=users,events | status=inactive\n\n"
        "ARCHITECTURE: Airbyte runs in Kubernetes (abctl), NOT Docker Compose.\n"
        "UI: http://localhost:8000 | Check: kubectl get pods -n airbyte-abctl\n"
        "ALTERNATIVE: shopflow_datalake_pipeline DAG in Airflow manages syncs via Vault creds.\n"
        "VOLUME: MySQL CDC is highest-volume (core shopflow tables). SAP handles financials.\n"
        "RELIABILITY: MySQL CDC and SAP are critical path. REST (JSONPlaceholder) is reference data.\n"
    )


@tool
def list_airbyte_connections(max_connections: str = "20") -> str:
    """List all Airbyte connections with status AND latest sync job details (records, duration, last sync time).
    max_connections: maximum connections to return, e.g. "20" (default).
    This is the primary tool for connection health — call it first for any sync status question."""
    try:
        resp = requests.post(
            f"{_base()}/api/v1/connections/list",
            headers=_headers(),
            json={"workspaceId": WORKSPACE_ID},
            timeout=10,
        )
        resp.raise_for_status()
        conns = resp.json().get("connections", [])
        if not conns:
            return "No connections found."


        lines = [f"Airbyte connections ({len(conns)}):"]
        for c in conns:
            conn_id = c["connectionId"]
            sched = c.get("scheduleData", {}).get("basicSchedule", {})
            freq = (f"{sched.get('units','')} {sched.get('timeUnit','')}".strip()
                    or c.get("scheduleType", "manual"))
            name = KNOWN_CONNECTIONS.get(conn_id, c.get("name", "unnamed"))

            # Fetch latest sync job inline to avoid requiring extra tool calls
            job_info = "no sync jobs"
            try:
                job_resp = requests.post(
                    f"{_base()}/api/v1/jobs/list",
                    headers=_headers(),
                    json={"configId": conn_id, "configTypes": ["sync"],
                          "pagination": {"pageSize": 1}},
                    timeout=8,
                )
                if job_resp.ok:
                    jobs = job_resp.json().get("jobs", [])
                    if jobs:
                        j = jobs[0]["job"]
                        attempts = jobs[0].get("attempts", [])
                        last = attempts[-1] if attempts else {}
                        duration = ""
                        if last.get("endedAt") and last.get("createdAt"):
                            secs = last["endedAt"] - last["createdAt"]
                            duration = f"{secs//60}m {secs%60}s"
                        records = _records(last)
                        started = _ts(j.get("createdAt"))
                        job_info = (
                            f"status={j.get('status','?')} | "
                            f"last_sync={started} | "
                            f"records={records} | "
                            f"duration={duration or 'n/a'} | "
                            f"job_id={j.get('id','n/a')}"
                        )
            except Exception:
                pass

            lines.append(
                f"\n  [{conn_id[:8]}] {name}\n"
                f"    connection_status={c.get('status','?')} | schedule={freq}\n"
                f"    latest_job: {job_info}"
            )

        healthy = sum(1 for c in conns if c.get("status") == "active")
        lines.append(f"\nSummary: {healthy}/{len(conns)} connections active")
        return "\n".join(lines)
    except requests.exceptions.ConnectionError:
        return _unreachable_msg()
    except ValueError as e:
        if "credentials not configured" in str(e).lower() or "client_id" in str(e).lower():
            return _no_credentials_msg()
        return f"Error listing connections: {str(e)}"
    except Exception as e:
        return f"Error listing connections: {str(e)}"


@tool
def get_airbyte_connection_status(connection_id: str = MYSQL_CONN_ID) -> str:
    """Get detailed status of one Airbyte connection by its UUID."""
    try:
        resp = requests.post(
            f"{_base()}/api/v1/connections/get",
            headers=_headers(),
            json={"connectionId": connection_id},
            timeout=10,
        )
        if resp.status_code == 404:
            return f"Connection {connection_id} not found."
        resp.raise_for_status()
        c = resp.json()
        name = KNOWN_CONNECTIONS.get(connection_id, c.get("name", connection_id))
        sched = c.get("scheduleData", {}).get("basicSchedule", {})
        freq = f"{sched.get('units','')} {sched.get('timeUnit','')}".strip() or "manual"
        return (
            f"Connection: {name}\n"
            f"ID:       {connection_id}\n"
            f"Status:   {c.get('status','unknown')}\n"
            f"Schedule: {freq}\n"
            f"Source ID:  {c.get('sourceId','n/a')}\n"
            f"Dest ID:    {c.get('destinationId','n/a')}"
        )
    except requests.exceptions.ConnectionError:
        return _unreachable_msg()
    except Exception as e:
        return f"Error: {str(e)}"


@tool
def get_latest_sync_job(connection_id: str = MYSQL_CONN_ID) -> str:
    """Get the most recent sync job result for an Airbyte connection (records synced, duration, status)."""
    try:
        resp = requests.post(
            f"{_base()}/api/v1/jobs/list",
            headers=_headers(),
            json={
                "configId": connection_id,
                "configTypes": ["sync"],
                "pagination": {"pageSize": 1}
            },
            timeout=10,
        )
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        if not jobs:
            return f"No sync jobs found for connection {connection_id}."
        j = jobs[0]["job"]
        attempts = jobs[0].get("attempts", [])
        last = attempts[-1] if attempts else {}
        duration = ""
        if last.get("endedAt") and last.get("createdAt"):
            secs = last["endedAt"] - last["createdAt"]
            duration = f"{secs//60}m {secs%60}s"
        name = KNOWN_CONNECTIONS.get(connection_id, connection_id)
        stats = last.get("totalStats") or {}
        bytes_synced = stats.get("bytesCommitted", last.get("bytesSynced", "n/a"))
        return (
            f"Latest sync — {name}:\n"
            f"  Job ID:         {j.get('id')}\n"
            f"  Status:         {j.get('status')}\n"
            f"  Started:        {_ts(j.get('createdAt'))}\n"
            f"  Duration:       {duration or 'n/a'}\n"
            f"  Records synced: {_records(last)}\n"
            f"  Bytes synced:   {bytes_synced}"
        )
    except requests.exceptions.ConnectionError:
        return _unreachable_msg()
    except ValueError as e:
        if "credentials not configured" in str(e).lower() or "client_id" in str(e).lower():
            return _no_credentials_msg()
        return f"Error fetching job: {str(e)}"
    except Exception as e:
        return f"Error fetching job: {str(e)}"


@tool
def trigger_airbyte_sync(connection_id: str = MYSQL_CONN_ID) -> str:
    """Manually trigger an Airbyte sync for a given connection UUID."""
    try:
        resp = requests.post(
            f"{_base()}/api/v1/connections/sync",
            headers=_headers(),
            json={"connectionId": connection_id},
            timeout=15,
        )
        if resp.status_code in (404, 503, 502):
            return (
                f"Cannot trigger sync (HTTP {resp.status_code}). "
                "Use Airflow DAG 'shopflow_datalake_pipeline' instead."
            )
        resp.raise_for_status()
        job = resp.json().get("job", {})
        name = KNOWN_CONNECTIONS.get(connection_id, connection_id)
        return (
            f"Sync triggered for {name}\n"
            f"Job ID: {job.get('id','n/a')} | Status: {job.get('status','pending')}"
        )
    except requests.exceptions.ConnectionError:
        return _unreachable_msg()
    except ValueError as e:
        if "credentials not configured" in str(e).lower() or "client_id" in str(e).lower():
            return _no_credentials_msg()
        return f"Error triggering sync: {str(e)}"
    except Exception as e:
        return f"Error triggering sync: {str(e)}"


@tool
def get_airbyte_source_info() -> str:
    """List all configured Airbyte sources in the workspace."""
    try:
        resp = requests.post(
            f"{_base()}/api/v1/sources/list",
            headers=_headers(),
            json={"workspaceId": WORKSPACE_ID},
            timeout=10,
        )
        resp.raise_for_status()
        sources = resp.json().get("sources", [])
        if not sources:
            return "No sources configured."
        lines = [
            f"  {s.get('name','?')} | type={s.get('sourceName','?')} | id={s['sourceId'][:8]}…"
            for s in sources
        ]
        return f"Airbyte sources ({len(sources)}):\n" + "\n".join(lines)
    except requests.exceptions.ConnectionError:
        return _unreachable_msg()
    except Exception as e:
        return f"Error: {str(e)}"
