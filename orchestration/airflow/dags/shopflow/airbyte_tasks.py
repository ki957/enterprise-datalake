"""Airbyte auth, job management, and MySQL CDC sync task."""

import os
import time

AIRBYTE_URL           = os.getenv("AIRBYTE_URL", "http://airbyte-abctl-control-plane:80")
AIRBYTE_CLIENT_ID     = os.getenv("AIRBYTE_CLIENT_ID", "")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET", "")
AIRBYTE_MYSQL_CONN    = os.getenv("AIRBYTE_MYSQL_CONN_ID", "9993f6c9-040d-47bb-830b-31de9137a477")

_TOKEN_TTL = 3000  # refresh every ~50 min (tokens expire at 55 min)


def _get_airbyte_token() -> str:
    import requests
    resp = requests.post(
        f"{AIRBYTE_URL}/api/public/v1/applications/token",
        json={"client_id": AIRBYTE_CLIENT_ID, "client_secret": AIRBYTE_CLIENT_SECRET},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _airbyte_trigger_sync(connection_id: str, token: str) -> str:
    import requests
    resp = requests.post(
        f"{AIRBYTE_URL}/api/public/v1/jobs",
        json={"connectionId": connection_id, "jobType": "sync"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return str(data.get("jobId") or data.get("id"))


def _airbyte_wait_for_job(job_id: str, token: str, timeout: int = 600) -> str:
    import requests
    start = time.time()
    token_refreshed_at = start
    while time.time() - start < timeout:
        if time.time() - token_refreshed_at >= _TOKEN_TTL:
            try:
                token = _get_airbyte_token()
                token_refreshed_at = time.time()
                print(f"  Airbyte token refreshed for job {job_id}")
            except Exception as exc:
                print(f"  [WARN] Token refresh failed: {exc} — continuing with existing token")
        resp = requests.get(
            f"{AIRBYTE_URL}/api/public/v1/jobs/{job_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status")
        print(f"  Airbyte job {job_id} status: {status}")
        if status == "succeeded":
            return status
        if status in ("failed", "cancelled"):
            raise RuntimeError(f"Airbyte job {job_id} ended with status: {status}")
        time.sleep(15)
    raise TimeoutError(f"Airbyte job {job_id} did not complete within {timeout}s")


def trigger_airbyte_mysql_sync(**ctx):
    """Trigger Airbyte MySQL → MinIO CDC sync and wait for completion."""
    print("Triggering Airbyte MySQL CDC sync ...")
    token  = _get_airbyte_token()
    job_id = _airbyte_trigger_sync(AIRBYTE_MYSQL_CONN, token)
    print(f"  Job started: {job_id}")
    token  = _get_airbyte_token()  # refresh before long wait
    status = _airbyte_wait_for_job(job_id, token)
    print(f"  MySQL sync completed: {status}")
    ctx["ti"].xcom_push(key="mysql_job_id", value=job_id)
