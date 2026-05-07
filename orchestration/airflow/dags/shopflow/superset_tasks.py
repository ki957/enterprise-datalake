"""Superset dataset refresh task."""

import os

SUPERSET_URL          = os.getenv("SUPERSET_URL", "http://superset:8088")
SUPERSET_USER         = os.getenv("SUPERSET_USER", "admin")
SUPERSET_PASS         = os.getenv("SUPERSET_PASSWORD", "Superset@2024")
SUPERSET_DATASET_IDS  = [9, 10, 11, 12, 13, 14]


def refresh_superset_dashboard(**ctx):
    """Refresh all 6 Superset datasets so charts reflect latest Gold data."""
    import requests

    session    = requests.Session()
    login_resp = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_USER, "password": SUPERSET_PASS, "provider": "db"},
        timeout=30,
    )
    login_resp.raise_for_status()
    token = login_resp.json()["access_token"]

    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Referer": SUPERSET_URL,
    })

    csrf_resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", timeout=30)
    csrf_resp.raise_for_status()
    session.headers["X-CSRFToken"] = csrf_resp.json()["result"]

    refreshed, failed = [], []
    for ds_id in SUPERSET_DATASET_IDS:
        resp = session.put(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}/refresh", timeout=30)
        if resp.status_code == 200:
            refreshed.append(ds_id)
            print(f"  [OK] Dataset {ds_id} refreshed")
        else:
            failed.append(ds_id)
            print(f"  [WARN] Dataset {ds_id} refresh returned {resp.status_code}: {resp.text[:200]}")

    print(f"\nSuperset refresh: {len(refreshed)} OK, {len(failed)} warnings")
    ctx["ti"].xcom_push(key="superset_refreshed", value=len(refreshed))
