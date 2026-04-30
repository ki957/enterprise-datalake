#!/usr/bin/env python3
"""
Superset Dashboard Importer
============================
Imports dashboard ZIPs from infrastructure/superset/dashboards/ into
a running Superset instance. Run this after `make visualization` on a
fresh environment to restore dashboards without going through the UI.

Usage:
    python scripts/import_superset_dashboards.py

The ZIPs were created by scripts/setup_superset.py (or via the Superset UI
export). They live in git so every team member gets the same dashboards.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

SUPERSET_URL  = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.getenv("SUPERSET_USER", "admin")
SUPERSET_PASS = os.getenv("SUPERSET_PASSWORD", "Superset@2024")

DASHBOARD_DIR = Path(__file__).parent.parent / "infrastructure" / "superset" / "dashboards"

_token = None


def _get_token() -> str:
    global _token
    if _token:
        return _token
    data = json.dumps({"username": SUPERSET_USER, "password": SUPERSET_PASS,
                       "provider": "db", "refresh": True}).encode()
    req = urllib.request.Request(
        f"{SUPERSET_URL}/api/v1/security/login",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        _token = json.loads(resp.read())["access_token"]
    return _token


def _get_csrf() -> str:
    token = _get_token()
    req = urllib.request.Request(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read()).get("result", "")


def wait_for_superset(timeout=120):
    print(f"Waiting for Superset at {SUPERSET_URL} ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{SUPERSET_URL}/health", timeout=5)
            print("  Ready.")
            return
        except Exception:
            time.sleep(4)
    print("ERROR: Superset not reachable.", file=sys.stderr)
    sys.exit(1)


def import_zip(zip_path: Path) -> None:
    """Import a dashboard ZIP via the Superset REST API."""
    token = _get_token()
    csrf  = _get_csrf()

    zip_bytes = zip_path.read_bytes()
    boundary  = "----FormBoundary7MA4YWxkTrZu0gW"

    # Build multipart/form-data body manually (no external deps)
    body  = f"--{boundary}\r\n"
    body += f'Content-Disposition: form-data; name="formData"; filename="{zip_path.name}"\r\n'
    body += "Content-Type: application/zip\r\n\r\n"
    body  = body.encode() + zip_bytes + f"\r\n--{boundary}\r\n".encode()
    body += b'Content-Disposition: form-data; name="overwrite"\r\n\r\ntrue\r\n'
    body += f"--{boundary}--\r\n".encode()

    req = urllib.request.Request(
        f"{SUPERSET_URL}/api/v1/dashboard/import/",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "X-CSRFToken":   csrf,
            "Referer":       SUPERSET_URL,
            "Content-Type":  f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            print(f"  Imported {zip_path.name}: {result.get('message', 'OK')}")
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode(errors="replace")
        print(f"  ERROR importing {zip_path.name}: HTTP {exc.code} — {body_text[:200]}")


def main():
    wait_for_superset()

    zips = sorted(DASHBOARD_DIR.glob("*.zip"))
    if not zips:
        print(f"No ZIP files found in {DASHBOARD_DIR}")
        print("Run `python scripts/setup_superset.py` first to generate them.")
        sys.exit(1)

    print(f"\nImporting {len(zips)} dashboard(s) from {DASHBOARD_DIR} ...")
    for z in zips:
        print(f"  {z.name} ...")
        import_zip(z)

    print(f"\nDone. View dashboards at {SUPERSET_URL}/dashboard/list")


if __name__ == "__main__":
    main()
