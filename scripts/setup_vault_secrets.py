#!/usr/bin/env python3
"""
Vault Secrets Bootstrap
========================
Writes all pipeline credentials into HashiCorp Vault KV (v2) so that:
  - Airflow reads connections / variables from Vault (secrets backend)
  - Future services can fetch secrets at runtime instead of using .env

Vault address:  http://localhost:8200
Root token:     root  (dev mode — used only during bootstrap)

KV paths written:
  secret/airflow/connections/clickhouse_default  — Airflow ClickHouse connection
  secret/airflow/connections/minio_default       — Airflow MinIO connection (S3)
  secret/airflow/connections/postgres_default    — Airflow PostgreSQL connection
  secret/airflow/variables/clickhouse_password   — ClickHouse password (plain)
  secret/airflow/variables/minio_secret_key      — MinIO secret key (plain)
  secret/datalake/clickhouse                     — ClickHouse credentials for scripts
  secret/datalake/minio                          — MinIO credentials for scripts
  secret/datalake/postgres                       — PostgreSQL credentials for scripts

AppRole created:
  Role name:  airflow
  Policy:     airflow-reader  (read-only on secret/data/airflow/* and secret/data/datalake/*)
  role_id / secret_id printed at end — copy into .env as VAULT_APPROLE_ROLE_ID / VAULT_APPROLE_SECRET_ID

Audit logging:
  File audit device enabled at /vault/logs/audit.log inside the container.

Usage:
    python scripts/setup_vault_secrets.py
    python scripts/setup_vault_secrets.py --vault-addr http://localhost:8200 --token root
"""

import argparse
import json
import sys
import urllib.error
import urllib.request


def vault_write(addr: str, token: str, path: str, data: dict) -> None:
    """Write a KV v2 secret to Vault."""
    url = f"{addr}/v1/{path}"
    payload = json.dumps({"data": data}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={"X-Vault-Token": token, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        status = resp.status
    if status not in (200, 204):
        raise RuntimeError(f"Vault write to {path} returned HTTP {status}")
    print(f"  ✓  {path}")


def vault_enable_kv(addr: str, token: str, mount: str = "secret") -> None:
    """Enable KV v2 secrets engine at <mount>/ (idempotent — ignores 400 if already mounted)."""
    url = f"{addr}/v1/sys/mounts/{mount}"
    payload = json.dumps({"type": "kv", "options": {"version": "2"}}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={"X-Vault-Token": token, "Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req)
        print(f"  KV v2 engine enabled at {mount}/")
    except urllib.error.HTTPError as exc:
        if exc.code == 400:
            print(f"  KV v2 engine already mounted at {mount}/ (skipping)")
        else:
            raise


# ── AppRole auth ──────────────────────────────────────────────────────────────

def vault_enable_approle(addr: str, token: str) -> None:
    """Enable the AppRole auth method (idempotent)."""
    url = f"{addr}/v1/sys/auth/approle"
    payload = json.dumps({"type": "approle"}).encode()
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={"X-Vault-Token": token, "Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req)
        print("  AppRole auth method enabled.")
    except urllib.error.HTTPError as exc:
        if exc.code == 400:
            print("  AppRole auth method already enabled (skipping).")
        else:
            raise


def vault_create_policy(addr: str, token: str) -> None:
    """Create the airflow-reader policy: read-only on airflow/* and datalake/* paths."""
    policy_hcl = (
        'path "secret/data/airflow/*" { capabilities = ["read", "list"] }\n'
        'path "secret/data/datalake/*" { capabilities = ["read", "list"] }\n'
        'path "secret/metadata/airflow/*" { capabilities = ["read", "list"] }\n'
        'path "secret/metadata/datalake/*" { capabilities = ["read", "list"] }\n'
    )
    url = f"{addr}/v1/sys/policies/acl/airflow-reader"
    payload = json.dumps({"policy": policy_hcl}).encode()
    req = urllib.request.Request(
        url, data=payload, method="PUT",
        headers={"X-Vault-Token": token, "Content-Type": "application/json"},
    )
    urllib.request.urlopen(req)
    print("  Policy 'airflow-reader' created (read-only on airflow/* and datalake/*).")


def vault_create_approle_role(addr: str, token: str) -> None:
    """Create the 'airflow' AppRole bound to airflow-reader policy."""
    url = f"{addr}/v1/auth/approle/role/airflow"
    payload = json.dumps({
        "policies": ["airflow-reader"],
        "token_ttl": "1h",
        "token_max_ttl": "4h",
        "token_num_uses": 0,    # unlimited
        "bind_secret_id": True,
    }).encode()
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={"X-Vault-Token": token, "Content-Type": "application/json"},
    )
    urllib.request.urlopen(req)
    print("  AppRole 'airflow' created (token_ttl=1h, max=4h, policy=airflow-reader).")


def vault_get_role_id(addr: str, token: str) -> str:
    url = f"{addr}/v1/auth/approle/role/airflow/role-id"
    req = urllib.request.Request(url, headers={"X-Vault-Token": token})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["data"]["role_id"]


def vault_get_secret_id(addr: str, token: str) -> str:
    url = f"{addr}/v1/auth/approle/role/airflow/secret-id"
    payload = b"{}"
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={"X-Vault-Token": token, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["data"]["secret_id"]


# ── Audit logging ─────────────────────────────────────────────────────────────

def vault_enable_audit(addr: str, token: str) -> None:
    """Enable file audit device at /vault/logs/audit.log (idempotent)."""
    url = f"{addr}/v1/sys/audit/file"
    payload = json.dumps({
        "type": "file",
        "description": "File audit log for all Vault operations",
        "options": {"file_path": "/vault/logs/audit.log"},
    }).encode()
    req = urllib.request.Request(
        url, data=payload, method="PUT",
        headers={"X-Vault-Token": token, "Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req)
        print("  Audit logging enabled → /vault/logs/audit.log (inside container).")
    except urllib.error.HTTPError as exc:
        if exc.code == 400:
            print("  Audit device 'file' already enabled (skipping).")
        else:
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap Vault secrets for the data lake")
    parser.add_argument("--vault-addr", default="http://localhost:8200", help="Vault address")
    parser.add_argument("--token", default="root", help="Vault root token (bootstrap only)")
    args = parser.parse_args()

    addr  = args.vault_addr.rstrip("/")
    token = args.token

    # ── Health check ──────────────────────────────────────────────────────────
    try:
        req = urllib.request.Request(
            f"{addr}/v1/sys/health",
            headers={"X-Vault-Token": token},
        )
        with urllib.request.urlopen(req) as resp:
            health = json.loads(resp.read())
        if not health.get("initialized"):
            print("ERROR: Vault is not initialized. Run 'make security' first.", file=sys.stderr)
            sys.exit(1)
        print(f"Vault is up ({addr}), version={health.get('version', '?')}")
    except Exception as exc:
        print(f"ERROR: Cannot reach Vault at {addr}: {exc}", file=sys.stderr)
        sys.exit(1)

    vault_enable_kv(addr, token, mount="secret")

    print("\nWriting datalake secrets...")

    # ── ClickHouse ─────────────────────────────────────────────────────────────
    vault_write(addr, token, "secret/data/datalake/clickhouse", {
        "host":     "clickhouse",
        "port":     "8123",
        "user":     "default",
        "password": "Click@2024",
        "database": "gold",
    })

    # ── MinIO ──────────────────────────────────────────────────────────────────
    vault_write(addr, token, "secret/data/datalake/minio", {
        "endpoint":   "minio:9000",
        "access_key": "admin",
        "secret_key": "Minio@2024",
        "bucket_raw": "raw",
        "secure":     "false",
    })

    # ── PostgreSQL ─────────────────────────────────────────────────────────────
    vault_write(addr, token, "secret/data/datalake/postgres", {
        "host":     "postgres",
        "port":     "5432",
        "user":     "postgres",
        "password": "Postgres@2024",
        "database": "airflow",
    })

    # ── Airflow Connections (used by Vault secrets backend) ────────────────────
    # Airflow Vault backend expects conn_uri at:
    #   secret/airflow/connections/<conn_id>  →  { "conn_uri": "..." }
    vault_write(addr, token, "secret/data/airflow/connections/clickhouse_default", {
        "conn_uri": "http://default:Click%402024@clickhouse:8123/gold",
    })
    vault_write(addr, token, "secret/data/airflow/connections/minio_s3", {
        "conn_uri": "s3://admin:Minio%402024@minio:9000",
    })
    vault_write(addr, token, "secret/data/airflow/connections/postgres_default", {
        "conn_uri": "postgresql://postgres:Postgres%402024@postgres:5432/airflow",
    })

    # ── Airflow Variables (used by Vault secrets backend) ─────────────────────
    # Airflow Vault backend expects value at:
    #   secret/airflow/variables/<var_name>  →  { "value": "..." }
    vault_write(addr, token, "secret/data/airflow/variables/clickhouse_password", {
        "value": "Click@2024",
    })
    vault_write(addr, token, "secret/data/airflow/variables/minio_secret_key", {
        "value": "Minio@2024",
    })
    vault_write(addr, token, "secret/data/airflow/variables/postgres_password", {
        "value": "Postgres@2024",
    })

    print("\nAll secrets written to Vault.")

    # ── AppRole auth setup ────────────────────────────────────────────────────
    print("\nConfiguring AppRole auth (Airflow will use this instead of root token)...")
    vault_enable_approle(addr, token)
    vault_create_policy(addr, token)
    vault_create_approle_role(addr, token)
    role_id   = vault_get_role_id(addr, token)
    secret_id = vault_get_secret_id(addr, token)

    # ── Audit logging ─────────────────────────────────────────────────────────
    print("\nEnabling audit logging...")
    vault_enable_audit(addr, token)

    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  Vault bootstrap complete                                            ║
╠══════════════════════════════════════════════════════════════════════╣
║  Secrets written:   secret/data/datalake/{{clickhouse,minio,postgres}} ║
║  Airflow conns:     secret/data/airflow/connections/*               ║
║  Airflow vars:      secret/data/airflow/variables/*                 ║
║  AppRole:           airflow  (policy: airflow-reader)               ║
║  Audit log:         /vault/logs/audit.log (inside vault container)  ║
╠══════════════════════════════════════════════════════════════════════╣
║  ACTION REQUIRED — add these to your .env file:                     ║
║                                                                      ║
║  VAULT_APPROLE_ROLE_ID={role_id[:44]:<44}  ║
║  VAULT_APPROLE_SECRET_ID={secret_id[:42]:<42}  ║
╠══════════════════════════════════════════════════════════════════════╣
║  Then restart Airflow:                                               ║
║    make governance                                                   ║
╚══════════════════════════════════════════════════════════════════════╝
""")

    print("To verify secret access via AppRole (no root token):")
    print(f'  LOGIN=$(curl -sf -X POST {addr}/v1/auth/approle/login \\')
    print(f'    -d \'{{"role_id":"{role_id}","secret_id":"{secret_id}"}}\' | python3 -m json.tool)')
    print("  # Use the client_token from the response to read secrets.")


if __name__ == "__main__":
    main()
