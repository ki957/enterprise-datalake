#!/usr/bin/env python3
"""
Superset Setup Script
======================
Programmatically configures Superset against the running ClickHouse gold layer:
  1. Creates the ClickHouse database connection
  2. Registers datasets for every gold.* table
  3. Creates a set of charts and a "ShopFlow & SaaS — Business Intelligence" dashboard
  4. Exports the dashboard to infrastructure/superset/dashboards/ for git

Run AFTER `make visualization` (Superset must be up at localhost:8088):
    python scripts/setup_superset.py

The exported ZIP can be re-imported on any fresh Superset instance via:
    python scripts/import_superset_dashboards.py
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
import zipfile
from pathlib import Path

SUPERSET_URL   = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER  = os.getenv("SUPERSET_USER", "admin")
SUPERSET_PASS  = os.getenv("SUPERSET_PASSWORD", "Superset@2024")

CH_HOST     = os.getenv("CLICKHOUSE_HOST_SUPERSET", "clickhouse")
CH_PORT     = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER     = os.getenv("CLICKHOUSE_DEFAULT_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_DEFAULT_PASSWORD", "Click@2024")

EXPORT_DIR = Path(__file__).parent.parent / "infrastructure" / "superset" / "dashboards"

GOLD_TABLES = [
    "dim_customers", "dim_products", "dim_vendors",
    "fct_orders", "fct_procurement", "fct_reviews",
    "dim_users", "fct_daily_active_users", "fct_event_funnel", "fct_mrr",
    "unified_customers",
]


# ── HTTP helpers ──────────────────────────────────────────────────────────────

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


def _api(method: str, path: str, data=None, *, binary=False):
    token = _get_token()
    url   = f"{SUPERSET_URL}{path}"
    body  = json.dumps(data).encode() if data is not None else None
    headers = {"Authorization": f"Bearer {token}"}
    if not binary:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw) if not binary else raw
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode(errors="replace")
        if exc.code == 422 and "already exists" in body_text.lower():
            return {"_exists": True}
        if exc.code == 404:
            return {"_not_found": True}
        raise RuntimeError(f"HTTP {exc.code} {method} {path}: {body_text[:300]}") from exc


def _get_csrf_token() -> str:
    result = _api("GET", "/api/v1/security/csrf_token/")
    return result.get("result", "")


# ── Setup steps ───────────────────────────────────────────────────────────────

def wait_for_superset(timeout=120):
    print(f"Waiting for Superset at {SUPERSET_URL} ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{SUPERSET_URL}/health", timeout=5)
            print("  Superset is ready.")
            return
        except Exception:
            time.sleep(4)
    print("ERROR: Superset did not become ready. Is `make visualization` running?", file=sys.stderr)
    sys.exit(1)


def create_clickhouse_database() -> int:
    """Register ClickHouse as a Superset database connection. Returns DB id."""
    print("\n[1/4] Creating ClickHouse database connection ...")
    sqlalchemy_uri = (
        f"clickhousedb+http://{CH_USER}:{CH_PASSWORD}@{CH_HOST}:{CH_PORT}/gold"
    )
    result = _api("POST", "/api/v1/database/", data={
        "database_name": "ClickHouse — Data Lake",
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "extra": json.dumps({"allows_virtual_table_explore": True}),
    })
    if result.get("_exists"):
        # Find existing
        dbs = _api("GET", "/api/v1/database/?q=(filters:!((col:database_name,opr:DatabaseSearch,val:'ClickHouse')))")
        for db in dbs.get("result", []):
            if "ClickHouse" in db.get("database_name", ""):
                print(f"  Database already exists (id={db['id']})")
                return db["id"]
    db_id = result.get("id", result.get("result", {}).get("id"))
    print(f"  Created ClickHouse database (id={db_id})")
    return db_id


def create_datasets(db_id: int) -> dict[str, int]:
    """Register gold.* tables as Superset datasets. Returns {table_name: dataset_id}."""
    print(f"\n[2/4] Registering {len(GOLD_TABLES)} gold datasets ...")
    dataset_ids = {}
    for table in GOLD_TABLES:
        result = _api("POST", "/api/v1/dataset/", data={
            "database": db_id,
            "schema": "gold",
            "table_name": table,
        })
        if result.get("_exists"):
            print(f"  {table:35s}  (already exists)")
            # Try to find the existing ID
            ds = _api("GET", f"/api/v1/dataset/?q=(filters:!((col:table_name,opr:DatasetIsNullOrEmpty,val:'{table}')))")
            for d in ds.get("result", []):
                if d.get("table_name") == table:
                    dataset_ids[table] = d["id"]
                    break
        else:
            ds_id = result.get("id", result.get("result", {}).get("id"))
            dataset_ids[table] = ds_id
            print(f"  {table:35s}  id={ds_id}")
    return dataset_ids


def create_charts(dataset_ids: dict[str, int]) -> list[int]:
    """Create key charts. Returns list of chart ids."""
    print("\n[3/4] Creating charts ...")
    chart_ids = []

    charts_spec = [
        {
            "slice_name": "Monthly Revenue",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": dataset_ids.get("fct_orders"),
            "params": json.dumps({
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "revenue"}, "aggregate": "SUM", "label": "Revenue"}],
                "groupby": [],
                "time_grain_sqla": "P1M",
                "x_axis": "order_date",
                "row_limit": 500,
            }),
        },
        {
            "slice_name": "Revenue by Customer Segment",
            "viz_type": "pie",
            "datasource_id": dataset_ids.get("fct_orders"),
            "params": json.dumps({
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "revenue"}, "aggregate": "SUM", "label": "Revenue"}],
                "groupby": ["customer_segment"],
                "row_limit": 10,
            }),
        },
        {
            "slice_name": "MRR by Plan",
            "viz_type": "pie",
            "datasource_id": dataset_ids.get("fct_mrr"),
            "params": json.dumps({
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_mrr"}, "aggregate": "SUM", "label": "MRR"}],
                "groupby": ["plan"],
                "row_limit": 10,
            }),
        },
        {
            "slice_name": "Customer Type Breakdown",
            "viz_type": "echarts_bar",
            "datasource_id": dataset_ids.get("unified_customers"),
            "params": json.dumps({
                "metrics": [{"expressionType": "COUNT", "label": "Customers"}],
                "groupby": ["customer_type"],
                "row_limit": 10,
            }),
        },
        {
            "slice_name": "Top Cross-Domain Customers",
            "viz_type": "table",
            "datasource_id": dataset_ids.get("unified_customers"),
            "params": json.dumps({
                "metrics": [],
                "groupby": ["email", "full_name", "saas_plan", "saas_mrr",
                            "total_shopflow_revenue", "customer_type"],
                "filters": [{"col": "customer_type", "op": "==", "val": "cross_domain"}],
                "row_limit": 25,
                "order_desc": True,
            }),
        },
    ]

    for spec in charts_spec:
        if spec.get("datasource_id") is None:
            print(f"  SKIP {spec['slice_name']} — dataset not found")
            continue
        result = _api("POST", "/api/v1/chart/", data={
            "slice_name":    spec["slice_name"],
            "viz_type":      spec["viz_type"],
            "datasource_id": spec["datasource_id"],
            "datasource_type": "table",
            "params":        spec["params"],
        })
        cid = result.get("id", result.get("result", {}).get("id"))
        if cid:
            chart_ids.append(cid)
            print(f"  {spec['slice_name']:45s}  id={cid}")
        else:
            print(f"  {spec['slice_name']:45s}  (skipped: {result})")

    return chart_ids


def create_dashboard(chart_ids: list[int]) -> int:
    """Create the Business Intelligence dashboard."""
    print("\n[4/4] Creating dashboard ...")
    result = _api("POST", "/api/v1/dashboard/", data={
        "dashboard_title": "ShopFlow & SaaS — Business Intelligence",
        "published": True,
        "position_data": json.dumps({}),
        "metadata": json.dumps({
            "chart_configuration": {},
            "color_scheme": "supersetColors",
        }),
        "charts": chart_ids,
    })
    if result.get("_exists"):
        print("  Dashboard already exists.")
        return -1
    dash_id = result.get("id", result.get("result", {}).get("id"))
    print(f"  Dashboard created (id={dash_id})")
    return dash_id


def export_dashboard(dash_id: int) -> None:
    """Export dashboard as ZIP and save to infrastructure/superset/dashboards/."""
    if dash_id < 0:
        print("\n[export] Dashboard ID unknown — skipping export.")
        return
    print(f"\n[export] Exporting dashboard {dash_id} ...")
    raw = _api("GET", f"/api/v1/dashboard/export/?q=[{dash_id}]", binary=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = EXPORT_DIR / "business_intelligence.zip"
    out.write_bytes(raw)
    print(f"  Saved to {out}")

    # Also list ZIP contents for transparency
    with zipfile.ZipFile(out) as zf:
        print("  Contents:")
        for name in zf.namelist():
            print(f"    {name}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    wait_for_superset()

    db_id      = create_clickhouse_database()
    dataset_ids = create_datasets(db_id)
    chart_ids  = create_charts(dataset_ids)
    dash_id    = create_dashboard(chart_ids)
    export_dashboard(dash_id)

    print(f"""
Setup complete.
  Dashboard: {SUPERSET_URL}/dashboard/list
  Exported:  {EXPORT_DIR}/business_intelligence.zip

To re-import on a fresh Superset:
  python scripts/import_superset_dashboards.py
""")


if __name__ == "__main__":
    main()
