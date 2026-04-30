"""
ShopFlow SAP OData API simulation (Flask).
Exposes vendor, purchase-order, and cost-center data with
timestamp-cursor incremental filtering.

Endpoints:
    GET /health
    GET /api/vendors?updated_after=<ISO8601>
    GET /api/purchase-orders?updated_after=<ISO8601>
    GET /api/cost-centers?updated_after=<ISO8601>
"""

import os
from datetime import datetime, timezone

from flask import Flask, jsonify, request

from data_generator import generate_cost_centers, generate_purchase_orders, generate_vendors

app = Flask(__name__)

# ── Seed in-memory store at startup ──────────────────────────────────────────
_vendors = generate_vendors(50)
_purchase_orders = generate_purchase_orders(300, [v["vendor_id"] for v in _vendors])
_cost_centers = generate_cost_centers(30)

print(f"[sap-api] Loaded {len(_vendors)} vendors, "
      f"{len(_purchase_orders)} purchase orders, "
      f"{len(_cost_centers)} cost centers.")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_cursor(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _filter_by_cursor(records: list[dict], cursor: datetime | None) -> list[dict]:
    if cursor is None:
        return records
    return [
        r for r in records
        if datetime.fromisoformat(r["updated_at"].replace("Z", "+00:00")) > cursor
    ]


def _paginate(records: list[dict]) -> dict:
    page = max(1, request.args.get("page", 1, type=int))
    page_size = min(200, request.args.get("page_size", 100, type=int))
    start = (page - 1) * page_size
    end = start + page_size
    slice_ = records[start:end]
    return {
        "count": len(records),
        "page": page,
        "page_size": page_size,
        "next_page": page + 1 if end < len(records) else None,
        "results": slice_,
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "sap-api",
        "records": {
            "vendors": len(_vendors),
            "purchase_orders": len(_purchase_orders),
            "cost_centers": len(_cost_centers),
        },
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    })


@app.get("/api/vendors")
def get_vendors():
    cursor = _parse_cursor(request.args.get("updated_after"))
    filtered = _filter_by_cursor(_vendors, cursor)
    # Sort by updated_at ascending for stable cursor pagination
    filtered.sort(key=lambda r: r["updated_at"])
    return jsonify(_paginate(filtered))


@app.get("/api/purchase-orders")
def get_purchase_orders():
    cursor = _parse_cursor(request.args.get("updated_after"))
    filtered = _filter_by_cursor(_purchase_orders, cursor)
    filtered.sort(key=lambda r: r["updated_at"])
    return jsonify(_paginate(filtered))


@app.get("/api/cost-centers")
def get_cost_centers():
    cursor = _parse_cursor(request.args.get("updated_after"))
    filtered = _filter_by_cursor(_cost_centers, cursor)
    filtered.sort(key=lambda r: r["updated_at"])
    return jsonify(_paginate(filtered))


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
