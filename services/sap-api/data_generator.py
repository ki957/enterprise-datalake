"""
SAP OData synthetic data generator.
Produces: 50 vendors, 300 purchase orders, 30 cost centers.
Called once at Flask app startup to seed in-memory store.
"""

import random
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker()
random.seed(7)
Faker.seed(7)

VENDOR_CATEGORIES = ["Raw Materials", "Logistics", "IT Services", "Facilities", "Marketing", "Consulting"]
PO_STATUSES = ["OPEN", "APPROVED", "RECEIVED", "CLOSED", "CANCELLED"]
CC_TYPES = ["COST", "PROFIT", "INVESTMENT"]
COUNTRIES = ["DE", "US", "IN", "GB", "FR", "JP", "SG", "AU", "BR", "CA"]


def _rand_dt(days_ago_max: int = 365, days_ago_min: int = 0) -> datetime:
    delta = timedelta(
        seconds=random.randint(
            int(timedelta(days=days_ago_min).total_seconds()),
            int(timedelta(days=days_ago_max).total_seconds()),
        )
    )
    return datetime.now(tz=timezone.utc) - delta


def generate_vendors(n: int = 50) -> list[dict]:
    vendors = []
    for i in range(1, n + 1):
        created = _rand_dt(730, 60)
        vendors.append({
            "vendor_id": f"V{i:04d}",
            "name": fake.company(),
            "contact_name": fake.name(),
            "email": fake.company_email(),
            "phone": fake.phone_number()[:30],
            "country": random.choice(COUNTRIES),
            "city": fake.city(),
            "category": random.choice(VENDOR_CATEGORIES),
            "payment_terms": random.choice(["NET30", "NET60", "NET90", "IMMEDIATE"]),
            "is_active": random.random() > 0.1,
            "created_at": created.isoformat(),
            "updated_at": (created + timedelta(days=random.randint(0, 30))).isoformat(),
        })
    return vendors


def generate_purchase_orders(n: int = 300, vendor_ids: list[str] | None = None) -> list[dict]:
    if vendor_ids is None:
        vendor_ids = [f"V{i:04d}" for i in range(1, 51)]
    pos = []
    for i in range(1, n + 1):
        created = _rand_dt(365, 0)
        pos.append({
            "po_id": f"PO{i:06d}",
            "vendor_id": random.choice(vendor_ids),
            "amount": round(random.uniform(500.0, 250000.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "INR"]),
            "status": random.choice(PO_STATUSES),
            "line_items": random.randint(1, 20),
            "description": fake.bs()[:200],
            "requested_by": fake.name(),
            "po_date": created.isoformat(),
            "delivery_date": (created + timedelta(days=random.randint(7, 90))).isoformat(),
            "created_at": created.isoformat(),
            "updated_at": (created + timedelta(days=random.randint(0, 10))).isoformat(),
        })
    return pos


def generate_cost_centers(n: int = 30) -> list[dict]:
    ccs = []
    for i in range(1, n + 1):
        created = _rand_dt(900, 120)
        ccs.append({
            "cost_center_id": f"CC{i:04d}",
            "name": f"{fake.job()} Center",
            "type": random.choice(CC_TYPES),
            "department": fake.bs().split()[0].capitalize(),
            "manager": fake.name(),
            "budget": round(random.uniform(50000.0, 5000000.0), 2),
            "currency": "USD",
            "country": random.choice(COUNTRIES),
            "is_active": random.random() > 0.05,
            "created_at": created.isoformat(),
            "updated_at": (created + timedelta(days=random.randint(0, 60))).isoformat(),
        })
    return ccs
