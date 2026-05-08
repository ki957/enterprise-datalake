#!/usr/bin/env python3
"""
SaaS PostgreSQL synthetic data generator.
Produces: 10,000 users, 200,000 events, 10,000 subscriptions.

Tables: saas_users, saas_events, saas_subscriptions
DB: airflow (shared with Airflow metadata — saas tables live in public schema)

Usage:
    pip install faker psycopg2-binary
    python scripts/generate_saas_data.py [--host HOST] [--port PORT]
    python scripts/generate_saas_data.py --small   # 500/5000/500 for fast dev
"""

import argparse
import json
import random
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
from faker import Faker

fake = Faker()
random.seed(7)
Faker.seed(7)

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULTS = {
    "host":     "127.0.0.1",
    "port":     5432,
    "user":     "postgres",
    "password": "Postgres@2024",
    "database": "airflow",
}

N_USERS         = 10_000
N_EVENTS        = 200_000
N_SUBSCRIPTIONS = 10_000
CHUNK_SIZE      = 5_000

PLANS = ["free", "starter", "growth", "enterprise"]
PLAN_MRR = {
    "free":       0.00,
    "starter":    29.00,
    "growth":     99.00,
    "enterprise": 499.00,
}
PLAN_WEIGHTS = [30, 35, 25, 10]   # % distribution

STATUS_OPTIONS = ["active", "churned", "trial"]
STATUS_WEIGHTS = [65, 20, 15]

BILLING_CYCLES  = ["monthly", "annual"]
EVENT_TYPES     = [
    "page_view", "click", "signup", "login", "logout",
    "upgrade", "downgrade", "feature_used", "export", "invite_sent",
]
PAGES = [
    "/dashboard", "/settings", "/billing", "/reports",
    "/integrations", "/team", "/onboarding", "/pricing",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def rand_dt(start_days_ago: int = 730, end_days_ago: int = 0) -> datetime:
    start = datetime.now() - timedelta(days=start_days_ago)
    end   = datetime.now() - timedelta(days=end_days_ago)
    return start + (end - start) * random.random()


# ── Generators ────────────────────────────────────────────────────────────────

def user_rows(n: int):
    for i in range(n):
        plan   = random.choices(PLANS, weights=PLAN_WEIGHTS, k=1)[0]
        status = random.choices(STATUS_OPTIONS, weights=STATUS_WEIGHTS, k=1)[0]
        created = rand_dt(730, 0)
        yield {
            "email":      f"saas_user_{i+1}@{fake.domain_name()}",
            "name":       fake.name(),
            "company":    fake.company()[:255],
            "country":    fake.country_code(),
            "plan":       plan,
            "mrr":        PLAN_MRR[plan] if status == "active" else 0.00,
            "status":     status,
            "created_at": created,
            "updated_at": created + timedelta(days=random.randint(0, 60)),
        }


def event_rows(n: int, user_ids: list):
    # Weight recent events heavily (last 90 days) for realistic DAU analytics
    for _ in range(n):
        r = random.random()
        occurred = (
            rand_dt(90, 0)   if r < 0.60 else
            rand_dt(180, 90) if r < 0.85 else
            rand_dt(365, 180)
        )
        event_type = random.choices(
            EVENT_TYPES,
            weights=[30, 20, 5, 15, 8, 3, 2, 10, 4, 3],
            k=1,
        )[0]
        props = {"duration_ms": random.randint(50, 5000)}
        if event_type == "page_view":
            props["referrer"] = random.choice(["google", "direct", "email", "twitter"])
        elif event_type == "feature_used":
            props["feature"] = random.choice(["export", "filter", "dashboard", "api"])
        yield {
            "user_id":     random.choice(user_ids),
            "event_type":  event_type,
            "page":        random.choice(PAGES),
            "properties":  json.dumps(props),
            "occurred_at": occurred,
        }


def subscription_rows(n: int, user_ids: list):
    for i in range(n):
        plan    = random.choices(PLANS[1:], weights=[45, 35, 20], k=1)[0]  # no free subs
        billing = random.choices(BILLING_CYCLES, weights=[70, 30], k=1)[0]
        amount  = PLAN_MRR[plan] * (10.0 if billing == "annual" else 1.0)
        started = rand_dt(730, 0)
        expires = started + timedelta(days=365 if billing == "annual" else 30)
        status  = random.choices(["active", "cancelled", "expired"], weights=[65, 20, 15], k=1)[0]
        yield {
            "user_id":       user_ids[i % len(user_ids)],
            "plan":          plan,
            "amount":        round(amount, 2),
            "billing_cycle": billing,
            "started_at":    started,
            "expires_at":    expires,
            "status":        status,
            "created_at":    started,
        }


# ── Database helpers ──────────────────────────────────────────────────────────

def get_connection(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"],
        dbname=cfg["database"],
    )


def bulk_insert_chunked(conn, cursor, table: str, row_gen, total: int) -> int:
    cols     = None
    inserted = 0
    chunk    = []

    for row in row_gen:
        if cols is None:
            cols = list(row.keys())
            ph   = ", ".join(["%s"] * len(cols))
            sql  = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph})"
        chunk.append(tuple(row[c] for c in cols))
        if len(chunk) >= CHUNK_SIZE:
            psycopg2.extras.execute_batch(cursor, sql, chunk, page_size=CHUNK_SIZE)
            conn.commit()
            inserted += len(chunk)
            print(f"  {table}: {inserted:,} / {total:,}", end="\r", flush=True)
            chunk = []

    if chunk:
        psycopg2.extras.execute_batch(cursor, sql, chunk, page_size=CHUNK_SIZE)
        conn.commit()
        inserted += len(chunk)

    print(f"  {table}: {inserted:,} / {total:,}  ✓")
    return inserted


def fetch_ids(cursor, table: str, pk: str) -> list:
    cursor.execute(f"SELECT {pk} FROM {table} ORDER BY {pk}")
    return [row[0] for row in cursor.fetchall()]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate SaaS PostgreSQL synthetic data")
    parser.add_argument("--host",     default=DEFAULTS["host"])
    parser.add_argument("--port",     type=int, default=DEFAULTS["port"])
    parser.add_argument("--user",     default=DEFAULTS["user"])
    parser.add_argument("--password", default=DEFAULTS["password"])
    parser.add_argument("--database", default=DEFAULTS["database"])
    parser.add_argument("--small",    action="store_true",
                        help="Generate small dataset (500 users / 5000 events / 500 subs)")
    args = parser.parse_args()

    n_users = 500       if args.small else N_USERS
    n_evts  = 5_000     if args.small else N_EVENTS
    n_subs  = 500       if args.small else N_SUBSCRIPTIONS

    cfg = {k: getattr(args, k, DEFAULTS[k]) for k in DEFAULTS}

    print(f"Connecting to PostgreSQL at {cfg['host']}:{cfg['port']} / {cfg['database']} ...")
    conn   = get_connection(cfg)
    cursor = conn.cursor()

    print("Truncating existing data ...")
    cursor.execute("TRUNCATE TABLE saas_subscriptions, saas_events, saas_users RESTART IDENTITY CASCADE")
    conn.commit()

    print(f"Inserting {n_users:,} users ...")
    bulk_insert_chunked(conn, cursor, "saas_users", user_rows(n_users), n_users)
    user_ids = fetch_ids(cursor, "saas_users", "id")
    print(f"  IDs {user_ids[0]} – {user_ids[-1]}")

    print(f"Inserting {n_evts:,} events ...")
    bulk_insert_chunked(conn, cursor, "saas_events", event_rows(n_evts, user_ids), n_evts)

    print(f"Inserting {n_subs:,} subscriptions ...")
    bulk_insert_chunked(conn, cursor, "saas_subscriptions", subscription_rows(n_subs, user_ids), n_subs)

    cursor.close()
    conn.close()
    print(f"\nDone. {n_users:,} users / {n_evts:,} events / {n_subs:,} subscriptions loaded.")


if __name__ == "__main__":
    main()
