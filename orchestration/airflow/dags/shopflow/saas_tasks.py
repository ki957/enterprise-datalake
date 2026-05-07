"""SaaS PostgreSQL → ClickHouse raw.* extraction task."""

import io
import os

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
PG_DB   = os.getenv("POSTGRES_DB", "airflow")

CH_URL      = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
CH_USER     = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")
CH_AUTH     = (CH_USER, CH_PASSWORD)

_CHUNK_SIZE = 10_000


def extract_saas_to_clickhouse(**ctx):
    """
    Extract SaaS data from PostgreSQL → ClickHouse raw.* tables.

    Runs in parallel with the Airbyte wait step. Uses the ClickHouse HTTP
    API INSERT with TabSeparated format in 10K-row chunks so memory stays flat.
    """
    import psycopg2
    import requests

    pg     = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                               password=PG_PASS, dbname=PG_DB)
    pg_cur = pg.cursor()

    def ch_exec(sql: str) -> None:
        r = requests.post(CH_URL, data=sql, auth=CH_AUTH, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"ClickHouse error: {r.text[:400]}")

    def ch_insert_tsv(table: str, data: str) -> None:
        r = requests.post(
            CH_URL,
            params={"query": f"INSERT INTO {table} FORMAT TabSeparated"},
            data=data.encode("utf-8"),
            auth=CH_AUTH,
            timeout=120,
        )
        if r.status_code != 200:
            raise RuntimeError(f"ClickHouse INSERT error on {table}: {r.text[:400]}")

    # ── saas_users ────────────────────────────────────────────────────────────
    ch_exec("""
        CREATE TABLE IF NOT EXISTS raw.saas_users (
            id          Int32,
            email       String,
            name        String,
            company     String,
            country     String,
            plan        String,
            mrr         Float64,
            status      String,
            created_at  DateTime,
            updated_at  DateTime
        ) ENGINE = ReplacingMergeTree()
        ORDER BY id
    """)
    ch_exec("TRUNCATE TABLE raw.saas_users")
    pg_cur.execute(
        "SELECT id, email, name, company, country, plan, mrr, status, created_at, updated_at "
        "FROM saas_users ORDER BY id"
    )
    while True:
        rows = pg_cur.fetchmany(_CHUNK_SIZE)
        if not rows:
            break
        buf = io.StringIO()
        for r in rows:
            id_, email, name, company, country, plan, mrr, status, ca, ua = r
            buf.write(f"{id_}\t{email}\t{name or ''}\t{company or ''}\t"
                      f"{country or ''}\t{plan or ''}\t{mrr or 0}\t{status or ''}\t"
                      f"{ca.strftime('%Y-%m-%d %H:%M:%S') if ca else '1970-01-01 00:00:00'}\t"
                      f"{ua.strftime('%Y-%m-%d %H:%M:%S') if ua else '1970-01-01 00:00:00'}\n")
        ch_insert_tsv("raw.saas_users", buf.getvalue())
    print("  saas_users: done")

    # ── saas_events ───────────────────────────────────────────────────────────
    ch_exec("""
        CREATE TABLE IF NOT EXISTS raw.saas_events (
            id          Int32,
            user_id     Int32,
            event_type  String,
            page        String,
            properties  String,
            occurred_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (occurred_at, id)
    """)
    ch_exec("TRUNCATE TABLE raw.saas_events")
    pg_cur.execute(
        "SELECT id, user_id, event_type, page, properties::text, occurred_at "
        "FROM saas_events ORDER BY id"
    )
    total_events = 0
    while True:
        rows = pg_cur.fetchmany(_CHUNK_SIZE)
        if not rows:
            break
        buf = io.StringIO()
        for r in rows:
            id_, uid, etype, page, props, occ = r
            buf.write(f"{id_}\t{uid or 0}\t{etype or ''}\t{page or ''}\t"
                      f"{(props or '{}').replace(chr(9), ' ')}\t"
                      f"{occ.strftime('%Y-%m-%d %H:%M:%S') if occ else '1970-01-01 00:00:00'}\n")
        ch_insert_tsv("raw.saas_events", buf.getvalue())
        total_events += len(rows)
        print(f"  saas_events: {total_events:,}", end="\r", flush=True)
    print(f"  saas_events: {total_events:,}  done")

    # ── saas_subscriptions ────────────────────────────────────────────────────
    ch_exec("""
        CREATE TABLE IF NOT EXISTS raw.saas_subscriptions (
            id            Int32,
            user_id       Int32,
            plan          String,
            amount        Float64,
            billing_cycle String,
            started_at    DateTime,
            expires_at    DateTime,
            status        String,
            created_at    DateTime
        ) ENGINE = ReplacingMergeTree()
        ORDER BY id
    """)
    ch_exec("TRUNCATE TABLE raw.saas_subscriptions")
    pg_cur.execute(
        "SELECT id, user_id, plan, amount, billing_cycle, started_at, expires_at, status, created_at "
        "FROM saas_subscriptions ORDER BY id"
    )
    while True:
        rows = pg_cur.fetchmany(_CHUNK_SIZE)
        if not rows:
            break
        buf = io.StringIO()
        for r in rows:
            id_, uid, plan, amount, billing, started, expires, status, ca = r
            buf.write(f"{id_}\t{uid or 0}\t{plan or ''}\t{amount or 0}\t{billing or ''}\t"
                      f"{started.strftime('%Y-%m-%d %H:%M:%S') if started else '1970-01-01 00:00:00'}\t"
                      f"{expires.strftime('%Y-%m-%d %H:%M:%S') if expires else '1970-01-01 00:00:00'}\t"
                      f"{status or ''}\t"
                      f"{ca.strftime('%Y-%m-%d %H:%M:%S') if ca else '1970-01-01 00:00:00'}\n")
        ch_insert_tsv("raw.saas_subscriptions", buf.getvalue())
    print("  saas_subscriptions: done")

    pg_cur.close()
    pg.close()
    print("SaaS extract to ClickHouse raw.* complete.")
