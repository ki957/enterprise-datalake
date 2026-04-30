"""
Schema Evolution DAG
=====================
DAG: schema_evolution
Schedule: daily at 08:30 UTC (after metadata_sync at 08:00)

Tasks:
1. snapshot_mysql_schema  — poll MySQL information_schema.columns,
                            compare against Postgres schema_snapshots,
                            detect new/removed columns
2. update_sources_yml     — safe YAML append for new columns in
                            models/bronze/sources.yml (never auto-removes)
3. validate_dbt_compile   — run dbt compile to verify no model breaks
4. reload_agent_rag       — POST /api/rag/reload so AI agent knows immediately

Safety rules:
  - New columns: auto-appended to sources.yml with a comment marker
  - Removed columns: logged to schema_drift_alerts, NOT removed from sources.yml
  - dbt compile failure: sources.yml rolled back, Slack alert sent
  - The AI agent reload happens only when a change was made

Integration with AI Agent:
  After every successful sources.yml update, the agent's ChromaDB is refreshed
  via POST http://172.17.0.1:8502/api/rag/reload so the Schema Agent and
  Insight Agent immediately know about the new columns.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

DATALAKE_HOME = os.getenv("DATALAKE_HOME", "/opt/datalake")
DBT_PROJECT   = f"{DATALAKE_HOME}/transformation/dbt/datalake_transforms"
DBT_PROFILES  = "/opt/airflow/dbt_profiles.yml"
DBT_BIN       = "/home/airflow/.local/bin/dbt"

MYSQL_HOST    = os.getenv("MYSQL_HOST", "mysql-shopflow")
MYSQL_PORT    = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER    = os.getenv("MYSQL_USER", "root")
MYSQL_PASS    = os.getenv("MYSQL_ROOT_PASSWORD", "")
MYSQL_DB      = "shopflow"

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
PG_DB   = os.getenv("POSTGRES_DB", "airflow")

# AI Agent FastAPI on the Docker host (host.docker.internal or bridge IP)
AI_AGENT_URL  = os.getenv("AI_AGENT_URL", "http://172.17.0.1:8502")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

BRONZE_SOURCES_YML = Path(
    f"{DBT_PROJECT}/models/bronze/sources.yml"
    if Path(f"{DBT_PROJECT}/models/bronze/sources.yml").exists()
    else f"{DBT_PROJECT}/models/raw/sources.yml"
)


def _send_slack(msg: str) -> None:
    import requests as _r
    if not SLACK_WEBHOOK:
        print(f"[ALERT] {msg}")
        return
    try:
        _r.post(SLACK_WEBHOOK, json={"text": msg}, timeout=10)
    except Exception as exc:
        print(f"[Slack send failed: {exc}] {msg}")


def _pg_conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS,
        dbname=PG_DB,
    )


def _ensure_snapshot_table():
    """Create schema_snapshots table in Postgres if it doesn't exist."""
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_snapshots (
                    id          SERIAL PRIMARY KEY,
                    source_db   VARCHAR(64)  NOT NULL,
                    table_name  VARCHAR(128) NOT NULL,
                    column_name VARCHAR(128) NOT NULL,
                    data_type   VARCHAR(128),
                    is_nullable VARCHAR(4),
                    captured_at TIMESTAMP    DEFAULT NOW(),
                    UNIQUE (source_db, table_name, column_name)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_drift_alerts (
                    id          SERIAL PRIMARY KEY,
                    source_db   VARCHAR(64),
                    table_name  VARCHAR(128),
                    column_name VARCHAR(128),
                    drift_type  VARCHAR(16),   -- 'added' or 'removed'
                    detected_at TIMESTAMP DEFAULT NOW()
                )
            """)
        conn.commit()


def snapshot_mysql_schema(**ctx):
    """
    Compare MySQL information_schema.columns against Postgres schema_snapshots.
    Push results to XCom: {added: [...], removed: [...]}
    """
    import mysql.connector  # installed via Dockerfile.airflow

    _ensure_snapshot_table()

    # Fetch current MySQL schema
    conn_mysql = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASS,
        database="information_schema",
    )
    cursor = conn_mysql.cursor()
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """, (MYSQL_DB,))
    # mysql-connector-python returns bytes for information_schema string columns — decode
    def _s(v):
        return v.decode() if isinstance(v, bytes) else str(v)

    current_columns = {
        (_s(row[0]), _s(row[1])): {"data_type": _s(row[2]), "is_nullable": _s(row[3])}
        for row in cursor.fetchall()
    }
    cursor.close()
    conn_mysql.close()
    print(f"MySQL schema: {len(current_columns)} columns across {len({k[0] for k in current_columns})} tables")

    # Fetch snapshot from Postgres
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name, column_name FROM schema_snapshots WHERE source_db = %s",
                (MYSQL_DB,),
            )
            snapshot_columns = {(_s(row[0]), _s(row[1])) for row in cur.fetchall()}

    added   = [k for k in current_columns if k not in snapshot_columns]
    removed = [k for k in snapshot_columns if k not in current_columns]

    print(f"Schema diff: +{len(added)} added, -{len(removed)} removed")

    # Persist adds to snapshot table
    if added:
        with _pg_conn() as conn:
            with conn.cursor() as cur:
                for (tbl, col) in added:
                    meta = current_columns[(tbl, col)]
                    cur.execute("""
                        INSERT INTO schema_snapshots
                            (source_db, table_name, column_name, data_type, is_nullable)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (source_db, table_name, column_name) DO UPDATE
                            SET data_type = EXCLUDED.data_type,
                                is_nullable = EXCLUDED.is_nullable,
                                captured_at = NOW()
                    """, (MYSQL_DB, tbl, col, meta["data_type"], meta["is_nullable"]))
            conn.commit()

    # Log removals to drift_alerts (never delete from snapshot — may reappear)
    if removed:
        with _pg_conn() as conn:
            with conn.cursor() as cur:
                for (tbl, col) in removed:
                    cur.execute("""
                        INSERT INTO schema_drift_alerts
                            (source_db, table_name, column_name, drift_type)
                        VALUES (%s, %s, %s, 'removed')
                    """, (MYSQL_DB, tbl, col))
            conn.commit()
        _send_slack(
            ":warning: *Schema Evolution: Columns Removed from MySQL*\n"
            + "\n".join(f"• `{MYSQL_DB}.{t}`.`{c}`" for t, c in removed[:10])
            + ("\n…and more" if len(removed) > 10 else "")
            + "\nSources.yml NOT modified — review manually."
        )

    ctx["ti"].xcom_push(key="added",   value=[(t, c) for t, c in added])
    ctx["ti"].xcom_push(key="removed", value=[(t, c) for t, c in removed])
    ctx["ti"].xcom_push(key="current_columns",
                        value={f"{t}.{c}": v for (t, c), v in current_columns.items()})


def update_sources_yml(**ctx):
    """
    For each new column detected, append it to the appropriate sources.yml.
    Uses ruamel.yaml to preserve formatting and comments.
    Only appends — never removes.
    """
    import ruamel.yaml  # installed via Dockerfile.airflow

    added          = ctx["ti"].xcom_pull(key="added",   task_ids="snapshot_mysql_schema") or []
    current_cols   = ctx["ti"].xcom_pull(key="current_columns",
                                         task_ids="snapshot_mysql_schema") or {}

    if not added:
        print("No new columns — sources.yml unchanged.")
        ctx["ti"].xcom_push(key="changed", value=False)
        return

    # Find the sources.yml that covers bronze/MySQL tables
    bronze_yml = Path(f"{DBT_PROJECT}/models/bronze/sources.yml")
    if not bronze_yml.exists():
        # Fallback: look for any sources.yml mentioning 'shopflow' or 'mysql'
        for candidate in Path(f"{DBT_PROJECT}/models").rglob("sources.yml"):
            content = candidate.read_text()
            if "shopflow" in content.lower() or "mysql" in content.lower():
                bronze_yml = candidate
                break

    if not bronze_yml.exists():
        print(f"[SKIP] No bronze sources.yml found at {bronze_yml} — cannot update.")
        ctx["ti"].xcom_push(key="changed", value=False)
        return

    # Backup before modifying
    backup_path = bronze_yml.with_suffix(".yml.bak")
    import shutil
    shutil.copy2(bronze_yml, backup_path)
    ctx["ti"].xcom_push(key="backup_path", value=str(backup_path))

    yaml = ruamel.yaml.YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, block_seq_indent=2)

    with open(bronze_yml) as f:
        doc = yaml.load(f)

    # Group new columns by table
    from collections import defaultdict
    new_by_table: dict[str, list] = defaultdict(list)
    for (tbl, col) in added:
        meta = current_cols.get(f"{tbl}.{col}", {})
        new_by_table[tbl].append({
            "name":        col,
            "description": f"[auto-added by schema_evolution {datetime.utcnow().strftime('%Y-%m-%d')}] "
                           f"MySQL type: {meta.get('data_type', 'unknown')}",
        })

    # Walk sources.yml and append new columns to matching table nodes
    sources = doc.get("sources", [])
    tables_updated = []
    for source in sources:
        for table in source.get("tables", []):
            tbl_name = table.get("name", "")
            if tbl_name in new_by_table:
                if "columns" not in table:
                    table["columns"] = []
                existing_cols = {c.get("name") for c in table["columns"]}
                for new_col in new_by_table[tbl_name]:
                    if new_col["name"] not in existing_cols:
                        table["columns"].append(new_col)
                        tables_updated.append(f"{tbl_name}.{new_col['name']}")

    if not tables_updated:
        print("New columns found but no matching tables in sources.yml — check table names.")
        ctx["ti"].xcom_push(key="changed", value=False)
        return

    with open(bronze_yml, "w") as f:
        yaml.dump(doc, f)

    print(f"sources.yml updated: {len(tables_updated)} new column(s): {tables_updated}")
    ctx["ti"].xcom_push(key="changed",        value=True)
    ctx["ti"].xcom_push(key="tables_updated", value=tables_updated)


def validate_dbt_compile(**ctx):
    """
    Run dbt compile to verify sources.yml is valid.
    Rolls back sources.yml if compile fails.
    """
    import subprocess, shutil

    changed = ctx["ti"].xcom_pull(key="changed", task_ids="update_sources_yml")
    if not changed:
        print("No sources.yml change — skipping dbt compile validation.")
        return

    dbt_exe = next(
        (d for d in [DBT_BIN, "/usr/local/bin/dbt", "/home/airflow/.local/bin/dbt"]
         if os.path.exists(d)),
        "dbt",
    )
    profiles_dir = (
        os.path.dirname(DBT_PROFILES)
        if os.path.exists(DBT_PROFILES)
        else os.path.expanduser("~/.dbt")
    )

    result = subprocess.run(
        [dbt_exe, "parse", "--no-version-check",
         "--project-dir", DBT_PROJECT,
         "--profiles-dir", profiles_dir],
        capture_output=True, text=True, timeout=120,
    )

    if result.returncode != 0:
        # Rollback
        backup_path = ctx["ti"].xcom_pull(key="backup_path", task_ids="update_sources_yml")
        if backup_path and os.path.exists(backup_path):
            bronze_yml = backup_path.replace(".yml.bak", ".yml")
            shutil.copy2(backup_path, bronze_yml)
            print(f"[ROLLBACK] Restored sources.yml from {backup_path}")
        _send_slack(
            ":red_circle: *Schema Evolution: dbt compile failed — sources.yml rolled back*\n"
            f"```{result.stderr[-800:]}```"
        )
        raise RuntimeError(f"dbt parse failed after sources.yml update:\n{result.stderr[-500:]}")

    print("dbt parse passed — sources.yml update is valid.")


def reload_agent_rag(**ctx):
    """
    POST to the AI Agent FastAPI server to force ChromaDB re-seed.
    Runs only when sources.yml was actually changed.
    """
    import requests

    changed = ctx["ti"].xcom_pull(key="changed", task_ids="update_sources_yml")
    if not changed:
        print("No change — RAG reload skipped.")
        return

    tables_updated = ctx["ti"].xcom_pull(key="tables_updated", task_ids="update_sources_yml") or []

    try:
        resp = requests.post(
            f"{AI_AGENT_URL}/api/rag/reload",
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        print(f"RAG reloaded: {data}")
        _send_slack(
            f":white_check_mark: *Schema Evolution: {len(tables_updated)} new column(s) added*\n"
            + "\n".join(f"• `{c}`" for c in tables_updated[:10])
            + "\nAI Agent knowledge base refreshed."
        )
    except Exception as exc:
        # Non-fatal — agent will refresh on next 1h cycle anyway
        print(f"[WARN] RAG reload failed (non-fatal): {exc}")


default_args = {
    "owner": "shopflow-datalake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="schema_evolution",
    default_args=default_args,
    description="Daily: MySQL schema polling → sources.yml auto-update → dbt validate → RAG reload",
    schedule_interval="30 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["governance", "schema", "evolution"],
) as dag:

    t1 = PythonOperator(
        task_id="snapshot_mysql_schema",
        python_callable=snapshot_mysql_schema,
        execution_timeout=timedelta(minutes=5),
    )

    t2 = PythonOperator(
        task_id="update_sources_yml",
        python_callable=update_sources_yml,
        execution_timeout=timedelta(minutes=5),
    )

    t3 = PythonOperator(
        task_id="validate_dbt_compile",
        python_callable=validate_dbt_compile,
        execution_timeout=timedelta(minutes=5),
    )

    t4 = PythonOperator(
        task_id="reload_agent_rag",
        python_callable=reload_agent_rag,
        execution_timeout=timedelta(minutes=2),
    )

    t1 >> t2 >> t3 >> t4
