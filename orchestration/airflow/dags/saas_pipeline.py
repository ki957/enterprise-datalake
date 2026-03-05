from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'datalake',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_to_clickhouse():
    import psycopg2
    import clickhouse_connect
    pg = psycopg2.connect(host="postgres", port=5432, database="airflow", user="postgres", password="Postgres@2024")
    ch = clickhouse_connect.get_client(host="clickhouse", port=8123, username="default", password="Click@2024")
    cur = pg.cursor()
    cur.execute("SELECT id,email,name,company,country,plan,mrr,status,created_at,updated_at FROM saas_users")
    users = cur.fetchall()
    ch.command("TRUNCATE TABLE IF EXISTS raw.saas_users")
    ch.insert("raw.saas_users", users, column_names=["id","email","name","company","country","plan","mrr","status","created_at","updated_at"])
    cur.execute("SELECT id,user_id,event_type,page,occurred_at FROM saas_events")
    events = cur.fetchall()
    ch.command("TRUNCATE TABLE IF EXISTS raw.saas_events")
    ch.insert("raw.saas_events", events, column_names=["id","user_id","event_type","page","occurred_at"])
    cur.close()
    pg.close()
    print(f"✅ Extracted {len(users)} users, {len(events)} events")

def check_data_quality():
    import clickhouse_connect
    ch = clickhouse_connect.get_client(host="clickhouse", port=8123, username="default", password="Click@2024")
    users = ch.query("SELECT count() FROM raw.saas_users").result_rows[0][0]
    events = ch.query("SELECT count() FROM raw.saas_events").result_rows[0][0]
    assert users > 0, "No users found!"
    assert events > 0, "No events found!"
    print(f"✅ Quality check passed: {users} users, {events} events")

with DAG(
    dag_id="saas_data_pipeline",
    default_args=default_args,
    description="SaaS Data Lake End-to-End Pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["saas", "datalake", "poc"],
) as dag:

    extract = PythonOperator(
        task_id="extract_postgres_to_clickhouse",
        python_callable=extract_to_clickhouse,
    )

    quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_data_quality,
    )

    extract >> quality_check
