"""
Streaming Enrichment DAG
========================
DAG: streaming_enrichment
Schedule: every 5 minutes

Consumes Kafka topic dbz.shopflow.products (Debezium CDC from MySQL),
calls Groq llama-4-scout in batches of 10 for AI enrichment,
writes results to raw.shopflow_products_enriched,
then triggers dbt mart refresh and RAG reload on first-run detection.

Enrichment fields added per product:
  - sentiment:          positive / neutral / negative
  - quality_tier:       premium / standard / budget
  - enriched_category:  more granular category label from LLM
  - keywords:           Array(String) of descriptive tags

Integration with AI Agent:
  dbt mart mart_product_enrichment.sql reads raw.shopflow_products_enriched
  and lands the enriched data in gold.mart_product_enrichment.
  The live schema sync in knowledge_base.py picks up gold.mart_product_enrichment
  automatically on next reload, making it queryable by the Insight Agent.
"""

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = "dbz.shopflow.products"
KAFKA_GROUP_ID  = "airflow-enrichment-consumer"

GROQ_API_KEY    = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL      = os.getenv("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")

CH_URL      = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
CH_USER     = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")

AI_AGENT_URL  = os.getenv("AI_AGENT_URL", "http://172.17.0.1:8502")
DATALAKE_HOME = os.getenv("DATALAKE_HOME", "/opt/datalake")
DBT_PROJECT   = f"{DATALAKE_HOME}/transformation/dbt/datalake_transforms"
DBT_PROFILES  = "/opt/airflow/dbt_profiles.yml"
DBT_BIN       = "/home/airflow/.local/bin/dbt"

BATCH_SIZE       = 10    # products per Groq call — avoids rate limits
POLL_TIMEOUT_MS  = 15000 # ms to wait for Kafka messages (group coordinator handshake needs ~5s+)
MAX_MESSAGES     = 200   # max messages per DAG run


def _ch(sql: str) -> str:
    import requests
    resp = requests.post(CH_URL, data=sql, auth=(CH_USER, CH_PASSWORD), timeout=30)
    resp.raise_for_status()
    return resp.text.strip()


def _ensure_raw_table():
    """Create raw.shopflow_products_enriched if it doesn't exist."""
    _ch("CREATE DATABASE IF NOT EXISTS raw")
    _ch("""
        CREATE TABLE IF NOT EXISTS raw.shopflow_products_enriched (
            product_id         UInt64,
            product_name       String,
            original_category  String,
            enriched_category  String,
            sentiment          LowCardinality(String),
            quality_tier       LowCardinality(String),
            keywords           Array(String),
            enriched_at        DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(enriched_at)
        ORDER BY product_id
    """)


def _enrich_batch_with_groq(products: list[dict]) -> list[dict]:
    """
    Call Groq to enrich a batch of products.
    Returns list of enrichment dicts: {product_id, sentiment, quality_tier,
                                        enriched_category, keywords}
    Falls back to neutral/standard/original_category on any error.
    """
    from groq import Groq

    client = Groq(api_key=GROQ_API_KEY)

    product_text = "\n".join(
        f"{i+1}. id={p.get('product_id', p.get('id','?'))} "
        f"name={p.get('name', p.get('product_name','?'))} "
        f"category={p.get('category','?')} price={p.get('price','?')}"
        for i, p in enumerate(products)
    )

    prompt = f"""You are a product classification assistant. Analyze these products and return ONLY a JSON array.

Products:
{product_text}

Return a JSON array with exactly {len(products)} objects, one per product, in order:
[
  {{
    "product_id": <number>,
    "sentiment": "positive" | "neutral" | "negative",
    "quality_tier": "premium" | "standard" | "budget",
    "enriched_category": "<specific subcategory>",
    "keywords": ["<tag1>", "<tag2>", "<tag3>"]
  }}
]

Rules:
- sentiment: positive=high-quality/popular/well-reviewed, negative=cheap/problematic/low-quality, neutral=average
- quality_tier: premium=price>100 or luxury category, budget=price<20 or generic, standard=everything else
- enriched_category: be more specific than the original (e.g. "Electronics" → "Smartphones")
- keywords: 3-5 descriptive tags
- Return ONLY the JSON array, no other text."""

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as exc:
        print(f"  [WARN] Groq enrichment failed for batch: {exc}")
        # Return neutral defaults so pipeline doesn't stall
        return [
            {
                "product_id":        p.get("product_id", p.get("id", 0)),
                "sentiment":         "neutral",
                "quality_tier":      "standard",
                "enriched_category": p.get("category", "Unknown"),
                "keywords":          [],
            }
            for p in products
        ]


def consume_and_enrich(**ctx):
    """
    Poll Kafka for product CDC events, enrich via Groq, write to ClickHouse.
    Tracks whether this is the first successful enrichment run (for dbt trigger).
    """
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    _ensure_raw_table()

    # Try to connect to Kafka — non-fatal if not yet running
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="replace")),
            consumer_timeout_ms=POLL_TIMEOUT_MS,
            session_timeout_ms=30000,
        )
    except NoBrokersAvailable:
        print("[INFO] Kafka not available yet — streaming pipeline not started. Skipping.")
        ctx["ti"].xcom_push(key="records_enriched", value=0)
        return
    except Exception as exc:
        print(f"[WARN] Kafka connection failed: {exc}. Skipping.")
        ctx["ti"].xcom_push(key="records_enriched", value=0)
        return

    # Collect messages up to MAX_MESSAGES
    raw_products = []
    try:
        for msg in consumer:
            value = msg.value
            if not value or not isinstance(value, dict):
                continue
            # Debezium JSON serializer wraps in {"schema":{...},"payload":{...}} even with
            # ExtractNewRecordState — unwrap to the actual row data
            if "payload" in value:
                value = value["payload"]
            raw_products.append(value)
            if len(raw_products) >= MAX_MESSAGES:
                break
    finally:
        consumer.close()

    if not raw_products:
        print("No new product messages in Kafka — nothing to enrich.")
        ctx["ti"].xcom_push(key="records_enriched", value=0)
        return

    print(f"Consumed {len(raw_products)} product messages. Enriching in batches of {BATCH_SIZE}...")

    all_enriched = []
    for i in range(0, len(raw_products), BATCH_SIZE):
        batch = raw_products[i : i + BATCH_SIZE]
        enriched = _enrich_batch_with_groq(batch)
        all_enriched.extend(enriched)
        # Small delay between Groq calls to respect rate limits
        import time
        if i + BATCH_SIZE < len(raw_products):
            time.sleep(0.5)

    if not all_enriched:
        ctx["ti"].xcom_push(key="records_enriched", value=0)
        return

    # Build name/category lookup from raw messages (Debezium uses product_id)
    id_to_raw = {p.get("product_id", p.get("id", 0)): p for p in raw_products}

    # Insert into ClickHouse raw table
    rows = []
    for item in all_enriched:
        pid  = int(item.get("product_id", 0))
        raw  = id_to_raw.get(pid, {})
        name = str(raw.get("name", "")).replace("'", "\\'")
        orig_cat = str(raw.get("category", item.get("enriched_category", "Unknown"))).replace("'", "\\'")
        enr_cat  = str(item.get("enriched_category", orig_cat)).replace("'", "\\'")
        sentiment = item.get("sentiment", "neutral")
        tier      = item.get("quality_tier", "standard")
        keywords  = item.get("keywords", [])
        kw_str    = "[" + ", ".join(f"'{k}'" for k in keywords[:5]) + "]"
        rows.append(
            f"({pid}, '{name}', '{orig_cat}', '{enr_cat}', "
            f"'{sentiment}', '{tier}', {kw_str}, now())"
        )

    _ch(
        "INSERT INTO raw.shopflow_products_enriched "
        "(product_id, product_name, original_category, enriched_category, "
        "sentiment, quality_tier, keywords, enriched_at) VALUES "
        + ", ".join(rows)
    )

    print(f"Inserted {len(rows)} enriched products into raw.shopflow_products_enriched")
    ctx["ti"].xcom_push(key="records_enriched", value=len(rows))


def run_dbt_mart(**ctx):
    """
    Run dbt mart_product_enrichment model to land enriched data in gold.
    Only runs when new records were enriched this cycle.
    """
    import subprocess

    records = ctx["ti"].xcom_pull(key="records_enriched", task_ids="consume_and_enrich") or 0
    if records == 0:
        print("No new records — skipping dbt mart refresh.")
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
        [dbt_exe, "run",
         "--select", "mart_product_enrichment",
         "--project-dir", DBT_PROJECT,
         "--profiles-dir", profiles_dir,
         "--no-version-check",
         "--target-path", "/tmp/dbt_target"],
        capture_output=True, text=True, timeout=120,
    )

    if result.returncode != 0:
        # Non-fatal — raw table is still populated, mart will run next cycle
        print(f"[WARN] dbt mart_product_enrichment failed:\n{result.stderr[-500:]}")
    else:
        print("dbt mart_product_enrichment: OK")
        ctx["ti"].xcom_push(key="mart_refreshed", value=True)


def reload_rag(**ctx):
    """
    POST /api/rag/reload so the Insight Agent immediately knows about
    gold.mart_product_enrichment via the live schema sync.
    Only runs when the dbt mart was successfully refreshed.
    """
    import requests

    mart_refreshed = ctx["ti"].xcom_pull(key="mart_refreshed", task_ids="run_dbt_mart")
    if not mart_refreshed:
        return

    try:
        resp = requests.post(f"{AI_AGENT_URL}/api/rag/reload", timeout=20)
        resp.raise_for_status()
        print(f"RAG reloaded: {resp.json()}")
    except Exception as exc:
        print(f"[WARN] RAG reload failed (non-fatal): {exc}")


default_args = {
    "owner": "shopflow-datalake",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="streaming_enrichment",
    default_args=default_args,
    description="Every 5min: Kafka CDC → Groq enrichment → raw.shopflow_products_enriched → dbt gold mart",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "enrichment", "real-time"],
) as dag:

    t1 = PythonOperator(
        task_id="consume_and_enrich",
        python_callable=consume_and_enrich,
        execution_timeout=timedelta(minutes=4),
    )

    t2 = PythonOperator(
        task_id="run_dbt_mart",
        python_callable=run_dbt_mart,
        execution_timeout=timedelta(minutes=3),
    )

    t3 = PythonOperator(
        task_id="reload_rag",
        python_callable=reload_rag,
        execution_timeout=timedelta(minutes=1),
    )

    t1 >> t2 >> t3
