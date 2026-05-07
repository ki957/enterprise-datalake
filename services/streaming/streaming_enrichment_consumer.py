"""
Streaming Enrichment Continuous Consumer
=========================================
Long-running Kafka consumer that replaces the polling DAG (streaming_enrichment_dag.py).

Runs as a standalone service (systemd or Docker container) that:
1. Continuously consumes from dbz.shopflow.products
2. Enriches products via Groq in batches
3. Writes to ClickHouse raw.shopflow_products_enriched
4. Triggers dbt mart refresh on batch completion

Usage:
    python streaming_enrichment_consumer.py

Environment:
    KAFKA_BOOTSTRAP_SERVERS  — Kafka broker address
    GROQ_API_KEY             — Groq API key
    CLICKHOUSE_URL           — ClickHouse HTTP endpoint
    AI_AGENT_URL             — AI Agent service URL
    DATALAKE_HOME            — DataLake project root
    BATCH_SIZE               — Products per Groq call (default: 10)
    FLUSH_INTERVAL_SEC       — Seconds between batch flushes (default: 30)
"""

import json
import os
import time
import signal
import sys
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = "dbz.shopflow.products"
KAFKA_GROUP_ID  = "streaming-enrichment-consumer"

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

BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "10"))
FLUSH_INTERVAL   = int(os.getenv("FLUSH_INTERVAL_SEC", "30"))

_shutdown = False

def _handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    print(f"[{datetime.utcnow().isoformat()}] Received signal {signum}, shutting down...")

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def _ch(sql: str) -> str:
    import requests
    resp = requests.post(CH_URL, data=sql, auth=(CH_USER, CH_PASSWORD), timeout=30)
    resp.raise_for_status()
    return resp.text.strip()


def _ensure_raw_table():
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
    from groq import Groq

    client = Groq(api_key=GROQ_API_KEY)

    product_text = "\n".join(
        f"{i+1}. id={p.get('product_id', p.get('id','?'))} "
        f"name={p.get('name', p.get('product_name','?'))} "
        f"category={p.get('category','?')} price={p.get('price','?')}"
        for i, p in enumerate(products)
    )

    prompt = f"""You are a product classification analyst. Analyze these products and return ONLY a JSON array.

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
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as exc:
        print(f"  [WARN] Groq enrichment failed: {exc}")
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


def _flush_batch(batch: list[dict], id_to_raw: dict):
    if not batch:
        return 0

    rows = []
    for item in batch:
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
    print(f"[{datetime.utcnow().isoformat()}] Inserted {len(rows)} enriched products into raw.shopflow_products_enriched")
    return len(rows)


def _run_dbt_mart():
    import subprocess
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

    try:
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
            print(f"[WARN] dbt mart_product_enrichment failed: {result.stderr[-200:]}")
        else:
            print("dbt mart_product_enrichment: OK")
            # Reload RAG
            try:
                import requests
                resp = requests.post(f"{AI_AGENT_URL}/api/rag/reload", timeout=20)
                resp.raise_for_status()
                print(f"RAG reloaded: {resp.json()}")
            except Exception as exc:
                print(f"[WARN] RAG reload failed (non-fatal): {exc}")
    except Exception as exc:
        print(f"[WARN] dbt run failed: {exc}")


def main():
    print(f"[{datetime.utcnow().isoformat()}] Starting streaming enrichment consumer...")
    _ensure_raw_table()

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="replace")),
            consumer_timeout_ms=5000,
            session_timeout_ms=30000,
        )
    except NoBrokersAvailable:
        print("[ERROR] Kafka not available. Exiting.")
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] Kafka connection failed: {exc}. Exiting.")
        sys.exit(1)

    batch = []
    raw_products = []
    id_to_raw = {}
    last_flush = time.time()
    total_enriched = 0

    print(f"[{datetime.utcnow().isoformat()}] Consumer connected to {KAFKA_BOOTSTRAP}, topic {KAFKA_TOPIC}")

    try:
        while not _shutdown:
            for msg in consumer:
                if _shutdown:
                    break

                value = msg.value
                if not value or not isinstance(value, dict):
                    continue
                if "payload" in value:
                    value = value["payload"]

                raw_products.append(value)
                pid = value.get("product_id", value.get("id", 0))
                id_to_raw[pid] = value

                if len(raw_products) >= BATCH_SIZE:
                    enriched = _enrich_batch_with_groq(raw_products)
                    batch.extend(enriched)
                    raw_products = []

                    if len(batch) >= BATCH_SIZE:
                        total_enriched += _flush_batch(batch, id_to_raw)
                        batch = []
                        last_flush = time.time()
                        _run_dbt_mart()

            # Time-based flush
            if batch and (time.time() - last_flush) >= FLUSH_INTERVAL:
                total_enriched += _flush_batch(batch, id_to_raw)
                batch = []
                last_flush = time.time()
                _run_dbt_mart()

    finally:
        # Final flush on shutdown
        if batch:
            total_enriched += _flush_batch(batch, id_to_raw)
        consumer.close()
        print(f"[{datetime.utcnow().isoformat()}] Consumer stopped. Total enriched: {total_enriched}")


if __name__ == "__main__":
    main()
