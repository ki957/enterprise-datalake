# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A modular, Docker Compose-based Enterprise Data Lake with two data domains and an AI layer:

- **ShopFlow** — e-commerce: MySQL CDC → Debezium/Kafka → MinIO → ClickHouse → dbt silver/gold
- **SaaS** — subscriptions: PostgreSQL → Airflow extract → ClickHouse raw → dbt staging/marts
- **AI Layer** — LangGraph 11-agent system with React UI, FastAPI backend, cost tracking

All services run locally via Docker Compose. No cloud dependencies. Requires a `.env` file (copy from `.env.example`).

## Commands

```bash
# Stack bring-up (order matters — layers depend on prior layers)
make base          # Docker networks + volumes (always run first)
make all           # Full stack in dependency order
make ui            # AI Agent: FastAPI :8502 + React Vite :3001

# Layer-by-layer
make security      # Vault + Keycloak
make storage       # MinIO
make transform     # ClickHouse + Spark + dbt-docs nginx
make serving       # PostgreSQL
make governance    # Airflow (custom Dockerfile.airflow)
make observability # Prometheus + Grafana + Alertmanager + Loki
make visualization # Superset + Redis
make sources       # MySQL + SAP API Flask :5001
make streaming     # Kafka (KRaft) + Kafka Connect (Debezium)

# One-time setup (run after the relevant layer is up)
make setup-vault       # Write credentials to Vault
make setup-keycloak    # Provision datalake realm + Grafana OIDC client
make setup-debezium    # Register Debezium MySQL connector (idempotent)
make setup-superset    # ClickHouse datasource + dashboards

# Data seeding
make seed-mysql-small  # 500 customers / 200 products / 2000 orders
make seed-mysql        # 50K / 5K / 500K (production scale)
make seed-saas-small   # 500 users / 5K events / 500 subscriptions
make seed-saas         # Production-scale SaaS data
make seed-all          # Both domains at production scale

# Bronze ingestion without Airbyte (MySQL + HTTP → MinIO → ClickHouse views)
make bronze-init       # Full dataset
make bronze-init-small # Fast dev dataset

# Spark jobs (run on host against spark://localhost:7077)
make spark-unified     # Build gold.unified_customers from all domains
make spark-profile     # Profile MinIO Parquet → summary stats in MinIO logs/

# Testing
pytest tests/test_data_generators.py tests/test_saas_pipeline_logic.py -v
pytest tests/test_saas_pipeline_logic.py::test_ch_insert_serialisation -v  # single test
pytest tests/test_dag_integrity.py -v   # requires: pip install apache-airflow==2.8.0
make test-integration   # spins up isolated ClickHouse :18123 + PostgreSQL :15432

# AI Agent smoke test (4 questions, scored 0–100, PASS ≥ 70)
cd services/ai-agent && python test_agents.py

# dbt (run on host, targets localhost:8123)
cd transformation/dbt/datalake_transforms
dbt run --select marts          # agent-generated models
dbt run --select gold           # dimensions + facts
dbt run --select silver staging # both staging layers
dbt compile --select <model> --no-version-check --target-path /tmp/dbt_target
dbt parse --no-version-check    # syntax check only, no DB

# Operations
make ps / make logs / make ram / make health
make down    # stop, preserve volumes
make clean   # docker system prune -f (preserves volumes)
make nuke    # full reset including volumes (prompts for confirmation)
make apply-ttl  # apply ClickHouse TTL retention policies
```

### Required `~/.dbt/profiles.yml`

```yaml
datalake_transforms:
  target: dev
  outputs:
    dev:
      type: clickhouse
      host: localhost
      port: 8123
      user: default
      password: Click@2024
      schema: default
      secure: false
```

Set `DBT_TARGET_PATH=/tmp/dbt_target` when running dbt as non-root — the default `target/` directory may be root-owned inside Docker mounts.

## Service Access

| Service | URL | Credentials |
|---------|-----|-------------|
| AI Agent (React) | http://localhost:3001 | — |
| AI Agent API | http://localhost:8502 | — |
| Airflow | http://localhost:8080 | admin / Airflow@2024 |
| Superset | http://localhost:8088 | admin / Superset@2024 |
| Grafana | http://localhost:3000 | admin / Grafana@2024 |
| ClickHouse HTTP | http://localhost:8123 | default / Click@2024 |
| MinIO Console | http://localhost:9001 | admin / Minio@2024 |
| Keycloak | http://localhost:8180 | admin / (from .env) |
| Vault | http://localhost:8200 | token: root |
| PostgreSQL | localhost:5432 | postgres / Postgres@2024 |
| MySQL | localhost:3306 | root / MySQL@2024 |
| Spark Master | http://localhost:8081 | — |
| dbt Docs | http://localhost:8082 | — |

`Postgres@2024` → `Postgres%402024` in SQLAlchemy URLs. The governance compose hardcodes `%40` — never substitute `${POSTGRES_PASSWORD}` in connection URLs.

## Architecture

### Data Flow

```
MySQL (shopflow CDC)  ──► Debezium → Kafka topic dbz.shopflow.*
                                   └──► streaming_enrichment DAG (Groq AI → raw.shopflow_products_enriched)

MySQL / SAP API / REST ──► make bronze-init → MinIO raw/ Parquet
                        OR  Airbyte (k8s, not Docker Compose)

MinIO raw/ ──► bronze.src_* (ClickHouse s3() views via setup_clickhouse_bronze.py)
           ──► dbt silver/ → staging.*
           ──► dbt gold/   → gold.{dim_*, fct_*}

PostgreSQL (saas) ──► Airflow DAG → raw.* → dbt staging/marts → gold.*

gold.* ──► Superset / Grafana / AI Agent / Spark
```

### ClickHouse Schema Layers

| Schema | Layer | How created |
|--------|-------|-------------|
| `bronze` | S3 views over MinIO | `setup_clickhouse_bronze.py` — not dbt |
| `staging` | Silver + SaaS staging | dbt `models/silver/` + `models/staging/` |
| `gold` | Dimensions, facts, marts | dbt `models/gold/` + `models/marts/` |
| `raw` | SaaS extracts | Airflow `saas_pipeline.py` direct inserts |

`models/bronze/` and `models/raw/` contain only `sources.yml` — no SQL models.

### dbt Folder → ClickHouse Schema Mapping

Controlled by `macros/generate_schema_name.sql` (uses schema as-is, no target prefix):

| Folder | Schema | Engine |
|--------|--------|--------|
| `models/silver/` | `staging` | `MergeTree()` incremental |
| `models/gold/dimensions/` | `gold` | `ReplacingMergeTree()` — SCD Type 2 |
| `models/gold/facts/` | `gold` | `MergeTree()` append-only |
| `models/staging/` | `staging` | incremental |
| `models/marts/` | `gold` | table |

Agent-generated models land in `models/marts/` with a `-- generated by nl_dbt_agent` marker. The tool blocks overwriting files without this marker.

### Airflow DAGs (`orchestration/airflow/dags/`)

DAGs load from the host repo mount — file edits take effect without container restart.

| File | Schedule | Purpose |
|------|----------|---------|
| `saas_pipeline.py` | daily 02:00 | PostgreSQL → ClickHouse raw → dbt staging/marts |
| `shopflow_pipeline.py` | daily 06:00 | Airbyte syncs → MinIO → dbt silver/gold → Superset |
| `transformation_dags/dbt_runner.py` | daily 07:00 | Full dbt run + test |
| `transformation_dags/spark_profiler.py` | daily 07:30 | MinIO Parquet profiling |
| `transformation_dags/unified_profile.py` | daily 08:00 | Spark unified customer profile |
| `governance_dags/metadata_sync.py` | daily 08:00 | dbt docs, schema drift check |
| `governance_dags/schema_evolution_dag.py` | daily 08:30 | MySQL schema changes → auto-update sources.yml |
| `quality_dags/data_quality_suite.py` | daily 09:00 | Great Expectations checkpoint |
| `quality_dags/auto_contract_dag.py` | weekly | Profile gold tables → GE expectation suites |
| `streaming_dags/streaming_enrichment_dag.py` | every 5 min | Kafka → Groq enrichment → raw table |
| `ingestion_dags/airbyte_health.py` | hourly | Poll Airbyte connector status |

`AIRFLOW__API__AUTH_BACKENDS` must include `basic_auth` — without it all Orchestration Agent API calls return 401.

### AI Agent Layer (`services/ai-agent/`)

**Process model**: FastAPI (`server.py`) runs on the host at `:8502`. The legacy Streamlit `app.py` / `make ai-agent` is deprecated — use `make ui`.

**11 specialist agents** — each is a `create_react_agent` (LangGraph), max 4 tools (Groq limit):

| Agent | File | Trigger keywords |
|-------|------|-----------------|
| Insight | `insight_agent.py` | (default) |
| Schema | `schema_agent.py` | schema, column, describe |
| Quality | `quality_agent.py` | quality, null, duplicate |
| Orchestration | `orchestration_agent.py` | airflow, dag, failed |
| Ingestion | `ingestion_agent.py` | minio, ingest, parquet |
| Performance | `performance_agent.py` | slow, optimize, storage |
| Airbyte | `airbyte_agent.py` | airbyte, connector |
| Self-Healing | `self_healing_agent.py` | self heal, auto fix |
| Anomaly | `anomaly_agent.py` | anomaly detection, trend |
| NL→dbt | `nl_dbt_agent.py` | generate model, create dbt |
| Contract | `contract_agent.py` | expectation, contract |

Routing is keyword-scored in `graph/pipeline_graph.py::_route()`. Intent-specific agents (self_healing, anomaly, nl_dbt, contract) are weighted 3× so one keyword match beats multiple generic matches.

**Response cache**: 5-minute TTL keyed on `md5(f"{agent}:{message.strip().lower()}")`. Clear via `POST /api/rag/reload`.

**Self-healing guardrails** (`tools/healing_tools.py::_GUARDRAILS`):

| Action | Behaviour |
|--------|-----------|
| `restart_airflow_task`, `trigger_airbyte_sync` | `auto_act=True` — executes immediately |
| `rollback_dbt_model`, `switch_fallback_source`, `scale_spark_executor` | `auto_act=False` — requires human approval |
| `drop_table`, `delete_data` | `blocked=True` — permanently blocked |

## Critical Constraints

- **ClickHouse port**: host = `localhost:9002` (native driver, used by `clickhouse_tools.py`). Inside Docker = `clickhouse:9000`. HTTP API = `localhost:8123`.
- **No LAG/LEAD in ClickHouse SQL** — use self-join on `addMonths(dt, 1)` for period-over-period.
- **No `concat()` with Decimal** — cast first: `toString(round(amount, 2))`.
- **Date truncation**: `toStartOfMonth()` not `DATE_TRUNC`. Filter booleans: `is_current = 1` not `= true`.
- **Groq LLM** (`agents/base.py`): model `meta-llama/llama-4-scout-17b-16e-instruct`, `streaming=False` (streaming causes intermittent tool-call failures), max 4 tools per agent.
- **All `@tool` optional parameters must use `str` type** — Groq validates JSON types strictly; `int`/`bool` defaults cause errors when the LLM passes a quoted string. Convert internally (e.g. `int(limit) if str(limit).isdigit() else 10`).
- **`create_chart` labels/values** are `str` not `list` — `_parse_list()` in `chart_tools.py` accepts list, JSON array string, or comma-separated string.
- **Jinja `{{ }}` in tool call JSON** causes Groq parse errors — `dbt_write_tools.py` strips Jinja from LLM SQL and injects it server-side via `_normalise_sql()`.
- **Agent `STEP 0`** (question-type detection) must use the compact one-liner format — verbose multi-paragraph blocks bleed into the technical path and cause tool-call errors.
- **NL→dbt writes only to `models/marts/`** — `create_dbt_model` tool refuses to write anywhere else.
- **Airbyte runs in k8s** (`abctl`), not Docker Compose. From k8s pods, Docker services are at `172.17.0.1`.
- **Schema evolution DAG** rolls back `sources.yml` if `dbt compile` fails; removed columns are only logged, never auto-removed.
