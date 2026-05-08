# Enterprise Data Lake

> A fully self-contained, production-pattern data lake with medallion architecture, 11 LangGraph AI agents, and a complete observability stack — running entirely in Docker Compose.

[![Python](https://img.shields.io/badge/python-3.11-blue?style=flat-square&logo=python)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt--clickhouse-1.10.0-FF694B?style=flat-square&logo=dbt)](https://www.getdbt.com)
[![Airflow](https://img.shields.io/badge/airflow-2.8.0-017CEE?style=flat-square&logo=apache-airflow)](https://airflow.apache.org)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?style=flat-square&logo=docker)](https://www.docker.com)
[![LangGraph](https://img.shields.io/badge/langgraph-0.2.76-7C3AED?style=flat-square)](https://github.com/langchain-ai/langgraph)
[![CI](https://github.com/ki957/enterprise-datalake/actions/workflows/ci.yml/badge.svg)](https://github.com/ki957/enterprise-datalake/actions/workflows/ci.yml)

**25+ services · 11 DAGs · 11 AI agents · 40+ dbt models · 13 CI checks**

---

## What Is This

Two data domains run through a unified pipeline. **ShopFlow** is an e-commerce domain sourced from MySQL 8.0 via CDC — up to 50K customers, 5K products, and 500K orders seeded with realistic synthetic data. **SaaS** is a subscription domain sourced from PostgreSQL 15 — up to 10K users, 200K events, and 10K subscriptions. Both domains follow an ELT philosophy: raw data lands in MinIO as Parquet and is never discarded; all transformations happen inside ClickHouse via dbt.

Data flows through a medallion architecture. The **Bronze** layer is a set of ClickHouse VIEWs that read Parquet files directly from MinIO using the `s3()` table function — zero data is copied into ClickHouse at this stage. The **Silver** layer is dbt-managed `MergeTree` staging tables with cleaned types, standardized timestamps, and null guards. The **Gold** layer is a dimensional star schema: `ReplacingMergeTree` dimensions for SCD Type 2 and `MergeTree` facts for append-only records. Eleven Airflow DAGs orchestrate the full daily pipeline including Great Expectations quality checks, dbt testing, and Superset dashboard refresh.

On top of the data stack sits an AI agent layer. A LangGraph `StateGraph` supervisor routes user messages to 11 specialist ReAct agents powered by Groq (Llama 4 Scout 17B). Each agent has access to domain-specific tools — ClickHouse SQL, Airflow REST, dbt model reads, MinIO inspection, and more. The system includes ChromaDB RAG with FK-aware document retrieval, PostgreSQL-backed conversation memory, per-call cost tracking, and a React 18 + FastAPI chat UI. There is no dependency on any cloud service.

---

## Architecture

### Data Flow

```text
DATA SOURCES
┌──────────────────────┐  ┌───────────────────┐  ┌───────────────────┐
│  MySQL 8.0 (CDC)     │  │  SAP OData API    │  │  PostgreSQL 15    │
│  ShopFlow :3306      │  │  Flask :5001      │  │  SaaS :5432      │
│  50K cust/500K ord   │  │  50 vendors/300PO │  │  10K users/subs  │
└──────────┬───────────┘  └────────┬──────────┘  └─────────┬─────────┘
           │ Debezium CDC (binlog)  │ PyArrow                │ psycopg2
           ▼                       ▼                         ▼
  Kafka 3.7 KRaft         ┌──────────────────────────────────────────┐
  Kafka Connect            │  MinIO  (S3-compatible Object Store)     │
  Debezium 2.6             │  s3://raw/   s3://silver/   s3://logs/  │
                           └───────────────────┬──────────────────────┘
                                               │ s3() table function
                                               ▼
                           ┌──────────────────────────────────────────┐
                           │  ClickHouse 23.8                          │
                           │  bronze.*  (S3 views, zero-copy)         │
                           │  staging.* (dbt Silver, MergeTree)       │
                           │  gold.*    (dbt Star Schema)             │
                           └──────────────────┬───────────────────────┘
                                              │
                ┌─────────────────────────────┤
                ▼                             ▼
    Airflow 2.8  (11 DAGs)         dbt-clickhouse 1.10
    Great Expectations 0.18        40+ models · 7 custom tests
                └─────────────────────────────┤
                                              ▼
                           ┌──────────────────────────────────────────┐
                           │  Serving & Visualization                  │
                           │  Superset 3.1 · Grafana 10.2 · dbt Docs │
                           └──────────────────┬───────────────────────┘
                                              ▼
                           ┌──────────────────────────────────────────┐
                           │  AI Agent Layer                           │
                           │  FastAPI :8502  +  React 18 :3001        │
                           │  11 LangGraph ReAct agents (Groq Llama 4)│
                           │  ChromaDB RAG · PostgreSQL memory         │
                           └──────────────────────────────────────────┘
                           ┌──────────────────────────────────────────┐
                           │  Observability                            │
                           │  Prometheus 2.48 · Loki 2.9 · Grafana   │
                           │  Alertmanager · StatsD exporter (Airflow)│
                           └──────────────────────────────────────────┘
                           ┌──────────────────────────────────────────┐
                           │  Security                                  │
                           │  HashiCorp Vault 1.15 (AppRole secrets)  │
                           │  Keycloak 23.0  (OIDC SSO)               │
                           └──────────────────────────────────────────┘
```

### Component Table

| Layer | Service | Version | Role |
|---|---|---|---|
| Sources | MySQL | 8.0 | ShopFlow CDC source; binlog ROW format, GTID mode |
| Sources | PostgreSQL | 15 | SaaS subscriptions + Airflow/Superset metadata DB |
| Sources | SAP OData API | Flask (internal) | Simulated SAP with 50 vendors, 300 purchase orders |
| Streaming | Apache Kafka | 3.7.0 (KRaft) | CDC event stream; no Zookeeper required |
| Streaming | Kafka Connect / Debezium | 2.6 | MySQL binlog → Kafka topics `dbz.shopflow.*` |
| Ingestion | Airbyte | 0.50.40 | MySQL + REST → MinIO Parquet (runs in k8s via `abctl`) |
| Storage | MinIO | latest | S3-compatible object store; buckets: raw, silver, gold, checkpoints, logs |
| OLAP | ClickHouse | 23.8-alpine | Columnar analytics engine; S3 table function for zero-copy bronze reads |
| Transform | dbt-clickhouse | 1.10.0 | 40+ SQL models; silver staging + gold star schema; 7 custom data tests |
| Transform | Apache Spark | 3.5.0 | Cross-domain unified customer profile job; MinIO bronze Parquet profiler |
| Orchestration | Apache Airflow | 2.8.0 | 11 DAGs; LocalExecutor; Vault secrets backend; StatsD → Prometheus bridge |
| Data Quality | Great Expectations | 0.18.0 | 6 expectation suites targeting ClickHouse; 1 checkpoint |
| Visualization | Apache Superset | 3.1.0 | BI dashboards; ClickHouse datasource; Keycloak SSO; git-tracked state |
| Visualization | Grafana | 10.2.0 | Infrastructure + pipeline metrics; Loki log exploration; Keycloak SSO |
| AI Backend | FastAPI + uvicorn | — | AI Agent REST API on :8502; Prometheus metrics; OTEL tracing |
| AI Frontend | React 18 + Vite | 5.3.1 | Chat UI; Zustand state; Plotly charts; PWA with service worker |
| Observability | Prometheus | 2.48.0 | Metrics scraping (30-day retention); alert rules for pipeline health |
| Observability | Loki | 2.9.3 | Log aggregation via Promtail; queryable from Grafana |
| Observability | Alertmanager | 0.26.0 | Slack webhook routing for pipeline failure alerts |
| Security | HashiCorp Vault | 1.15 | Dev-mode secrets backend for all Airflow connections |
| Security | Keycloak | 23.0 | OIDC provider for Grafana + Superset; realm: `datalake` |
| Cache | Redis | 7.2-alpine | Superset cache + session backend |

---

## Tech Stack

| Domain | Technology | Why |
|---|---|---|
| Sources | MySQL 8.0, PostgreSQL 15 | CDC-capable (binlog GTID), battle-tested relational stores |
| Streaming | Apache Kafka 3.7 KRaft | Eliminates Zookeeper; single-binary deployment |
| Streaming | Debezium 2.6 | Low-latency MySQL CDC with schema change detection |
| Object Storage | MinIO | S3-compatible, self-hosted, Parquet-native; preserves raw data indefinitely |
| OLAP | ClickHouse 23.8 | Sub-second queries over 500K+ rows; native S3 table function for zero-copy bronze layer |
| Transform | dbt-clickhouse 1.10 | SQL-first, version-controlled transforms; ReplacingMergeTree + MergeTree engine control |
| Transform | Apache Spark 3.5 | Cross-domain joins too large for ClickHouse SQL; produces unified customer profile |
| Orchestration | Apache Airflow 2.8 | DAG-based scheduling; REST API for AI agent control; StatsD → Prometheus metrics bridge |
| Data Quality | Great Expectations 0.18 | Expectation suites checked in CI and run daily; contract auto-generation via AI agent |
| AI / LLM | Groq API — Llama 4 Scout 17B | Free tier; <5s P95 latency; function calling support; `streaming=False` required |
| AI Framework | LangGraph 0.2.76 | Compiled `StateGraph` supervisor; deterministic keyword routing without LLM classification |
| Vector DB | ChromaDB 0.5.23 | Local RAG with FK-aware document retrieval; live schema sync from ClickHouse |
| Frontend | React 18.3, Vite 5.3, Tailwind 3.4 | Fast dev iteration; PWA service worker; lazy-loaded Plotly (4.8 MB chunk) |
| State | Zustand 4.5 | Minimal React state without Redux boilerplate |
| Observability | Prometheus + Grafana + Loki | Full metrics + logs stack; 3 pre-built dashboards |
| Security | HashiCorp Vault 1.15 | AppRole auth; all Airflow connections read from Vault at runtime |
| Security | Keycloak 23.0 | OIDC SSO; eliminates per-service credential management for operators |
| CI/CD | GitHub Actions + Docker Buildx | 13-check CI pipeline; GHCR image push; dbt docs to GitHub Pages |
| Linting | Ruff 0.4.4, sqlfluff 3.0.7, yamllint 1.35.1 | Pre-commit hooks enforce quality on every commit |

---

## Quick Start

**Prerequisites:** Docker with Compose v2, Python 3.11, ~8–12 GB RAM (use `seed-*-small` + `bronze-init-small` on constrained machines).

**1. Clone and configure**
```bash
git clone https://github.com/ki957/enterprise-datalake.git
cd enterprise-datalake
cp .env.example .env
# Edit .env — set GROQ_API_KEY at minimum
```

**2. Create networks and volumes**
```bash
make base
```

**3. Start the full stack**
```bash
make all
# Brings up 13 compose layers in dependency order (~3-5 min first run)
```

**4. Seed source data**
```bash
make seed-all          # 50K customers, 500K orders, 10K SaaS users (production scale)
# or: make seed-mysql-small && make seed-saas-small  (faster, dev-size)
```

**5. Initialize the Bronze layer**
```bash
make bronze-init       # MySQL + HTTP → MinIO Parquet + ClickHouse S3 views
# or: make bronze-init-small
```

**6. Start the AI Agent UI**
```bash
make ui
# FastAPI starts at http://localhost:8502
# React UI starts at http://localhost:3001
```

> **Shortcut:** `make setup` runs steps 2–5 end-to-end with error tolerance.

---

## Make Commands

<details>
<summary><strong>Stack Control</strong></summary>

```bash
make base           # Create Docker networks and named volumes (run first)
make all            # Full stack in dependency order
make down           # Stop all containers, preserve volumes
make clean          # down + docker system prune -f
make nuke           # Destructive: prompts for confirmation, deletes all volumes
make reset          # down + volume prune + make setup
make ps             # Container status table
make ram            # Memory + CPU usage per container
make health         # Smoke-test ClickHouse, MinIO, Airflow, Prometheus, Grafana endpoints
make logs           # Tail all container logs (last 50 lines each)
```
</details>

<details>
<summary><strong>Data Seeding</strong></summary>

```bash
make seed-mysql          # 50K customers / 5K products / 500K orders
make seed-mysql-small    # 500 / 200 / 2K  (development)
make seed-saas           # 10K users / 200K events / 10K subscriptions
make seed-saas-small     # 500 / 5K / 500  (development)
make seed-all            # Both domains at production scale
```
</details>

<details>
<summary><strong>Bronze Layer</strong></summary>

```bash
make bronze-init         # MySQL + HTTP → MinIO Parquet → ClickHouse S3 views (3 steps)
make bronze-init-small   # Same with small dataset
make setup-debezium      # Register Debezium MySQL connector in Kafka Connect (idempotent)
```
</details>

<details>
<summary><strong>AI Agent</strong></summary>

```bash
make ui       # Install deps + start FastAPI :8502 + React Vite :3001
make deploy   # Build Docker image → GHCR + build React production bundle
```
</details>

<details>
<summary><strong>Spark</strong></summary>

```bash
make spark-unified   # Submit unified_customer_profile.py → gold.unified_customers
make spark-profile   # Submit MinIO Parquet profiler → s3://logs/profiling/
```
</details>

<details>
<summary><strong>Testing</strong></summary>

```bash
make test              # Unit tests: test_data_generators + test_saas_pipeline_logic + DAG integrity
make test-integration  # Spins up docker-compose.test.yml (ClickHouse :18123 + PostgreSQL :15432),
                       # runs tests/integration/, tears down
```
</details>

<details>
<summary><strong>Setup & Operations</strong></summary>

```bash
make setup              # Full one-command bootstrap: vault → keycloak → seed → bronze → superset
make setup-vault        # Write all pipeline credentials to Vault
make setup-keycloak     # Provision datalake realm + Grafana/Superset OIDC clients
make setup-superset     # ClickHouse datasource + dashboards + export to git
make export-superset    # Export current dashboard state to infrastructure/superset/dashboards/
make import-superset    # Re-import dashboards from git into running Superset
make apply-ttl          # Apply ClickHouse TTL retention policies to bronze/staging tables
```
</details>

---

## Service Catalog

| Service | URL | Credentials |
|---|---|---|
| AI Agent UI (React) | http://localhost:3001 | — |
| AI Agent API (FastAPI) | http://localhost:8502 | — |
| AI Agent API Docs | http://localhost:8502/docs | — |
| Airflow | http://localhost:8080 | admin / (set in .env) |
| Apache Superset | http://localhost:8088 | admin / (set in .env) |
| Grafana | http://localhost:3000 | admin / (set in .env) or Keycloak SSO |
| ClickHouse HTTP | http://localhost:8123 | default / (set in .env) |
| MinIO Console | http://localhost:9001 | admin / (set in .env) |
| MinIO S3 API | http://localhost:9000 | — |
| Keycloak | http://localhost:8180 | admin / (set in .env) |
| HashiCorp Vault | http://localhost:8200 | Token: root (dev mode) |
| Airbyte | http://localhost:8000 | (k8s via abctl — see below) |
| Kafka Connect | http://localhost:8083 | — |
| Prometheus | http://localhost:9090 | — |
| Alertmanager | http://localhost:9093 | — |
| Spark Master UI | http://localhost:8081 | — |
| dbt Docs | http://localhost:8082 | — |
| SAP OData API | http://localhost:5001 | — |

All credentials are configured in `.env` (copy from `.env.example`). Default values in `.env.example` are for local development only.

> **Airbyte** runs in Kubernetes via `abctl`, not Docker Compose. The `make ingestion` target starts an nginx reverse proxy only. From k8s pods, Docker services are reachable at `172.17.0.1` (bridge gateway).

---

## Data Domains & Pipeline

### ShopFlow (E-commerce)

**Source:** MySQL 8.0 with binlog CDC enabled (GTID mode, ROW format). Tables: `customers`, `orders`, `order_items`, `products`. Vendors and purchase orders are sourced from a Flask-based SAP OData simulator. Reviews come from JSONPlaceholder REST.

**Scale:** Up to 50K customers, 5K products, 500K orders seeded by `scripts/generate_mysql_data.py`.

**Ingestion paths:**
- **Batch:** Airbyte 0.50.40 syncs MySQL + SAP + REST → MinIO Parquet (`raw/airbyte/mysql/`, `raw/airbyte/sap/`)
- **Direct (no Airbyte):** `make bronze-init` runs `scripts/ingest_mysql_to_minio.py` + `scripts/ingest_http_to_minio.py`
- **Streaming:** Debezium → Kafka topics `dbz.shopflow.*` → `streaming_enrichment` DAG (every 5 min) → Groq AI product enrichment → `raw.shopflow_products_enriched`

**Medallion:**
- Bronze: `bronze.src_customers`, `bronze.src_orders`, `bronze.src_products`, `bronze.src_vendors`, `bronze.src_purchase_orders`, `bronze.src_reviews`
- Silver: `staging.stg_customers`, `stg_orders`, `stg_products`, `stg_vendors`, `stg_purchase_orders`, `stg_reviews`
- Gold: `gold.dim_customers` (SCD2), `dim_products`, `dim_vendors`, `fct_orders`, `fct_procurement`, `fct_reviews`

**DAG:** `shopflow_datalake_pipeline` — daily at 06:00 UTC. Tasks: trigger Airbyte syncs → wait for MinIO files → dbt source freshness → dbt silver → dbt gold → dbt tests → Great Expectations → Superset refresh.

---

### SaaS (Subscriptions)

**Source:** PostgreSQL 15. Tables: `saas_users`, `saas_events`, `saas_subscriptions`.

**Scale:** Up to 10K users, 200K events, 10K subscriptions seeded by `scripts/generate_saas_data.py`.

**Ingestion:** Airflow `extract_postgres_to_clickhouse` task — psycopg2 cursor → ClickHouse HTTP bulk insert into `raw.*` tables. Rows serialized as TSV (`None → \N`, Decimal cast to string, microseconds stripped).

**Medallion:**
- Silver: `staging.stg_users`, `stg_events`, `stg_subscriptions`
- Gold Marts: `gold.dim_users` (SCD2), `fct_daily_active_users`, `fct_event_funnel`, `fct_mrr`, `fct_mrr_by_plan`, `fct_weekly_active_users`, `fct_customer_churn_rate`, `fct_customer_lifetime_value_by_segment`, `fct_customer_journey`, `fct_revenue_by_plan`

**DAG:** `saas_data_pipeline` — daily at 06:30 UTC (30-minute stagger after ShopFlow).

---

### Cross-Domain

A Spark job (`services/spark/jobs/unified_customer_profile.py`) joins ShopFlow customers against SaaS users to produce `gold.unified_customers`. The `unified_customer_profile` Airflow DAG waits on both pipeline DAGs via `ExternalTaskSensor` before submitting the Spark job.

---

## Medallion Architecture

```text
RAW (MinIO Parquet — preserved indefinitely)
  └─► BRONZE  (ClickHouse S3 VIEWs — zero-copy, type casting only)
        └─► SILVER  (dbt MergeTree — cleaned, deduped, standardized)
              └─► GOLD  (dbt star schema — dims + facts, BI-ready)
                    └─► MARTS  (cross-domain KPIs and aggregations)
```

**Bronze** — ClickHouse VIEWs using the `s3()` table function. Every query reads Parquet from MinIO at runtime; nothing is stored in ClickHouse. Type coercions (string → DateTime, string → Decimal) happen here. Created imperatively by `scripts/setup_clickhouse_bronze.py`, not by dbt.

**Silver** — dbt incremental `MergeTree` tables in the `staging` schema. Handles: lowercased emails, trimmed strings, null-guarded foreign keys, standardized timestamps. Six ShopFlow models, three SaaS models.

**Gold** — dbt `table` models. Dimensions use `ReplacingMergeTree` for SCD Type 2 deduplication (filter: `is_current = 1`). Facts use `MergeTree` for append-only insert semantics. Seven custom dbt data tests validate business invariants (e.g. `assert_orders_amount_positive.sql`, `assert_dim_customers_one_current_per_id.sql`).

**Marts** — Cross-domain dbt models in `models/marts/` — churn rate, MRR trends, event funnels, customer lifetime value by segment, rolling revenue by category, weekly active users. Agent-generated models also land here (marker: `-- generated by nl_dbt_agent`).

---

## AI Agent System

### Architecture

```text
User Message
     │
     ▼
pipeline_graph.py::_route()          ← keyword scoring, no LLM call
     │
     ├─► insight_agent       (default)
     ├─► schema_agent
     ├─► quality_agent
     ├─► orchestration_agent
     ├─► ingestion_agent
     ├─► performance_agent
     ├─► airbyte_agent
     ├─► self_healing_agent  (3× weight)
     ├─► anomaly_agent       (3× weight)
     ├─► nl_dbt_agent        (3× weight)
     └─► contract_agent      (3× weight)
          │
          ▼
   create_react_agent (LangGraph)
   ≤4 tools per agent (Groq limit)
   Llama 4 Scout 17B, streaming=False
          │
          ▼
   ChromaDB RAG (FK-aware retrieval)
   PostgreSQL memory (ai_agent_memory)
   Cost log (agent_cost_log)
   OTEL trace → Grafana
```

If all three Groq retries fail, the system returns a RAG-only answer. Users always receive a substantive response.

### The 11 Agents

| Agent | Routing Keywords | Capability |
|---|---|---|
| Insight | *(default)* | Natural-language SQL against ClickHouse gold layer; inline Plotly chart generation |
| Schema | schema, column, describe, lineage | Reads raw `.sql` model files; reports dbt source freshness |
| Quality | quality, null, duplicate, integrity | Great Expectations run status; null rate analysis; dbt test results |
| Orchestration | airflow, dag, run, trigger, schedule | Trigger DAGs; get run history; inspect task failure logs via Airflow REST API |
| Ingestion | minio, ingest, parquet, bronze, kafka | MinIO file landing checks; S3 view status; Kafka/Debezium health |
| Performance | slow, optimize, storage, p95, profile | ClickHouse `system.query_log` analysis; storage size per table |
| Airbyte | airbyte, connector, sync, connection | Airbyte sync status; connector health; OAuth2 token cached 55 min |
| Self-Healing | self heal, auto fix, diagnose, repair | Diagnoses failures; applies fixes within the guardrail table |
| Anomaly | anomaly, unusual pattern, trend | Contextual anomaly detection — compares against weekday baselines, not simple thresholds |
| NL→dbt | generate model, create dbt model, build metric | Writes production-ready dbt SQL to `models/marts/`; auto-runs `dbt compile` to validate |
| Contract | expectation, data contract, GE suite | Profiles tables; auto-generates Great Expectations suites from real data statistics |

### Tools

| Module | Tools provided |
|---|---|
| `clickhouse_tools.py` | Execute SQL, describe table schema, inspect slow query log |
| `airflow_tools.py` | Trigger DAG, get run history, fetch task instance logs |
| `dbt_tools.py` | Model status, source freshness, dbt test results |
| `dbt_write_tools.py` | Write new dbt model to `models/marts/`, run `dbt compile` |
| `minio_tools.py` | List buckets, check file presence, get object metadata |
| `airbyte_tools.py` | Sync status, connection health (OAuth2 token auto-refresh) |
| `healing_tools.py` | Restart Airflow task, trigger Airbyte sync, request approval for risky actions |
| `profiling_tools.py` | Column statistics, cardinality, null rates |
| `contract_tools.py` | Generate and write Great Expectations expectation suites |
| `chart_tools.py` | Create inline Plotly charts from query results |

**Self-healing guardrails** (`tools/healing_tools.py`): `restart_airflow_task` and `trigger_airbyte_sync` execute autonomously. `rollback_dbt_model`, `switch_fallback_source`, and `scale_spark_executor` require human approval. `drop_table` and `delete_data` are permanently blocked.

---

## Airflow DAG Catalog

| DAG | Schedule | Description |
|---|---|---|
| `shopflow_datalake_pipeline` | Daily 06:00 UTC | Airbyte syncs → MinIO wait → dbt silver/gold → dbt tests → Great Expectations → Superset refresh |
| `saas_data_pipeline` | Daily 06:30 UTC | PostgreSQL extract → ClickHouse raw → dbt staging/marts → quality check |
| `streaming_enrichment` | Every 5 min | Kafka CDC events → Groq AI product enrichment → `raw.shopflow_products_enriched` |
| `dbt_standalone_runner` | Daily 07:00 UTC | Full dbt run + test across all layers; reloads RAG on completion |
| `spark_data_profiler` | Daily 07:30 UTC | PySpark profiling of MinIO bronze Parquet → report to `s3://logs/profiling/` |
| `unified_customer_profile` | Daily 08:00 UTC | Spark cross-domain join: ShopFlow × SaaS → `gold.unified_customers` (waits on both pipelines) |
| `metadata_sync` | Daily 08:00 UTC | Publish dbt docs, log pipeline metadata, detect schema drift |
| `schema_evolution_dag` | Daily 08:30 UTC | Poll MySQL schema changes → auto-update `sources.yml` → `dbt compile` validate → reload RAG; rolls back `sources.yml` on compile failure |
| `data_quality_suite` | Daily 09:00 UTC | Extended quality checks: row counts, null rates, referential integrity across gold layer |
| `auto_contract_dag` | Weekly | Profile gold tables → write GE expectation suites; flag columns with no consumers as deprecation candidates |
| `airbyte_connection_health` | Hourly | Poll Airbyte connector status; alert on connection failures |

---

## CI/CD Pipeline

### CI Workflow (`ci.yml`)

Runs on every push and pull request to `main`/`master`. Uses `concurrency: cancel-in-progress` to avoid wasted runs.

| Job | Tool | What it checks |
|---|---|---|
| Secret Scan | detect-secrets 1.4.0 | Full repo scan; fails on any new credential not in baseline |
| Unit Tests | pytest 8.2.0 | `test_data_generators.py`, `test_saas_pipeline_logic.py`, `test_dag_integrity.py` |
| YAML Lint | yamllint 1.35.1 | All compose, Prometheus, Grafana, Airflow, and GE config files |
| Compose Validate | `docker compose config` | Validates all 13 `docker-compose.*.yml` files against `.env.example` |
| Python Lint | ruff 0.4.4 | DAGs, scripts, services, governance — target: py311 |
| dbt Compile | dbt-clickhouse 1.10.0 | `dbt parse` + `dbt compile` without a live database; validates all model SQL |
| Integration Tests | pytest + docker-compose.test.yml | ClickHouse :18123 + PostgreSQL :15432 test containers; end-to-end SaaS pipeline |
| Data Quality | Great Expectations 0.18 | Seeds ClickHouse test container; runs `datalake_checkpoint` |
| Python SAST | Bandit 1.7.9 | HIGH severity security issues in DAGs, scripts, services |
| Dependency Audit | pip-audit 2.7.3 | Audits both `requirements-dev.txt` and `services/ai-agent/requirements.txt` |
| Container Scan | Trivy | Dockerfile misconfiguration + filesystem CVE scan (HIGH/CRITICAL only) |
| SQL Lint | sqlfluff 3.0.7 | dbt models; ClickHouse dialect; dbt Jinja templater |

### Deploy Workflow (`deploy.yml`)

Triggers on push to `main` when `services/ai-agent/**` changes.

1. **Build + push** Docker image to GHCR (`ghcr.io/ki957/datalake-ai-agent`) with SHA tag and `latest`; Docker layer caching via GitHub Actions cache
2. **Build React bundle** — `npm ci && npm run build` in parallel with Docker build
3. **Smoke test** — pull the pushed image, start container, `GET /api/health`
4. **Deploy** — SSH deploy section is present but commented out; uncomment with `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY` secrets

### dbt Docs Workflow (`dbt-docs.yml`)

Triggers on changes to `transformation/dbt/datalake_transforms/models/**`. Generates and publishes dbt documentation to GitHub Pages.

### Pre-commit Hooks

Install once with `pre-commit install`. Runs automatically on every `git commit`.

| Hook | Version | What it enforces |
|---|---|---|
| pre-commit-hooks | 4.6.0 | Trailing whitespace, EOF newline, YAML/JSON validity, merge conflict markers, 500 KB file size limit, no direct commits to `main`/`master` |
| detect-secrets | 1.4.0 | Credential scan against `.secrets.baseline` |
| ruff | 0.4.4 | Python auto-fix linting across `orchestration/`, `scripts/`, `services/`, `governance/` |
| yamllint | 1.35.1 | YAML formatting (200-char line limit) |
| sqlfluff | 3.0.7 | SQL style enforcement for all dbt models (ClickHouse dialect) |

---

## Project Structure

```text
enterprise-datalake/
├── infrastructure/
│   ├── docker/                   # 13 docker-compose.*.yml layers
│   ├── clickhouse/               # performance.xml, prometheus.xml
│   ├── grafana/                  # 3 pre-built dashboards + provisioning
│   ├── kafka/                    # debezium-connector-mysql.json
│   ├── prometheus/               # prometheus.yml + datalake_alerts.yml
│   ├── loki/ promtail/           # Log aggregation config
│   ├── keycloak/ vault/          # Identity + secrets config
│   └── superset/superset_config.py
├── orchestration/
│   └── airflow/dags/
│       ├── shopflow_pipeline.py  # Main ShopFlow DAG
│       ├── saas_pipeline.py      # Main SaaS DAG
│       ├── ingestion_dags/       # airbyte_health
│       ├── streaming_dags/       # streaming_enrichment
│       ├── transformation_dags/  # dbt_runner, unified_profile, spark_profiler
│       ├── quality_dags/         # data_quality_suite, auto_contract
│       └── governance_dags/      # schema_evolution, metadata_sync
├── transformation/
│   ├── dbt/datalake_transforms/
│   │   └── models/
│   │       ├── bronze/           # sources.yml (S3 view sources — no SQL models)
│   │       ├── silver/           # stg_customers, stg_orders, stg_products, ...
│   │       ├── staging/          # stg_users, stg_events, stg_subscriptions
│   │       ├── gold/             # dim_*, fct_*
│   │       ├── marts/            # cross-domain KPI models + agent-generated models
│   │       └── semantic_models/  # MetricFlow metrics.yml + time spine
│   └── spark/jobs/               # unified_customer_profile.py, minio_profiler.py
├── services/
│   ├── ai-agent/
│   │   ├── agents/               # 11 specialist LangGraph agents
│   │   ├── tools/                # 10 tool modules
│   │   ├── graph/pipeline_graph.py  # LangGraph supervisor + keyword router
│   │   ├── rag/                  # ChromaDB GraphRAG-lite
│   │   ├── memory/               # PostgreSQL memory + cost tracker + audit store
│   │   └── server.py             # FastAPI application entry point
│   ├── ai-agent-v2/frontend/     # React 18 + Vite + Tailwind + Zustand
│   ├── sap-api/                  # Flask SAP OData simulator
│   ├── spark/jobs/               # PySpark jobs
│   └── streaming/                # Kafka enrichment consumer
├── governance/
│   ├── great_expectations/
│   │   ├── expectations/         # 6 expectation suites
│   │   └── checkpoints/          # datalake_checkpoint.yml
│   └── openmetadata/ingestion-configs/
├── tests/
│   ├── integration/              # ClickHouse + SaaS end-to-end tests
│   ├── test_dag_integrity.py
│   ├── test_data_generators.py
│   └── test_saas_pipeline_logic.py
├── scripts/                      # Data generators, setup scripts, ingest scripts
├── docs/
│   ├── PROJECT_DOCUMENTATION.md  # Full technical documentation
│   └── adr/                      # ADR-001 (ClickHouse), ADR-002 (Groq), ADR-003 (TLS)
├── Makefile
├── .env.example
├── .pre-commit-config.yaml
└── pytest.ini
```

---

## Contributing & Local Dev

**1. Install prerequisites**
```bash
# Python 3.11, Docker with Compose v2 plugin
pip install -r requirements-dev.txt
pip install pre-commit && pre-commit install
```

**2. Configure environment**
```bash
cp .env.example .env
# Required: GROQ_API_KEY
# All other values have working defaults for local development
```

**3. Run tests**
```bash
make test              # Unit tests — no services needed
make test-integration  # Spins up test containers, runs end-to-end suite, tears down
```

**4. AI Agent smoke test**
```bash
cd services/ai-agent && python test_agents.py
# Sends 4 test questions, expects score ≥ 70/100
```

**5. dbt locally** — requires `~/.dbt/profiles.yml`:
```yaml
datalake_transforms:
  target: dev
  outputs:
    dev:
      type: clickhouse
      host: localhost
      port: 8123
      user: default
      password: ""        # set to CLICKHOUSE_DEFAULT_PASSWORD from .env
      schema: default
      secure: false
```

```bash
cd transformation/dbt/datalake_transforms
dbt compile --no-version-check --target-path /tmp/dbt_target
dbt run --select gold --target-path /tmp/dbt_target
```

> Use `--target-path /tmp/dbt_target` when running as non-root — the default `target/` directory may be root-owned inside Docker volume mounts.

The pre-commit `no-commit-to-branch` hook blocks direct commits to `main` and `master`. Work on feature branches.

---

## Documentation

- **Full technical documentation:** [`docs/PROJECT_DOCUMENTATION.md`](docs/PROJECT_DOCUMENTATION.md)
- **Architecture Decision Records:** [`docs/adr/`](docs/adr/) — ADR-001: ClickHouse selection, ADR-002: Groq LLM, ADR-003: TLS strategy
- **AI agent dependency graph:** [`graphify-out/graph.html`](graphify-out/graph.html) — interactive knowledge graph of the codebase
- **dbt docs:** auto-published to GitHub Pages via [`dbt-docs.yml`](.github/workflows/dbt-docs.yml); served locally at `http://localhost:8082` after `make transform`
