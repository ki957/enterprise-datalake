# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A modular, Docker Compose-based Enterprise Data Lake. Two data domains run through a unified medallion architecture:

- **ShopFlow** — e-commerce: MySQL CDC → Airbyte → MinIO → ClickHouse → dbt silver/gold
- **SaaS** — subscriptions: PostgreSQL → Airflow extract → ClickHouse raw → dbt staging/marts
- **AI Layer** — LangGraph multi-agent system (11 specialist agents) with React UI, cost tracking, and CI/CD deploy pipeline

The project uses no cloud services. Everything runs locally via Docker Compose + host processes.

## Common Commands

```bash
# Always run base first — creates Docker networks and volumes
make base

# Layer-by-layer bring-up (order matters)
make security      # Vault + Keycloak
make storage       # MinIO
make transform     # ClickHouse + Spark + dbt-docs nginx
make serving       # PostgreSQL
make governance    # Airflow (custom image: Dockerfile.airflow)
make observability # Prometheus + Grafana + Alertmanager + Loki
make visualization # Superset + Redis
make sources       # MySQL (ShopFlow CDC) + SAP API Flask :5001
make streaming     # Kafka (KRaft) + Kafka Connect (Debezium)
make all           # Full stack in dependency order (includes streaming)

# AI Agent v2 (React + FastAPI — the active UI)
make ui            # Installs deps, starts FastAPI :8502 + Vite :3001

# CI/CD deploy (builds Docker image → GHCR + React bundle)
make deploy

# Bronze ingestion path (no Airbyte k8s needed)
make bronze-init         # MySQL + HTTP → MinIO Parquet → ClickHouse S3 views
make bronze-init-small   # Fast dev dataset

# Seeding
make seed-mysql-small    # 500 customers / 200 products / 2000 orders
make seed-mysql          # 50K customers / 5K products / 500K orders
make seed-saas-small     # 500 users / 5000 events / 500 subscriptions
make seed-saas           # Production-scale SaaS data
make seed-all            # Both domains at production scale

# One-time setup (run after relevant layer is up)
make setup-vault         # Write pipeline credentials to Vault
make setup-keycloak      # Create Grafana + Superset OIDC clients in datalake realm
make setup-superset      # ClickHouse datasource + dashboards; exports ZIP to git
make setup-debezium      # Register Debezium MySQL connector (idempotent; run after make streaming)

# Spark jobs (run on host against spark://localhost:7077)
make spark-unified       # Build gold.unified_customers from all domain sources
make spark-profile       # Profile MinIO Parquet files → summary stats

# Superset dashboard state (git-tracked)
make export-superset     # Export dashboards → infrastructure/superset/dashboards/ (then commit)
make import-superset     # Import dashboards from git ZIP into running Superset

# Maintenance
make apply-ttl           # Apply ClickHouse TTL retention policies
make clean               # docker system prune -f (stops containers, preserves volumes)

# Operations
make ps / make logs / make ram / make health
make down            # Stop, preserve volumes
make nuke            # Full reset including volumes (prompts)
```

### Pre-commit hooks

```bash
pip install pre-commit
pre-commit install          # wire into git
pre-commit run --all-files  # run manually
```

Hooks: trailing whitespace, YAML/JSON validation, no-commit-to-main/master, `detect-secrets` (baseline at `.secrets.baseline`), `ruff` Python linter. The secrets baseline must be updated when intentional credential placeholders are added: `detect-secrets scan > .secrets.baseline`.

### Testing

```bash
pip install -r requirements-dev.txt

# Unit tests — no services needed
pytest tests/test_data_generators.py tests/test_saas_pipeline_logic.py -v
pytest tests/test_saas_pipeline_logic.py::test_ch_insert_serialisation -v   # single test

# DAG integrity
pip install apache-airflow==2.8.0
pytest tests/test_dag_integrity.py -v

# Integration tests — spins up isolated ClickHouse :18123 + PostgreSQL :15432
make test-integration

# AI Agent smoke test (4 questions, scored 0-100, PASS ≥ 70)
cd services/ai-agent && python test_agents.py
```

### dbt Commands (run on host, not in container — targets localhost:8123)

```bash
cd transformation/dbt/datalake_transforms
dbt run --select marts          # Agent-generated models (models/marts/)
dbt run --select gold           # Gold dimensions + facts
dbt run --select silver staging # Both staging layers
dbt compile --select <model> --no-version-check --target-path /tmp/dbt_target
dbt parse --no-version-check    # Syntax check only, no DB — used in CI
dbt docs generate               # Rebuild docs at :8082
```

`DBT_TARGET_PATH=/tmp/dbt_target` must be set when running dbt as non-root — the default `target/` directory can be root-owned inside Docker mounts.

**Required `~/.dbt/profiles.yml`:**
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

## Service Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| AI Agent v2 (React) | http://localhost:3001 | — (mobile: `http://<PC-IP>:3001`) |
| AI Agent API (FastAPI) | http://localhost:8502 | — |
| Airflow | http://localhost:8080 | admin / Airflow@2024 |
| Superset | http://localhost:8088 | admin / Superset@2024 |
| Grafana | http://localhost:3000 | admin / Grafana@2024 or Keycloak SSO |
| ClickHouse HTTP | http://localhost:8123 | default / Click@2024 |
| ClickHouse Native | localhost:9002 | default / Click@2024 |
| MinIO Console | http://localhost:9001 | admin / Minio@2024 |
| Keycloak | http://localhost:8180 | admin / (KEYCLOAK_ADMIN_PASSWORD from .env) |
| Vault | http://localhost:8200 | token: root (dev mode) |
| PostgreSQL | localhost:5432 | postgres / Postgres@2024 |
| MySQL | localhost:3306 | root / MySQL@2024 |
| Prometheus | http://localhost:9090 | — |
| Spark Master | http://localhost:8081 | — |
| dbt Docs | http://localhost:8082 | — |
| Kafka | localhost:9092 | — |
| Kafka Connect (Debezium) | http://localhost:8083 | — |

`Postgres@2024` → `Postgres%402024` in SQLAlchemy connection strings. The governance compose hardcodes `%40` directly — never substitute `${POSTGRES_PASSWORD}` in connection URLs.

## Architecture

### Data Flow

```
MySQL (shopflow CDC)  ──┐
SAP OData API :5001   ──┼── Airbyte (k8s) ──► MinIO raw/airbyte/{mysql,sap,rest}/
JSONPlaceholder REST  ──┘   OR make bronze-init (direct ingest, no Airbyte)

MySQL (shopflow CDC)  ──► Debezium (Kafka Connect) ──► Kafka topic dbz.shopflow.*
                                                        └──► streaming_enrichment DAG
                                                             (Groq AI enrichment → raw.shopflow_products_enriched)

MinIO raw/ ──► bronze.src_* (ClickHouse s3() views, created by setup_clickhouse_bronze.py)
           ──► dbt silver/ ──► staging.*
           ──► dbt gold/   ──► gold.{dim_*, fct_*}

PostgreSQL (saas_users/events) ──► Airflow DAG extract ──► raw.* ──► dbt staging/marts ──► gold.*

gold.* ──► Superset dashboards
       ──► Grafana panels
       ──► AI Agent (queries via clickhouse_tools.py)
       ──► Spark (unified_customer_profile → gold.unified_customers)
```

### ClickHouse Schema Layers

| Schema | Layer | How Created |
|--------|-------|-------------|
| `bronze` | S3 views over MinIO Parquet | `setup_clickhouse_bronze.py` — **not dbt** |
| `staging` | Silver + SaaS staging tables | dbt `models/silver/` + `models/staging/` |
| `gold` | Dimensions, facts, marts | dbt `models/gold/` + `models/marts/` |
| `raw` | SaaS extracts | Airflow `saas_pipeline.py` direct inserts |

`models/bronze/` and `models/raw/` contain only `sources.yml` definitions — no SQL models. The bronze layer is always imperatively managed.

### dbt Model Folder → ClickHouse Schema

Controlled by `dbt_project.yml` + `macros/generate_schema_name.sql` (custom macro — uses schema as-is, no target prefix):

| Folder | Schema | Engine |
|--------|--------|--------|
| `models/silver/` | `staging` | `MergeTree()` |
| `models/gold/dimensions/` | `gold` | `ReplacingMergeTree()` — SCD Type 2 |
| `models/gold/facts/` | `gold` | `MergeTree()` — append-only |
| `models/staging/` | `staging` | inline `config(schema='staging')` |
| `models/marts/` | `gold` | inline `config(schema='gold')` |

Agent-generated models land in `models/marts/` with a `-- generated by nl_dbt_agent` marker. The tool blocks overwriting files without this marker.

### dbt Semantic Layer

`models/semantic_models/metrics.yml` defines MetricFlow-compatible metrics. Key constraints:
- `metricflow_time_spine.sql` must exist (in `models/semantic_models/`) — generates date spine 2020–2030
- Dimension names must not use reserved MetricFlow keywords (`month`, `year`, etc.) — use `expr:` to alias
- Ratio metrics must reference **metric** names (e.g. `revenue`, `active_customers`), not **measure** names (`total_revenue`, `total_customers`)

### AI Agent Architecture (`services/ai-agent/`)

**Process model**: FastAPI (`server.py`) runs on the **host** at `:8502`. The legacy Streamlit Docker container (`make ai-agent`) is deprecated — use `make ui` instead.

**11 specialist agents** — each is a `create_react_agent` (LangGraph) with ≤4 tools (Groq function-calling limit):

| Agent | File | Trigger keywords | Unique capability |
|-------|------|-----------------|-------------------|
| Insight | `insight_agent.py` | (default) | ClickHouse SQL + Plotly charts |
| Schema | `schema_agent.py` | schema, column, describe | Reads raw `.sql` model files |
| Quality | `quality_agent.py` | quality, null, duplicate | Great Expectations integration |
| Orchestration | `orchestration_agent.py` | airflow, dag, failed | Airflow REST API |
| Ingestion | `ingestion_agent.py` | minio, ingest, parquet | MinIO file inspection |
| Performance | `performance_agent.py` | slow, optimize, storage | ClickHouse system tables |
| Airbyte | `airbyte_agent.py` | airbyte, connector | OAuth2 token cached 55 min |
| Self-Healing | `self_healing_agent.py` | self heal, auto fix | Restarts failed Airflow tasks |
| Anomaly | `anomaly_agent.py` | anomaly detection, trend | Contextual vs threshold detection |
| NL→dbt | `nl_dbt_agent.py` | generate model, create dbt | Writes + compiles dbt SQL |
| Contract | `contract_agent.py` | expectation, contract | Profiles tables, writes GE suites |

Routing is keyword-scored in `graph/pipeline_graph.py::_route()`. Intent-specific agents (self_healing, anomaly, nl_dbt, contract) are weighted 2–3× so one keyword match beats multiple generic matches. Tie-breaking follows `_AGENT_PRIORITY` list order. Airbyte check runs before Ingestion since keywords overlap.

**Routing hints** (`server.py::ROUTING_HINTS`): When a specific agent is selected in the UI (not auto-routing), the server prepends a short topic label to the message before passing it to the agent (e.g. `"data contract: "` for contract). Keep these short and neutral — a long prescriptive hint forces the LLM into a specific workflow for every question, breaking creative/conceptual questions. Never use workflow verbs in hints.

**Agent question-type detection**: Agents that handle both technical and creative questions need a `STEP 0` at the top of their system prompt — detect intent signals (e.g. "clauses", "legal", "story", "analogy") before any tool-call workflow. Without this, the routing hint can push the LLM into tool-call mode even for conceptual questions.

**Unavailable-service fallback pattern**: When a backend service has no credentials or is unreachable, the tool itself should return a rich structured string (not a bare exception message) that contains all static knowledge the LLM needs to give a full answer. See `tools/airbyte_tools.py::_no_credentials_msg()` as the reference implementation.

**Response cache**: `server.py` caches responses for 5 minutes keyed on `hash(agent:message)`. Identical questions asked within that window skip the LLM. Clear the cache (and force RAG re-seed) via `POST /api/rag/reload` — also called automatically by Airflow after schema changes.

**RAG**: ChromaDB in `chroma_db/` (docker volume `docker_ai-agent-chroma`). One document per table + SQL pattern documents. Distance-threshold gating prevents irrelevant injection. Re-seeded when doc hash changes.

**Memory & Cost tracking** — all stored in the `airflow` PostgreSQL DB:
- `ai_agent_memory` — conversation history per session
- `agent_cost_log` — token counts + USD cost per call (Groq pricing: input $0.11/M, output $0.34/M)
- `audit_store` — every `create_dbt_model` write and Self-Healing action is audited

**Groq LLM constraints** (enforced in `agents/base.py`):
- Model: `meta-llama/llama-4-scout-17b-16e-instruct` — do not change
- `streaming=False` — streaming causes intermittent "Failed to call a function" on Groq
- ≤4 tools per agent — `llama-3.3-70b-versatile` and older models break with 5+
- Zero-argument tools break Groq's JSON schema generation — always add at least one optional param
- Jinja `{{ }}` in JSON tool call parameters causes parse errors — `dbt_write_tools.py` strips Jinja from SQL and injects it server-side
- Daily TPD quota errors (HTTP 429 + "tokens per day") are caught in `pipeline_graph.py` and surfaced as a clean user-facing message with the reset wait time — no retry, no fallback

**NL→dbt tool safety** (`tools/dbt_write_tools.py`):
- LLM writes plain SQL using `gold.table_name` style — `_normalise_sql()` converts to `{{ ref() }}` automatically
- `_CONFIG_BLOCK` is prepended by the tool, never by the LLM
- `create_dbt_model` only writes to `models/marts/` — never touches silver/gold/staging folders

### Frontend (`services/ai-agent-v2/frontend/`)

React 18 + Vite + Tailwind + Zustand + Plotly. Vite dev server binds `0.0.0.0:3001`; proxies `/api/*` → `:8502`.

Key files: `src/store/index.js` (all global state), `src/hooks/useChat.js` (SSE parsing), `src/data/agents.js` (agent metadata), `src/components/ui/CostDashboard.jsx` (lazy-loaded Plotly cost charts).

`CostDashboard` is lazy-loaded via `React.lazy()` — Plotly's 4.8 MB chunk only downloads when the user opens it.

**PWA**: service worker at `public/sw.js` — cache-first for static shell, network-only for `/api/*`. Manifest shortcuts: `/?agent=insight` and `/?costs=1` (handled in `App.jsx` `useEffect`).

### Airflow DAGs (`orchestration/airflow/dags/`)

11 DAGs, all active. Key architectural notes:
- DAGs are loaded from the **host repo mount** — file edits take effect without container restart
- `AIRFLOW__API__AUTH_BACKENDS` must include `basic_auth` — without it all Orchestration Agent API calls return 401
- `saas_pipeline.py::_ch_insert()` serialises rows as TSV: `None→\N`, strips microseconds, casts Decimal to float string, replaces tabs in values

| DAG file | Schedule | Purpose |
|----------|----------|---------|
| `saas_pipeline.py` | daily 02:00 | PostgreSQL extract → ClickHouse raw → dbt staging/marts |
| `shopflow_pipeline.py` | daily 06:00 | Triggers Airbyte syncs → waits for MinIO files → dbt silver/gold → Superset refresh |
| `transformation_dags/dbt_runner.py` | daily 07:00 | Full dbt run (all layers) + test |
| `transformation_dags/spark_profiler.py` | daily 07:30 | MinIO Parquet profiling via Spark |
| `transformation_dags/unified_profile.py` | daily 08:00 | Spark unified customer profile → `gold.unified_customers` |
| `governance_dags/metadata_sync.py` | daily 08:00 | Publish dbt docs, log pipeline metadata, check schema drift |
| `governance_dags/schema_evolution_dag.py` | daily 08:30 | Detect MySQL schema changes → auto-update `sources.yml` → validate dbt compile → reload RAG |
| `quality_dags/data_quality_suite.py` | daily 09:00 | Run Great Expectations checkpoint (`governance/great_expectations/`) |
| `quality_dags/auto_contract_dag.py` | weekly | Profile gold tables → write GE expectation suites |
| `streaming_dags/streaming_enrichment_dag.py` | every 5 min | Consume Kafka `dbz.shopflow.products` → Groq AI enrichment → `raw.shopflow_products_enriched` |
| `ingestion_dags/airbyte_health.py` | hourly | Poll Airbyte connector status; alert on failures |

The schema evolution DAG rolls back `sources.yml` changes if `dbt compile` fails; removed columns are only logged (never auto-removed).

### Keycloak SSO

Realm: `datalake`. Run `make setup-keycloak` once after Keycloak is up.

Grafana OAuth2: auth URL uses `localhost:8180` (browser redirect); token/userinfo use `keycloak:8080` (server-side via `frontend-net`). Superset uses `KeycloakSecurityManager` subclass in `infrastructure/superset/superset_config.py` — maps `preferred_username`/`email` from userinfo.

### Vault

Dev mode (`token: root`). Airflow reads all connections/variables from Vault via `VaultBackend`. Secret paths: `secret/data/airflow/connections/{clickhouse_default,minio_s3,postgres_default}`.

### Airbyte

Runs in k8s via `abctl` — not Docker Compose. `make ingestion` is nginx proxy only. From k8s pods, Docker services are at `172.17.0.1` (bridge gateway). Airbyte agent uses OAuth2 (`POST /api/v1/applications/token`); all list endpoints are POST, not GET.

### CI/CD (`github/workflows/`)

**`ci.yml`** — 12 jobs on every push/PR to main/master: secret scan → unit tests → YAML lint → compose validate → Python lint → dbt compile → integration tests → data quality → Bandit SAST → pip-audit → Trivy → SQL lint.

**`deploy.yml`** — triggers on push to `main` when `services/ai-agent/**` changes: builds Docker image → GHCR, builds React bundle (parallel), smoke-tests the image. Deploy-to-server section is commented; uncomment with `DEPLOY_HOST/DEPLOY_USER/DEPLOY_SSH_KEY` secrets.

## Critical Non-Obvious Constraints

- **ClickHouse port**: host = `localhost:9002` (native driver). Inside Docker = `clickhouse:9000`. `clickhouse_tools.py` defaults to `localhost:9002`.
- **No LAG/LEAD in ClickHouse SQL** — use self-join on `addMonths(dt, 1)` for period-over-period
- **No concat() with Decimal** — cast first: `toString(round(amount, 2))`
- **Date truncation**: `toStartOfMonth()` not `DATE_TRUNC`. Filter: `is_current = 1` not `= true`
- **dbt compile target path**: always pass `--target-path /tmp/dbt_target` to avoid permission errors on root-owned `target/`
- **Streamlit is legacy** — `make ai-agent` still works but the active development path is `make ui` (FastAPI + React). The Dockerfile now runs `server.py`, not `app.py`

### Governance (`governance/`)

- `great_expectations/` — GE 0.18 checkpoint suite targeting ClickHouse via `clickhouse-sqlalchemy`. Run via `governance/great_expectations/run_checkpoint.py` (called by `data_quality_suite` DAG). Exits with code 1 on failure so Airflow marks the task failed.
- `openmetadata/ingestion-configs/` — YAML configs for OpenMetadata metadata ingestion (not wired to a running OM instance by default; used for documentation purposes).
- `adr/` — Architecture Decision Records.

## Industry-Impact Improvement Opportunities

Areas where the project can be elevated to production-grade:

1. **MCP Server** (`services/mcp-server/`) — expose ClickHouse query, dbt model creation, and contract tools via Model Context Protocol. Existing `@tool` functions are 90% of the work. Enables Claude Desktop / Cursor to drive the datalake directly.

2. **Retrieval-Augmented Analytics** — extend Insight Agent to embed customer/product vectors in ClickHouse (vector support is native in v24+), enabling hybrid queries: *"customers similar to top 10 spenders"* = vector similarity + SQL aggregation merged.

3. **Auto-contracts from query traffic** — log which columns are actually queried against gold tables over 24h, infer data contracts from real usage, flag columns with no consumers as candidates for deprecation.
