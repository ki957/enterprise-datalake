# Enterprise Data Lake — Complete Technical Documentation

> **Audience:** Data engineers, analytics engineers, platform engineers, and students learning modern data stack architecture.  
> **Purpose:** Comprehensive reference covering *what* every component does, *why* it was chosen over alternatives, and *where* it lives in the codebase and data flow.

---

## Table of Contents

1. [Project Purpose and Philosophy](#1-project-purpose-and-philosophy)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Technology Stack Decision Matrix](#3-technology-stack-decision-matrix)
4. [Infrastructure Layer — Docker and Networking](#4-infrastructure-layer--docker-and-networking)
5. [Data Sources Layer](#5-data-sources-layer)
6. [Ingestion Layer — Airbyte and Custom Connectors](#6-ingestion-layer--airbyte-and-custom-connectors)
7. [Storage Layer — MinIO Object Store](#7-storage-layer--minio-object-store)
8. [The Medallion Architecture — Bronze, Silver, Gold](#8-the-medallion-architecture--bronze-silver-gold)
9. [Transformation Layer — ClickHouse and dbt](#9-transformation-layer--clickhouse-and-dbt)
10. [Orchestration Layer — Apache Airflow](#10-orchestration-layer--apache-airflow)
11. [Serving and Visualization Layer](#11-serving-and-visualization-layer)
12. [Security Layer — Vault and Keycloak](#12-security-layer--vault-and-keycloak)
13. [Observability Layer — Prometheus and Grafana](#13-observability-layer--prometheus-and-grafana)
14. [AI Agent Layer — LangGraph and Groq](#14-ai-agent-layer--langgraph-and-groq)
15. [Data Modeling Deep Dive](#15-data-modeling-deep-dive)
16. [End-to-End Data Lineage](#16-end-to-end-data-lineage)
17. [Deployment Runbook](#17-deployment-runbook)
18. [Design Decisions and Alternatives Considered](#18-design-decisions-and-alternatives-considered)

---

## 1. Project Purpose and Philosophy

### What This Project Is

This is a **fully self-contained, production-pattern Enterprise Data Lake** built entirely on open-source software. It ingests data from three heterogeneous source systems, stores raw data in an object store, transforms it through a staged medallion architecture, and serves analytics through a BI tool — all orchestrated by a workflow engine and observable through a monitoring stack.

Additionally, it includes an **AI-powered conversational agent** that allows natural-language interaction with the pipeline itself — triggering syncs, querying data quality, executing SQL, and diagnosing failures.

### Two Data Domains

The system models two business domains to demonstrate that a real-world data lake always integrates multiple operational systems:

| Domain | Description | Source Systems |
|--------|-------------|----------------|
| **ShopFlow** | Operational e-commerce | MySQL (transactional), SAP OData API (procurement), JSONPlaceholder REST API (reviews) |
| **SaaS** | Subscription analytics | PostgreSQL (user events and subscriptions) |

### Core Design Philosophy

**ELT, not ETL.** Data is extracted and loaded into the lake *as-is*, then transformed inside the analytical engine (ClickHouse) using dbt. This is the modern standard because:
- Raw data is preserved permanently (replayable)
- Transformations are versioned in Git (dbt models)
- The analytical engine (ClickHouse) is orders of magnitude faster than ETL middleware for complex transforms

**Layered deployment.** The stack is split into composable layers (security, storage, ingestion, transform, serving, governance, observability, visualization). Each layer can be rebuilt independently without tearing down the whole stack.

**Infrastructure as code.** Every service, network, volume, credential, and configuration is defined in files committed to version control. There are no manual steps except initial one-time setups, which are themselves scripted.

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                       │
│                                                                             │
│  MySQL 8.0 (shopflow)    SAP OData API (Flask)    JSONPlaceholder REST      │
│  CDC binlog enabled      50 vendors, 300 POs       /users /posts /comments  │
│  Port 3306               Port 5001                 Public API               │
└──────────────┬───────────────────┬─────────────────────────┬────────────────┘
               │                   │                         │
               ▼                   ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INGESTION LAYER                                    │
│                                                                             │
│  Airbyte (Kubernetes)              Python + PyArrow (Airflow Tasks)         │
│  MySQL CDC → Parquet               SAP API paginate → Parquet               │
│  Connection ID: 9993f6c9...        JSONPlaceholder → Parquet                │
│                                                                             │
│  All output written to MinIO raw bucket under airbyte/{source}/{table}/     │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    OBJECT STORAGE LAYER (MinIO)                             │
│                                                                             │
│  Bucket: raw/                        Bucket: silver/    Bucket: gold/       │
│  airbyte/mysql/customers/*.parquet   (reserved for      (reserved for       │
│  airbyte/mysql/orders/*.parquet       future Spark)      future exports)    │
│  airbyte/mysql/products/*.parquet                                           │
│  airbyte/sap/vendors/*.parquet                                              │
│  airbyte/sap/purchase_orders/*.parquet                                      │
│  airbyte/rest/users/*.parquet                                               │
│  airbyte/rest/posts/*.parquet                                               │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ ClickHouse S3() table function
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER (ClickHouse + dbt)                  │
│                                                                             │
│  BRONZE schema (S3 views)     STAGING schema (dbt silver)                   │
│  src_mysql_customers  ──────► stg_customers                                 │
│  src_mysql_orders     ──────► stg_orders                                    │
│  src_mysql_products   ──────► stg_products                                  │
│  src_sap_vendors      ──────► stg_vendors                                   │
│  src_sap_purchase_orders ───► stg_purchase_orders                           │
│  src_rest_posts       ──────► stg_reviews                                   │
│                                                                             │
│  GOLD schema (dbt gold)                                                     │
│  dim_customers (SCD Type 2, ReplacingMergeTree)                             │
│  dim_products  (ReplacingMergeTree)                                         │
│  dim_vendors   (ReplacingMergeTree)                                         │
│  fct_orders    (MergeTree, append-only)                                     │
│  fct_procurement (MergeTree, append-only)                                   │
│  fct_reviews   (MergeTree, append-only)                                     │
│                                                                             │
│  SaaS: raw.* → staging.stg_* → gold.dim_users / gold.fct_*                 │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
               ┌───────────────────────┼───────────────────────┐
               ▼                       ▼                       ▼
┌──────────────────┐     ┌─────────────────────┐   ┌──────────────────────┐
│  VISUALIZATION   │     │   ORCHESTRATION      │   │   AI AGENT           │
│                  │     │                     │   │                      │
│  Apache Superset │     │  Apache Airflow     │   │  Streamlit + Groq    │
│  Port 8088       │     │  Port 8080          │   │  Port 8501           │
│  Redis cache     │     │  shopflow_datalake  │   │  LangGraph routes    │
│  Gold layer      │     │  _pipeline DAG      │   │  7 specialist agents │
│  dashboards      │     │  Daily 06:00 UTC    │   │  RAG + memory        │
└──────────────────┘     └─────────────────────┘   └──────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    PLATFORM LAYER                                            │
│  Security: Vault (secrets) + Keycloak (SSO/OIDC)                            │
│  Observability: Prometheus (metrics) + Grafana (dashboards)                 │
│  Governance: Great Expectations (data quality) + OpenMetadata (catalog)     │
│  Spark: Distributed compute (available for batch transformations)            │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Technology Stack Decision Matrix

This section explains every technology choice: what it is, why it was chosen, and what alternative was considered.

### 3.1 ClickHouse — OLAP Database

**What it is:** A column-oriented database management system (DBMS) designed for real-time analytical queries over large datasets. Unlike row-oriented databases (PostgreSQL, MySQL), ClickHouse stores each column separately, enabling extremely fast aggregations and range scans.

**Why it was chosen:**
- **Speed:** Column storage compresses aggressively and reads only needed columns. A `SELECT sum(revenue) FROM fct_orders` reads one column from disk, not all rows.
- **SQL compatibility:** Full ANSI SQL with extensions (window functions, CTEs, array functions).
- **dbt adapter:** `dbt-clickhouse` is production-grade, enabling the same dbt workflow used with Snowflake or BigQuery.
- **Open source and self-hosted:** No per-query billing. Runs inside Docker with a single container.
- **MergeTree family engines:** ClickHouse's table engines (MergeTree, ReplacingMergeTree, SummingMergeTree) directly solve data warehouse patterns like SCD Type 2 and append-only facts.
- **S3 integration:** The `s3()` table function reads Parquet files from MinIO directly, without an ETL copy step — this powers the entire bronze layer.

**Where it lives:** `docker-compose.transform.yml` → `clickhouse` service. Ports: 8123 (HTTP/REST), 9002 (native protocol, mapped from internal 9000).

**Alternative considered:** Apache Druid — more complex to operate (requires ZooKeeper + Kafka), no native dbt adapter.

---

### 3.2 Apache Airflow — Workflow Orchestration

**What it is:** A platform for programmatically authoring, scheduling, and monitoring data pipelines as Directed Acyclic Graphs (DAGs) written in Python.

**Why it was chosen:**
- **Python-native:** Tasks are Python functions. Ingesting SAP API, writing Parquet to MinIO, querying ClickHouse, and running dbt all happen inside PythonOperators — no need for external scripts or DSLs.
- **LocalExecutor:** Sufficient for this stack size without Kubernetes or Celery complexity.
- **XCom:** Native inter-task communication (e.g., passing Airbyte job_id from trigger → status check task).
- **Built-in retry logic:** `retries=2, retry_delay=timedelta(minutes=3)` handles transient failures without custom code.
- **Rich UI:** Task-level log inspection, manual DAG triggering, execution history — essential for debugging.

**Where it lives:** `infrastructure/docker/docker-compose.governance.yml` → `airflow-webserver`, `airflow-scheduler`, `airflow-init` containers. Custom image defined in `infrastructure/docker/Dockerfile.airflow`. DAGs in `orchestration/airflow/dags/`.

**Why a custom Docker image (`Dockerfile.airflow`):** The official Airflow image has Python and the Airflow runtime, but lacks dbt-clickhouse, pyarrow, and minio. Rather than installing these at runtime (slow, non-reproducible), they are baked into a custom image extending the official one.

**Alternative considered:** Prefect — excellent Python SDK, but less mature ecosystem for dbt integration at this scale.

---

### 3.3 Airbyte — Data Integration Platform

**What it is:** An open-source ELT platform with a catalog of 300+ pre-built connectors. It handles incremental syncs, schema evolution, and writes normalized output to destinations like MinIO (S3).

**Why it was chosen:**
- **MySQL CDC out of the box:** Airbyte's MySQL source connector reads from the binary log (binlog) using Debezium under the hood, capturing inserts, updates, and deletes without polling the source.
- **Parquet output:** The S3-compatible destination writes Parquet files, the industry standard for columnar lake storage.
- **No-code connector configuration:** JSON-defined connections with UI for non-engineers.
- **Incremental sync modes:** `incremental_append` + `append_dedup` on the destination side — only new/changed rows are synced each run, minimizing data transfer.

**Why Kubernetes (`abctl`):** Airbyte's architecture spawns Docker containers per sync job (connector pods). In Docker Compose mode this causes networking conflicts. Airbyte's official CLI (`abctl`) deploys to a local Kubernetes cluster via `kind`, which handles pod lifecycle properly.

**Critical networking detail:** From inside Kubernetes pods, the Docker bridge gateway (`172.17.0.1`) is used to reach Docker Compose services (MySQL, MinIO). `localhost` from a k8s pod refers to the pod itself, not the host machine.

**Where it lives:** Deployed separately via `abctl`, not in Docker Compose. The `make ingestion` target only starts an nginx proxy (`localhost:8000`) that forwards to the Airbyte k8s service. Connection configs stored in `ingestion/airbyte/connection-configs/`.

**Alternative considered:** Fivetran — fully managed but costly; no self-hosted option at this scale.

---

### 3.4 MinIO — Object Storage (S3-Compatible)

**What it is:** A high-performance, Kubernetes-native object storage system fully compatible with the Amazon S3 API. Files are stored as objects in named buckets, accessed via HTTP.

**Why it was chosen:**
- **S3 API compatibility:** Every tool that works with AWS S3 (Airbyte, ClickHouse, PyArrow, Spark) works identically with MinIO — no code changes required when migrating to/from cloud.
- **Self-hosted:** Runs in a single Docker container; no cloud account required.
- **ClickHouse S3 integration:** ClickHouse's `s3()` table function reads Parquet from MinIO using standard HTTP, turning the object store into a queryable data lake layer (bronze views).
- **Persistence:** Data survives container restarts via a Docker named volume (`minio-data`).

**Bucket design rationale:**

| Bucket | Purpose | Why Separate |
|--------|---------|--------------|
| `raw` | Landing zone for all ingested Parquet | Preserve original data; enables reprocessing |
| `silver` | Intermediate outputs | Reserved for future Spark-based transforms |
| `gold` | Serving-ready exports | Reserved for future ML feature stores or BI tools needing files |
| `checkpoints` | Spark Structured Streaming | Enables exactly-once processing |
| `logs` | Pipeline run logs | Central log aggregation |

**Where it lives:** `infrastructure/docker/docker-compose.storage.yml` → `minio` (port 9001 console, 9000 API) + `minio-init` (one-time bucket setup using mc client).

---

### 3.5 dbt — Data Transformation Framework

**What it is:** A SQL-first transformation tool. Engineers write `SELECT` statements; dbt handles `CREATE TABLE AS SELECT` execution, dependency resolution, documentation, and data testing.

**Why it was chosen:**
- **Declarative transformations:** A dbt model is a plain SQL file. The framework handles whether it becomes a view or table, what schema it lands in, and what engine it uses (ClickHouse-specific).
- **Dependency graph:** dbt automatically determines model execution order using `{{ ref() }}` and `{{ source() }}` macros. Running `dbt run --select gold` will also run all upstream silver models that gold depends on.
- **Data testing built-in:** `schema.yml` files define tests (uniqueness, not_null, accepted_values, relationships) that run via `dbt test`. These are the data quality assertions in the pipeline.
- **Documentation:** `dbt docs generate` produces a data catalog with column-level lineage, served via nginx on port 8082.
- **Git-native:** Every transformation change is a SQL file diff in version control.

**ClickHouse adapter specifics (`dbt-clickhouse==1.10.0`):**
- Translates dbt's generic CREATE TABLE into ClickHouse-compatible DDL.
- Supports `+engine` config to specify MergeTree variants.
- Handles ClickHouse's `ORDER BY` requirements (required for all MergeTree tables).

**Where it lives:** `transformation/dbt/datalake_transforms/`. Run from host machine (not inside a container), targeting ClickHouse at `localhost:8123`.

**The `generate_schema_name` macro:** dbt's default behavior prefixes the target schema name to the custom schema (e.g., `default_staging`). This override macro returns the custom schema exactly as specified (`staging`, `gold`) — critical for ClickHouse where schema names map directly to databases.

---

### 3.6 Apache Spark — Distributed Compute

**What it is:** A unified analytics engine for large-scale data processing, supporting batch, streaming, SQL, and ML workloads.

**Why it was included:** Spark is deployed as a future-ready compute engine for:
- Processing files too large for dbt's in-database transforms
- Spark Structured Streaming for real-time event pipelines
- Machine learning feature engineering

**Current status:** Deployed (master + worker) but not actively used in the primary pipeline. ClickHouse handles all current transform needs efficiently.

**Where it lives:** `docker-compose.transform.yml` → `spark-master` (port 8081) + `spark-worker`.

---

### 3.7 Apache Superset — Business Intelligence

**What it is:** An open-source BI and data exploration platform. Supports interactive dashboards, SQL Lab (ad-hoc query editor), and 50+ chart types.

**Why it was chosen:**
- **ClickHouse connector:** `clickhouse-sqlalchemy` enables direct connection to the gold analytics layer.
- **No per-seat licensing:** Enterprise BI tools (Tableau, Looker, Power BI) are expensive. Superset is free.
- **Embedded API:** Dashboard refresh is callable via REST API from Airflow tasks, enabling pipeline-triggered cache invalidation.

**Why Redis:** Superset uses Redis as a caching backend for dashboard query results and session management. Without Redis, every dashboard load re-executes SQL.

**Where it lives:** `docker-compose.visualization.yml` → `superset` (port 8088) + `redis` (port 6379).

---

### 3.8 HashiCorp Vault — Secret Management

**What it is:** A secrets management platform. Stores, rotates, and controls access to credentials (passwords, API keys, tokens).

**Why it was chosen:**
- **Centralized credentials:** Rather than duplicating passwords across `.env` files and docker-compose configs, Vault serves as a single source of truth for secrets.
- **Dynamic secrets:** Vault can generate time-limited database credentials (though this project uses static credentials for simplicity).
- **Audit logging:** Every secret access is logged with timestamp and identity.

**Current mode:** Dev mode (root token: `root`). Not production-hardened — intended for learning the Vault workflow pattern.

**Where it lives:** `docker-compose.security.yml` → `vault` (port 8200).

---

### 3.9 Keycloak — Identity and Access Management

**What it is:** An open-source Identity Provider (IdP) implementing OAuth2, OpenID Connect (OIDC), and SAML 2.0. Handles user authentication and authorization.

**Why it was chosen:**
- **SSO (Single Sign-On):** One login for all services that support OIDC (Superset, Grafana, Airflow can all delegate authentication to Keycloak).
- **Role-Based Access Control (RBAC):** Data analyst vs. data engineer vs. admin roles.
- **OAuth2 flows:** Airbyte uses client credentials flow; Keycloak issues access tokens.

**Where it lives:** `docker-compose.security.yml` → `keycloak` (port 8180) + `keycloak-db` (PostgreSQL backend for Keycloak's own persistence).

---

### 3.10 Prometheus + Grafana — Observability

**What they are:**
- **Prometheus:** Time-series metrics database. Scrapes `/metrics` endpoints from services on a configured interval, stores data points with labels.
- **Grafana:** Visualization platform for metrics, logs, and traces. Dashboards connect to Prometheus as a data source.

**Why this combination:**
- Industry standard for container/microservice observability.
- Prometheus scrapes Docker-native metrics from Airflow, ClickHouse, MinIO, and Spark.
- Grafana provides pre-built dashboards for most of these services.

**Where they live:** `docker-compose.observability.yml` → `prometheus` (port 9090) + `grafana` (port 3000).

---

### 3.11 LangGraph + Groq — AI Agent Framework

**What they are:**
- **LangGraph:** A framework from LangChain for building stateful, multi-actor agentic applications as directed graphs.
- **Groq:** A cloud inference provider offering extremely fast LLM inference via custom hardware (LPU chips), with a generous free tier.

**Why LangGraph over LangChain agents:**
- LangGraph's graph model makes the routing logic explicit and deterministic. The supervisor routes by keyword matching (no LLM cost) before any model is invoked.
- State is typed (`AgentState` TypedDict) and flows through the graph cleanly.
- Each specialist node is independently retryable.

**Why Groq:**
- **Speed:** Groq's LPU achieves 300–500 tokens/second — near-instant responses for a chat UI.
- **Free tier:** `meta-llama/llama-4-scout-17b-16e-instruct` is available at no cost for development.
- **Tool calling support:** The model supports OpenAI-compatible function calling, required for agents that invoke tools (query_clickhouse, trigger_dag, etc.).

**Why `streaming=False`:** Groq's streaming mode has an intermittent bug where it emits partial function-call JSON that the SDK cannot parse, producing "Failed to call a function" errors. Disabling streaming makes the response deterministic.

**Where they live:** `services/ai-agent/`. Graph definition in `graph/pipeline_graph.py`. Agents in `agents/`. Tools in `tools/`. Streamlit UI in `app.py`. Deployed via `docker-compose.ai-agent.yml`.

---

## 4. Infrastructure Layer — Docker and Networking

### 4.1 Layered Docker Compose Design

The stack is split across 10 Docker Compose files. This is intentional: it allows individual layers to be brought up/down without affecting others.

```
infrastructure/docker/
├── docker-compose.base.yml          ← Networks + volumes (ALWAYS first)
├── docker-compose.security.yml      ← Vault + Keycloak
├── docker-compose.storage.yml       ← MinIO
├── docker-compose.ingestion.yml     ← Airbyte proxy nginx
├── docker-compose.transform.yml     ← ClickHouse + Spark + dbt-docs
├── docker-compose.serving.yml       ← PostgreSQL
├── docker-compose.governance.yml    ← Airflow
├── docker-compose.observability.yml ← Prometheus + Grafana
├── docker-compose.visualization.yml ← Superset + Redis
├── docker-compose.sources.yml       ← MySQL + SAP API Flask
└── docker-compose.ai-agent.yml      ← Streamlit AI Agent
```

**Why split instead of one monolithic file?**  
A monolithic `docker-compose.yml` with 30+ services takes 3+ minutes to restart for a single service change. Splitting by function means `make governance` rebuilds only Airflow without touching ClickHouse or MinIO. This is the same pattern used in microservice deployments.

### 4.2 Base Layer: Networks and Volumes

**File:** `docker-compose.base.yml`

All other compose files declare these networks/volumes as `external: true`, meaning they reference the base resources without recreating them.

**Four Docker Networks (why four):**

```
┌─────────────────────────────────────────────────────────────────┐
│  frontend-net (172.18.0.0/16)                                   │
│  Services exposed to users: Airflow, Superset, Keycloak,        │
│  Grafana, MinIO Console, SAP API                                │
│                                                                 │
│  backend-net (172.19.0.0/16)                                    │
│  Data processing internals: ClickHouse, Spark, MinIO API,       │
│  PostgreSQL, MySQL — no direct user access                      │
│                                                                 │
│  management-net                                                 │
│  Operations: Airflow Scheduler, Prometheus, Vault               │
│  Scheduler needs Vault for secrets; Prometheus scrapes all      │
│                                                                 │
│  keycloak-net                                                   │
│  Auth domain isolation: Keycloak app + its PostgreSQL DB        │
│  Prevents keycloak-db from being reachable by data services     │
└─────────────────────────────────────────────────────────────────┘
```

**Why network isolation?**  
Network segmentation is a security principle: a compromised Superset instance cannot directly reach the Keycloak database because they are on different networks with no route between them. This mirrors production VPC/subnet design on AWS/GCP.

**Named Volumes (persistent state):**

| Volume | Service | Why Persistent |
|--------|---------|----------------|
| `minio-data` | MinIO | Raw Parquet files — lake data must survive restarts |
| `clickhouse-data` | ClickHouse | All transformed tables (bronze, staging, gold) |
| `postgres-data` | PostgreSQL | Airflow metadata + AI agent conversation memory |
| `vault-data` | Vault | Secrets would be lost on restart otherwise |
| `keycloak-db-data` | Keycloak DB | User accounts, realm configurations |
| `spark-data` | Spark | Shared workspace for cluster nodes |
| `grafana-data` | Grafana | Dashboard definitions |
| `ai-agent-chroma` | ChromaDB | Vector embeddings for RAG (expensive to re-generate) |

### 4.3 Service Initialization Pattern

Several services run an **init container** — a short-lived task that runs once to configure the service, then exits. This is a Kubernetes pattern adapted to Docker Compose.

**`airflow-init`** (in `docker-compose.governance.yml`):
```bash
# 1. Run database migrations
airflow db migrate

# 2. Create admin user if not exists
airflow users create --username admin --password Airflow@2024 ...

# 3. Generate dbt profiles.yml pointing at ClickHouse
cat > /root/.dbt/profiles.yml << EOF
datalake_transforms:
  outputs:
    dev:
      type: clickhouse
      host: clickhouse
      port: 8123
      ...
EOF
```
Why generate `profiles.yml` at init time? The ClickHouse host inside Docker Compose is `clickhouse` (container name), not `localhost`. A profiles.yml committed to the repo would use `localhost`, breaking container-to-container communication.

**`minio-init`** (in `docker-compose.storage.yml`):
```bash
# Wait for MinIO to be healthy
until mc alias set local http://minio:9000 admin Minio@2024; do sleep 1; done

# Create buckets idempotently
mc mb --ignore-existing local/raw
mc mb --ignore-existing local/silver
mc mb --ignore-existing local/gold
mc mb --ignore-existing local/checkpoints
mc mb --ignore-existing local/logs

# Apply access policies
mc anonymous set-json /tmp/raw-policy.json local/raw
```

**`superset-init`** (custom init command in `docker-compose.visualization.yml`):
```bash
pip install clickhouse-sqlalchemy  # Add ClickHouse dialect
superset db upgrade                # Schema migrations
superset fab create-admin ...      # Admin user
superset init                      # Default roles + permissions
```

### 4.4 Health Checks

Every stateful service defines a health check. Docker uses this to determine when dependent services can start:

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 30s
```

The `start_period` grace period prevents false failures during slow JVM-based service startups (ClickHouse, Airflow).

---

## 5. Data Sources Layer

### 5.1 MySQL 8.0 — ShopFlow Transactional Database

**What it models:** A simplified e-commerce operational database with three tables.

**File:** `services/mysql/init/01_schema.sql`

**Schema Design:**

```sql
CREATE DATABASE shopflow;

-- Customers: 500 rows (generated by Faker)
CREATE TABLE customers (
    customer_id   INT NOT NULL AUTO_INCREMENT,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    phone         VARCHAR(20),
    segment       ENUM('B2B', 'B2C', 'VIP') DEFAULT 'B2C',
    country       VARCHAR(100),
    city          VARCHAR(100),
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
                  ON UPDATE CURRENT_TIMESTAMP,  -- ← Auto-updated, powers CDC cursor
    PRIMARY KEY (customer_id)
) ENGINE=InnoDB;

-- Products: 200 rows
CREATE TABLE products (
    product_id  INT NOT NULL AUTO_INCREMENT,
    name        VARCHAR(255) NOT NULL,
    category    VARCHAR(100),
    price       DECIMAL(10,2) NOT NULL,
    stock_qty   INT DEFAULT 0,
    sku         VARCHAR(100) UNIQUE,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id)
) ENGINE=InnoDB;

-- Orders: 2000 rows
CREATE TABLE orders (
    order_id    INT NOT NULL AUTO_INCREMENT,
    customer_id INT NOT NULL,
    product_id  INT NOT NULL,
    amount      DECIMAL(10,2) NOT NULL,
    quantity    INT DEFAULT 1,
    status      ENUM('pending', 'completed', 'cancelled') DEFAULT 'pending',
    order_date  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id)  REFERENCES products(product_id),
    KEY idx_customer_id (customer_id),
    KEY idx_product_id  (product_id),     -- ← Indexes speed up Airbyte CDC queries
    KEY idx_status      (status)
) ENGINE=InnoDB;
```

**CDC (Change Data Capture) Configuration:**

MySQL must be configured to write a binary log (binlog) in ROW format — this is what Airbyte's Debezium-based connector reads:

```
# In docker-compose.sources.yml MySQL command:
--server-id=1              # Unique ID in replication topology
--log-bin=mysql-bin        # Enable binary logging
--binlog-format=ROW        # Log actual row changes (not SQL statements)
--binlog-row-image=FULL    # Log all column values (not just changed ones)
--expire-logs-days=7       # Auto-clean logs older than 7 days
--gtid-mode=ON             # Global Transaction IDs — enables exact replay
--enforce-gtid-consistency=ON  # Ensures all transactions are GTID-safe
```

**Why ROW format over STATEMENT?**  
STATEMENT format logs the SQL that ran. If the SQL uses `NOW()` or `RAND()`, the replayed statement produces different results. ROW format logs the actual data values written, making replication deterministic.

**Data Generation** (`scripts/generate_mysql_data.py`):
- Uses Python Faker library with seed=42 (reproducible datasets)
- Generates 500 customers across 20+ countries and 3 segments
- 200 products across 10 categories with price ranges $5–$999.99
- 2000 orders spread over 2 years with realistic status distributions

### 5.2 SAP OData API — Procurement System Simulation

**What it models:** An enterprise procurement system exposing vendor master data and purchase orders through an OData-style REST API.

**File:** `services/sap-api/app.py`

**Why simulate SAP?** Real SAP systems require expensive licenses and complex setup. This Flask service mimics the interface patterns (OData pagination, cursor-based incremental extraction, JSON envelope response format) that data engineers encounter in production.

**API Endpoints:**

```
GET /health
    → { "status": "healthy", "vendors": 50, "purchase_orders": 300 }

GET /api/vendors?page=1&page_size=100&updated_after=2024-01-01T00:00:00
    → {
        "count": 50,
        "page": 1,
        "page_size": 100,
        "next_page": null,      # null = last page
        "results": [
          {
            "vendor_id": "V001",
            "name": "Acme Corp",
            "category": "Electronics",
            "country": "USA",
            "payment_terms": "NET30",
            "contact_email": "...",
            "updated_at": "2024-03-15T10:22:00"
          }, ...
        ]
      }

GET /api/purchase-orders?page=1&page_size=100
    → { ... "results": [{ po_id, vendor_id, amount, currency, status, po_date, delivery_date }] }

GET /api/cost-centers?page=1&page_size=100
    → { ... "results": [{ cc_id, name, department, budget, type }] }
```

**Cursor filtering:** The `?updated_after=<ISO8601>` parameter enables incremental extraction — only records modified after the last extraction timestamp are returned. This is how Airbyte's incremental sync mode works with this connector.

**Production deployment:** Gunicorn with 2 workers and 60-second timeout. Gunicorn is a production WSGI server; Flask's built-in development server is single-threaded and not suitable for concurrent requests.

**In-memory data:** All data is generated at startup and held in memory. This simplifies the service but means data resets on restart — acceptable for a simulation environment.

### 5.3 JSONPlaceholder REST API — Public Test API

**What it models:** User-generated content (posts, comments, users) acting as a proxy for product review data.

**Why use a public API?**  
- No infrastructure required
- Demonstrates that the pipeline handles external HTTP sources, not just internal databases
- Tests the full REST ingestion pathway (Airflow task → HTTP → Parquet → MinIO)

**Endpoints used:**

| Endpoint | Records | Mapped to |
|----------|---------|-----------|
| `/users` | 10 | `src_rest_users` (bronze) → ingestion validation |
| `/posts` | 100 | `src_rest_posts` (bronze) → `stg_reviews` (silver) |
| `/comments` | 500 | `src_rest_comments` (bronze) |

**Synthetic FK mapping:**  
JSONPlaceholder posts have no relationship to our product catalog. The silver model `stg_reviews.sql` generates a synthetic product_id using modulo arithmetic:
```sql
((post_id - 1) % 200) + 1 AS product_id  -- Maps 100 posts to 200 products
((post_id % 5) + 1)         AS rating     -- Generates 1–5 star ratings
```
This allows the gold `fct_reviews` fact table to have valid FK joins to `dim_products`, demonstrating the full dimensional model even with synthetic data.

---

## 6. Ingestion Layer — Airbyte and Custom Connectors

### 6.1 Airbyte Architecture (Kubernetes Deployment)

Airbyte is not a simple single-container application. Its architecture requires several cooperating services:

```
┌────────────────────────────────────────────────────────┐
│                 Airbyte (in Kubernetes)                │
│                                                        │
│  airbyte-webapp (nginx, port 8000)                     │
│       ↕                                                │
│  airbyte-server (REST API backend, port 8001)          │
│       ↕                                                │
│  airbyte-temporal (workflow engine, port 7233)         │
│       ↕                                                │
│  airbyte-worker (spawns sync containers)               │
│       ↕                                                │
│  airbyte-db (PostgreSQL — connection configs, logs)    │
│                                                        │
│  airbyte-connector-builder (custom connector UI, 8003) │
└────────────────────────────────────────────────────────┘
```

**Temporal (workflow engine inside Airbyte):** Airbyte uses Temporal.io for its internal workflow orchestration. Each sync job is a Temporal workflow that manages the connector container lifecycle — spawning the source connector, streaming records to the destination connector, handling retries, and reporting status. Users never interact with Temporal directly.

**Why k8s instead of Docker Compose for Airbyte?**  
Airbyte's worker spawns Docker containers per sync (one for source, one for destination). When run inside Docker Compose, these spawned containers cannot easily communicate with the parent Compose network. In Kubernetes, pod networking is managed by the k8s CNI, which handles dynamic pod-to-pod communication correctly.

**Networking bridge:** From k8s pods, Docker Compose services are reachable at `172.17.0.1` (the Docker bridge gateway IP). This is why the MySQL source and MinIO destination in Airbyte's connection configs use this IP, not `localhost` or a hostname.

### 6.2 MySQL CDC Connection Configuration

**File:** `ingestion/airbyte/connection-configs/mysql_connection.json`

```json
{
  "sourceConfiguration": {
    "host": "172.17.0.1",        // Docker bridge → MySQL container
    "port": 3306,
    "database": "shopflow",
    "username": "root",
    "replication_method": {
      "method": "CDC",           // Change Data Capture via binlog
      "initial_waiting_seconds": 300
    },
    "server_time_zone": "UTC"
  },
  "destinationConfiguration": {
    "s3_bucket_name": "raw",
    "s3_bucket_path": "airbyte/mysql",
    "format": { "format_type": "Parquet", "compression_codec": "SNAPPY" }
  },
  "syncCatalog": {
    "streams": [
      {
        "stream": { "name": "customers" },
        "config": {
          "syncMode": "incremental_append",       // Only new/changed rows
          "destinationSyncMode": "append_dedup",  // Deduplicate at destination
          "cursorField": ["updated_at"],           // Timestamp for incremental
          "primaryKey": [["customer_id"]]          // Dedup key
        }
      },
      // ... same for orders, products
    ]
  }
}
```

**Sync modes explained:**
- `incremental_append` (source): Airbyte reads from MySQL binlog, capturing all changes since the last sync. Each change is emitted as a new record.
- `append_dedup` (destination): At the destination (MinIO/Parquet), Airbyte deduplicates on the primary key, keeping only the latest record per customer_id. This means the Parquet files contain the current state, not a full change history.

### 6.3 Custom Python Ingestion (Airflow Tasks)

For SAP API and JSONPlaceholder, Airbyte is not used. Instead, Airflow tasks directly call the APIs and write Parquet to MinIO. This demonstrates that in real-world pipelines, not every source has an Airbyte connector — engineers write custom extractors.

**SAP ingestion logic** (inside `saas_pipeline.py → trigger_airbyte_sap_sync`):

```python
def ingest_sap_to_minio():
    minio_client = Minio("minio:9000", access_key="admin", secret_key="Minio@2024", secure=False)
    
    for entity in ["vendors", "purchase_orders", "cost_centers"]:
        records = []
        page = 1
        
        while True:
            response = requests.get(
                f"http://sap-api:5001/api/{entity}",
                params={"page": page, "page_size": 100}
            )
            data = response.json()
            records.extend(data["results"])
            
            if data["next_page"] is None:
                break
            page = data["next_page"]
        
        # Add metadata column (standard Airbyte pattern)
        for r in records:
            r["_airbyte_extracted_at"] = datetime.utcnow().isoformat()
        
        # Convert to Parquet using PyArrow
        table = pa.Table.from_pylist(records)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        
        # Upload to MinIO (purge old files first for idempotency)
        minio_client.remove_objects("raw", [f"airbyte/sap/{entity}/"])
        minio_client.put_object(
            "raw",
            f"airbyte/sap/{entity}/{entity}.parquet",
            buffer,
            length=buffer.tell(),
            content_type="application/octet-stream"
        )
```

**Why PyArrow for Parquet?**  
PyArrow is the reference implementation for the Apache Arrow in-memory format and Parquet file format. It handles schema inference, type mapping, and compression natively. Writing Parquet with PyArrow produces files that ClickHouse's `s3()` function and Spark can both read without conversion.

**Why Snappy compression?**  
Snappy provides a good balance of compression ratio and decompression speed. Parquet files are already column-encoded (high compression); Snappy adds ~30% additional size reduction with near-zero CPU cost on read.

---

## 7. Storage Layer — MinIO Object Store

### 7.1 Why Object Storage as the Lake Foundation

Traditional data warehouses store data in proprietary formats inside the database engine. Modern data lakes separate compute from storage:

```
Traditional DWH:                    Modern Data Lake:
┌────────────────────┐              ┌──────────────┐  ┌──────────────┐
│  Database Engine   │              │  Object Store│  │  Query Engine│
│  (storage +        │              │  (MinIO /    │  │  (ClickHouse/│
│   compute mixed)   │              │   S3)        │  │   Spark)     │
└────────────────────┘              └──────────────┘  └──────────────┘
                                         Parquet files    SQL queries
                                       (format-agnostic)  via S3 API
```

**Benefits:**
- **Multiple engines can query the same data:** ClickHouse, Spark, and even AWS Athena can all read the same Parquet files in MinIO via the S3 API.
- **Storage is cheap and durable:** Object stores have no compute cost at rest. You pay only when querying (with compute engines).
- **Schema flexibility:** Parquet files can be written by any producer (Airbyte, PyArrow, Spark) and read by any consumer that understands Parquet.
- **Replayability:** Raw data is never overwritten. If a dbt model has a bug, re-run dbt — the source Parquet is always available.

### 7.2 Parquet File Format

**What Parquet is:** A column-oriented binary file format optimized for analytical workloads. Unlike CSV (row-by-row text), Parquet stores each column's values contiguously.

```
CSV (row-oriented):                   Parquet (column-oriented):
customer_id,name,amount               customer_id column: [1,2,3,4,5,...]
1,Alice,100.00                        name column: [Alice,Bob,Carol,...]
2,Bob,200.00                          amount column: [100.00,200.00,300.00,...]
3,Carol,300.00

For: SELECT sum(amount) FROM orders   
CSV: read ALL bytes (customer_id, name, amount)
Parquet: read ONLY amount column bytes   ← 3× less I/O
```

**Parquet + ClickHouse S3:** ClickHouse's `s3()` table function can push predicates into Parquet reads (column pruning + row group filtering). This makes the bronze layer queries nearly as fast as querying data loaded into ClickHouse tables.

### 7.3 MinIO Access Policies

**File:** `storage/policies/`

Different buckets have different access policies, implementing least-privilege access:

```json
// raw-policy.json — Full access (Airbyte writes + ClickHouse reads)
{
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": ["*"]},
    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
    "Resource": ["arn:aws:s3:::raw/*", "arn:aws:s3:::raw"]
  }]
}

// gold-policy.json — Read-only (only consumers, no writes)
{
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": ["*"]},
    "Action": ["s3:GetObject", "s3:ListBucket"],
    "Resource": ["arn:aws:s3:::gold/*", "arn:aws:s3:::gold"]
  }]
}
```

In production, policies would use specific IAM roles rather than `"*"` as principal. The open policy here is for development simplicity.

---

## 8. The Medallion Architecture — Bronze, Silver, Gold

The medallion architecture (also called lakehouse architecture) organizes data into three quality tiers. Each tier adds progressively more trust, structure, and business logic.

```
RAW PARQUET (MinIO)
      │
      │ ClickHouse s3() views — zero copy
      ▼
BRONZE (ClickHouse schema: bronze)
      │ Type casting, CDC delete filtering
      │ No business logic; just queryable
      ▼
SILVER (ClickHouse schema: staging)
      │ dbt models in models/silver/ and models/staging/
      │ Data cleaning, standardization, computed fields
      │ Validated by dbt tests
      ▼
GOLD (ClickHouse schema: gold)
      │ dbt models in models/gold/ and models/marts/
      │ Dimensional model (star schema)
      │ BI-ready, denormalized for query performance
      ▼
SUPERSET DASHBOARDS / AI AGENT / DIRECT SQL
```

### 8.1 Bronze Layer — Raw Data Queryable

**What:** ClickHouse VIEWs that read Parquet files from MinIO using the `s3()` table function. No data is copied into ClickHouse — every query reads from MinIO in real time.

**Why views, not tables?** The bronze layer is a thin abstraction over MinIO. Making it a view means:
1. No storage duplication (data stays in MinIO)
2. Always reflects the latest Parquet files (no sync lag)
3. Zero ETL cost (ClickHouse handles the S3 read at query time)

**How bronze views are created** (`scripts/setup_clickhouse_bronze.py`):

```python
CREATE VIEW bronze.src_mysql_customers AS
SELECT
    toInt64(customer_id)                              AS customer_id,
    lower(trim(toString(email)))                      AS email,
    toString(first_name)                              AS first_name,
    toString(segment)                                 AS segment,
    parseDateTime64BestEffortOrNull(toString(created_at)) AS created_at
FROM s3(
    'http://minio:9000/raw/airbyte/mysql/customers/*.parquet',
    'admin',
    'Minio@2024',
    'Parquet'
)
WHERE customer_id IS NOT NULL
  AND (_ab_cdc_deleted_at IS NULL OR _ab_cdc_deleted_at = '')
```

**Key decisions:**
- **`_ab_cdc_deleted_at IS NULL`:** Airbyte CDC records deletes with a `_ab_cdc_deleted_at` timestamp. The bronze view excludes deleted rows so they don't propagate into silver/gold.
- **Wildcard `*.parquet`:** The pattern reads all Parquet files under the prefix. As Airbyte writes more sync files over time, they are automatically included without view updates.
- **Type casting at bronze:** ClickHouse cannot always infer exact types from Parquet (e.g., Parquet strings that contain timestamps). Bronze explicitly casts to ClickHouse types.

### 8.2 Silver Layer — Cleaned and Validated

**What:** dbt-managed ClickHouse tables in the `staging` schema. These contain cleaned, standardized, and enriched records — not yet modeled for analytics, but trustworthy.

**Location:** `transformation/dbt/datalake_transforms/models/silver/` → `models/staging/` (SaaS)

**ShopFlow silver models:**

#### `stg_customers` — Customer staging

```sql
{{ config(
    materialized = 'table',
    schema = 'staging',
    engine = 'MergeTree()',
    order_by = '(assumeNotNull(customer_id))',
    settings = {'allow_nullable_key': 1}
) }}

SELECT
    customer_id,
    trim(first_name)                                          AS first_name,
    trim(last_name)                                           AS last_name,
    lower(trim(email))                                        AS email,   -- Canonical lowercase
    CASE
        WHEN upper(trim(segment)) IN ('B2B', 'B2C', 'VIP')
        THEN upper(trim(segment))
        ELSE 'B2C'                                                        -- Default for bad data
    END                                                       AS segment,
    upper(trim(country))                                      AS country, -- Canonical uppercase
    created_at,
    toDate(created_at)                                        AS created_date,
    dateDiff('day', created_at, now())                        AS days_since_signup,
    updated_at
FROM {{ source('bronze', 'src_mysql_customers') }}
WHERE customer_id > 0
  AND email != ''
  AND email LIKE '%@%'      -- Basic email format filter (not a regex — performance)
```

**Transformations applied:**
- Email lowercased and trimmed → canonical form for joining
- Segment defaulted to 'B2C' for invalid values → no NULL segments in gold
- Country uppercased → consistent for geographic grouping
- `days_since_signup` computed → cohort analysis without recalculating in every BI query
- Filters remove rows with invalid customer_id ≤ 0 or malformed emails

#### `stg_orders` — Order staging

```sql
SELECT
    order_id,
    customer_id,
    product_id,
    toDecimal64(amount, 2)     AS amount,    -- Exact decimal, no floating-point errors
    multiIf(
        lower(status) = 'completed',
        toDecimal64(amount, 2),
        toDecimal64(0, 2)
    )                          AS revenue,   -- Revenue = 0 for non-completed orders
    lower(status)              AS status,
    order_date,
    toDate(order_date)         AS order_date_day,
    updated_at
FROM {{ source('bronze', 'src_mysql_orders') }}
WHERE order_id > 0
  AND customer_id > 0
  AND product_id > 0
  AND amount > 0
```

**`multiIf` vs `CASE`:** ClickHouse's `multiIf` is functionally equivalent to `CASE WHEN ... THEN ... ELSE ...` but compiles to a more efficient vectorized execution plan.

**`toDecimal64(amount, 2)`:** Monetary values must use exact decimal representation. Floating-point (FLOAT, DOUBLE) introduces rounding errors (e.g., 0.1 + 0.2 ≠ 0.3). `Decimal64(2)` stores exact values to 2 decimal places.

#### `stg_reviews` — Synthetic review data

```sql
SELECT
    post_id                                AS review_id,
    user_id,
    ((post_id - 1) % 200) + 1             AS product_id,  -- Synthetic FK: 100 posts → 200 products
    ((post_id % 5) + 1)                    AS rating,      -- Synthetic 1–5 stars
    title                                  AS review_title,
    body                                   AS review_text,
    now() - toIntervalDay(post_id)         AS created_at   -- Synthetic dates spread over time
FROM {{ source('bronze', 'src_rest_posts') }}
WHERE post_id IS NOT NULL
  AND user_id IS NOT NULL
```

### 8.3 Gold Layer — Analytics-Ready Dimensional Model

**What:** Star schema tables optimized for BI queries. Dimensions (slowly changing entities) and facts (immutable events) follow dimensional modeling principles.

**Why a star schema?**
- BI tools (Superset, Power BI) generate SQL with simple JOINs. A star schema (facts + dimensions) is the pattern they expect.
- Pre-joined/denormalized fact tables eliminate expensive runtime JOINs for dashboards.
- ClickHouse's columnar storage works best with wide, denormalized tables (fewer JOINs at query time).

**ClickHouse engine choices:**

| Pattern | Engine | Why |
|---------|--------|-----|
| Dimensions (updateable) | `ReplacingMergeTree(version)` | Deduplicates rows with same sort key, keeping highest version. Enables SCD Type 2 without manual DELETE. |
| Facts (append-only) | `MergeTree()` | Events never change. Append-only write pattern. Fastest scan performance. |

---

## 9. Transformation Layer — ClickHouse and dbt

### 9.1 ClickHouse Table Engines — Deep Dive

ClickHouse's storage engines are not interchangeable — they implement fundamentally different data structures:

**`MergeTree()` — The Base Engine**
```
Writes: Data is written in "parts" (sorted chunks on disk)
Background: Parts are merged asynchronously based on ORDER BY key
Reads: Range scans on ORDER BY columns are extremely fast
Use: Facts tables — orders, events (immutable, append-only)
```

**`ReplacingMergeTree(version)` — Deduplication Engine**
```
Same as MergeTree, but during background merges:
  - Rows with identical ORDER BY key are examined
  - Only the row with MAX(version) is kept
  
Use: Dimensions — customer, product master data that changes over time
Gotcha: Deduplication happens asynchronously during merges.
         Immediately after INSERT, duplicates may still be visible.
         Use FINAL keyword or OPTIMIZE TABLE to force merge:
         SELECT * FROM dim_customers FINAL WHERE is_current = 1
```

**Why `version` = `toUnixTimestamp(updated_at)`?**
Unix timestamps are integers. `ReplacingMergeTree(version)` keeps the row with the highest integer value. Using `updated_at` as the version ensures that the most recently updated record "wins" during deduplication.

### 9.2 SCD Type 2 Implementation

**What SCD Type 2 is:** Slowly Changing Dimension Type 2 tracks historical changes to dimension attributes. Instead of overwriting the old value, a new row is inserted with the new value, and the old row is flagged as historical.

```
Customer ID 1 changes email from alice@old.com to alice@new.com:

customer_key  | customer_id | email          | valid_from | valid_to            | is_current
MD5(1)_v1     | 1           | alice@old.com  | 2023-01-01 | 2024-06-15          | 0
MD5(1)_v2     | 1           | alice@new.com  | 2024-06-15 | 9999-12-31 23:59:59 | 1
```

**How it's implemented in ClickHouse with dbt:**

```sql
-- dim_customers.sql
{{ config(
    materialized = 'table',
    schema = 'gold',
    engine = 'ReplacingMergeTree(version)',
    order_by = '(customer_key, valid_from)'  -- Sort key = (entity, time)
) }}

SELECT
    lower(hex(MD5(toString(customer_id))))           AS customer_key,  -- Surrogate key
    customer_id,
    first_name,
    last_name,
    email,
    segment,
    country,
    created_at                                       AS valid_from,
    toDateTime('9999-12-31 23:59:59')                AS valid_to,      -- Open-ended = current
    toUInt8(1)                                       AS is_current,    -- 1 = active
    assumeNotNull(toUnixTimestamp(updated_at))       AS version        -- For ReplacingMergeTree
FROM {{ ref('stg_customers') }}
```

**The surrogate key (`customer_key`):**  
`lower(hex(MD5(toString(customer_id))))` generates a 32-character hex string (e.g., `6512bd43d9caa6e02c990b0a82652dca`).

Why MD5 instead of a sequence number?  
- MD5 is deterministic: the same input always produces the same key. No central sequence generator needed.
- Works across distributed systems (no coordination required for key generation).
- Enables FK validation: `fct_orders.customer_key` can be verified against `dim_customers.customer_key` without a join.

**Querying SCD Type 2 in BI tools:**  
Always filter `WHERE is_current = 1` (note: not `= true` — ClickHouse stores this as `UInt8`, not Boolean). This returns only the current version of each dimension member.

### 9.3 dbt Project Structure

```
transformation/dbt/datalake_transforms/
├── dbt_project.yml           ← Project config, materialization defaults per folder
├── profiles.yml              ← Generated by airflow-init (not committed to git)
├── macros/
│   └── generate_schema_name.sql  ← Overrides schema naming behavior
├── models/
│   ├── bronze/
│   │   └── sources.yml       ← Declares bronze.src_* as dbt sources (no SQL models)
│   ├── raw/
│   │   └── sources.yml       ← Declares raw.saas_* as dbt sources (no SQL models)
│   ├── silver/               → staging schema
│   │   ├── schema.yml        ← Data quality tests for ShopFlow staging models
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_products.sql
│   │   ├── stg_vendors.sql
│   │   ├── stg_purchase_orders.sql
│   │   └── stg_reviews.sql
│   ├── staging/              → staging schema (SaaS domain)
│   │   ├── stg_users.sql
│   │   ├── stg_events.sql
│   │   └── stg_subscriptions.sql
│   ├── gold/                 → gold schema
│   │   ├── schema.yml        ← Data quality tests for gold models
│   │   ├── dimensions/
│   │   │   ├── dim_customers.sql    (ReplacingMergeTree SCD2)
│   │   │   ├── dim_products.sql     (ReplacingMergeTree)
│   │   │   └── dim_vendors.sql      (ReplacingMergeTree)
│   │   └── facts/
│   │       ├── fct_orders.sql       (MergeTree)
│   │       ├── fct_procurement.sql  (MergeTree)
│   │       └── fct_reviews.sql      (MergeTree)
│   └── marts/                → gold schema (SaaS domain)
│       ├── dim_users.sql
│       ├── fct_daily_active_users.sql
│       ├── fct_event_funnel.sql
│       └── fct_mrr.sql
└── target/                   ← Generated by dbt (compiled SQL, catalog.json)
```

**`models/bronze/sources.yml` — What it is and why it exists:**  
This file contains no SQL. It declares the bronze ClickHouse schema as a dbt source, enabling:
```yaml
sources:
  - name: bronze
    database: bronze
    tables:
      - name: src_mysql_customers
      - name: src_mysql_orders
      ...
```
Silver models then reference these with `{{ source('bronze', 'src_mysql_customers') }}` instead of hardcoded table names. This enables `dbt source freshness` checks and lineage tracking.

### 9.4 dbt Data Quality Tests

**File:** `models/silver/schema.yml`, `models/gold/schema.yml`

dbt ships four built-in generic tests:

```yaml
models:
  - name: stg_customers
    columns:
      - name: customer_id
        tests:
          - unique           # No two rows share the same customer_id
          - not_null         # customer_id is never NULL
      - name: email
        tests:
          - unique           # Emails are globally unique
          - not_null
      - name: segment
        tests:
          - accepted_values:
              values: ['B2B', 'B2C', 'VIP']  # No unexpected segments
      - name: customer_id  # in stg_orders
        tests:
          - relationships:
              to: ref('stg_customers')
              field: customer_id  # FK integrity check
```

**How `dbt test` works:**  
For each test, dbt generates a SQL query. If the query returns rows, the test fails:
```sql
-- unique test compiles to:
SELECT customer_id
FROM staging.stg_customers
GROUP BY customer_id
HAVING count(*) > 1
-- Any result = failure (duplicate IDs exist)
```

The Airflow DAG runs `dbt test --select "silver gold"` and raises an error if any test returns failed rows. This is the automated data quality gate in the pipeline.

---

## 10. Orchestration Layer — Apache Airflow

### 10.1 DAG Design Philosophy

**File:** `orchestration/airflow/dags/saas_pipeline.py`

The DAG follows a **linear dependency chain** — each task only runs if the previous task succeeded. This design is intentional: if Airbyte fails to sync MySQL data, there is no point running dbt silver (the source data hasn't changed).

```
trigger_airbyte_mysql_sync
         ↓
trigger_airbyte_sap_sync        (custom Python: SAP API → MinIO)
         ↓
trigger_airbyte_rest_sync       (custom Python: JSONPlaceholder → MinIO)
         ↓
wait_for_minio_files             (validation gate)
         ↓
run_dbt_silver                   (15-min timeout)
         ↓
run_dbt_gold                     (15-min timeout)
         ↓
run_dbt_tests                    (10-min timeout)
         ↓
data_quality_check               (10 assertions)
         ↓
notify_success
         ↓
refresh_superset_dashboard
```

**DAG-level configuration:**
```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

dag = DAG(
    "shopflow_datalake_pipeline",
    schedule="0 6 * * *",    # Daily at 06:00 UTC (after business day closes)
    start_date=datetime(2024, 1, 1),
    catchup=False,            # Don't backfill historical dates on first run
    max_active_runs=1,        # Prevent concurrent runs (idempotency)
)
```

### 10.2 Task Deep Dive

#### Task 1: `trigger_airbyte_mysql_sync`

```python
def _trigger_airbyte_mysql_sync(**context):
    # Step 1: OAuth2 token (client credentials flow)
    token_resp = requests.post(
        "http://localhost:8000/api/v1/applications/token",
        json={"client_id": AIRBYTE_CLIENT_ID, "client_secret": AIRBYTE_CLIENT_SECRET}
    )
    token = token_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # Step 2: Trigger sync job (POST, not GET)
    sync_resp = requests.post(
        "http://localhost:8000/api/v1/connections/sync",
        json={"connectionId": MYSQL_CDC_CONNECTION_ID},
        headers=headers
    )
    job_id = sync_resp.json()["job"]["id"]
    context["ti"].xcom_push(key="airbyte_job_id", value=job_id)
    
    # Step 3: Poll until complete (every 15 seconds, 600-second timeout)
    for _ in range(40):
        status_resp = requests.post(
            "http://localhost:8000/api/v1/jobs/get",
            json={"id": job_id},
            headers=headers
        )
        status = status_resp.json()["job"]["status"]
        if status == "succeeded": return
        if status in ("failed", "cancelled"): raise AirflowException(...)
        time.sleep(15)
    raise AirflowException("Airbyte sync timed out")
```

**Why poll instead of callback?** Airbyte doesn't have native webhook support. Polling (every 15 seconds for up to 10 minutes) is the standard pattern for integrating synchronous workflow steps with external async jobs.

**XCom push:** `context["ti"].xcom_push(key="airbyte_job_id", value=job_id)` stores the job_id in Airflow's XCom table (PostgreSQL). The final `notify_success` task pulls this value to include in the run summary.

#### Task 4: `wait_for_minio_files` — Validation Gate

```python
def _wait_for_minio_files(**context):
    required_prefixes = [
        "airbyte/mysql/customers/",
        "airbyte/mysql/orders/",
        "airbyte/mysql/products/",
        "airbyte/sap/vendors/",
        "airbyte/sap/purchase_orders/",
        "airbyte/rest/users/",
        "airbyte/rest/posts/",
    ]
    
    for prefix in required_prefixes:
        objects = list(minio_client.list_objects("raw", prefix=prefix, recursive=True))
        assert len(objects) > 0, f"No files found at {prefix}"
```

**Why this task exists:** The previous three tasks trigger sync jobs and write to MinIO, but they don't verify the output. This task is an explicit contract verification step — it asserts that all expected paths contain files before any transformation begins. This prevents dbt from running against empty bronze views (which would produce empty gold tables silently).

#### Task 5 & 6: `run_dbt_silver` / `run_dbt_gold`

```python
def _run_dbt_silver(**context):
    # dbt project is mounted read-only at /dbt/datalake_transforms
    # Copy to writable temp dir (dbt writes to target/)
    import shutil, tempfile, subprocess
    
    temp_dir = tempfile.mkdtemp()
    project_copy = shutil.copytree("/dbt/datalake_transforms", f"{temp_dir}/project")
    
    result = subprocess.run(
        ["dbt", "run", "--select", "silver",
         "--project-dir", project_copy,
         "--profiles-dir", "/root/.dbt"],
        capture_output=True,
        text=True,
        timeout=900  # 15-minute timeout
    )
    
    if result.returncode != 0:
        raise AirflowException(f"dbt silver failed:\n{result.stderr}")
    
    context["ti"].xcom_push(key="dbt_silver_output", value=result.stdout[-2000:])
```

**Why copy to a temp directory?** The dbt project is mounted read-only inside the Docker container (`volumes: - /path/to/dbt:/dbt/datalake_transforms:ro`). dbt writes compiled SQL and manifest files to `target/` during execution. A read-only mount prevents this. Copying to `/tmp` provides a writable workspace.

**Why subprocess instead of Python dbt API?** dbt's Python API is unstable across versions. The CLI is the stable, documented interface. `subprocess.run` is the standard pattern for invoking external CLIs from Python.

#### Task 8: `data_quality_check`

```python
def _data_quality_check(**context):
    checks = [
        # Row count minimums
        ("SELECT count() FROM gold.dim_customers WHERE is_current = 1", ">=", 100,
         "dim_customers must have >= 100 current records"),
        
        ("SELECT count() FROM gold.fct_orders", ">=", 100,
         "fct_orders must have >= 100 orders"),
        
        # NULL validation on FK columns
        ("SELECT count() FROM gold.fct_orders WHERE customer_key IS NULL", "==", 0,
         "fct_orders must have no NULL customer_key"),
        
        # Business logic assertions
        ("SELECT count() FROM gold.fct_orders WHERE status = 'completed' AND revenue = 0", "==", 0,
         "Completed orders must have revenue > 0"),
        
        # SCD2 compliance
        ("SELECT count() FROM gold.dim_customers WHERE is_current NOT IN (0, 1)", "==", 0,
         "is_current must be binary"),
    ]
    
    for sql, operator, threshold, message in checks:
        result = clickhouse_query(sql)
        value = result[0][0]
        
        if operator == ">=" and value < threshold:
            raise AirflowException(f"QUALITY FAILURE: {message} (got {value})")
        elif operator == "==" and value != threshold:
            raise AirflowException(f"QUALITY FAILURE: {message} (got {value})")
```

**Why programmatic assertions instead of dbt tests?**  
dbt tests validate the *structure* of the data (uniqueness, not_null). Programmatic assertions validate *business rules* that depend on runtime values (row count thresholds, revenue > 0 for completed orders). Both are necessary for a complete quality framework.

#### Task 10: `refresh_superset_dashboard`

```python
def _refresh_superset():
    # Step 1: Authenticate
    session = requests.post("http://superset:8088/api/v1/security/login",
                            json={"username": "admin", "password": "Superset@2024",
                                  "provider": "db", "refresh": True})
    token = session.json()["access_token"]
    csrf = requests.get("http://superset:8088/api/v1/security/csrf_token/",
                        headers={"Authorization": f"Bearer {token}"}).json()["result"]
    headers = {"Authorization": f"Bearer {token}", "X-CSRFToken": csrf}
    
    # Step 2: Refresh dataset caches (6 datasets)
    for dataset_id in [9, 10, 11, 12, 13, 14]:
        requests.put(f"http://superset:8088/api/v1/dataset/{dataset_id}/refresh",
                     headers=headers)
```

**Why CSRF tokens for Superset?** Superset uses CSRF protection (Cross-Site Request Forgery) on mutating API calls. The X-CSRFToken header proves the request originated from a trusted caller, not a forged cross-origin request.

---

## 11. Serving and Visualization Layer

### 11.1 PostgreSQL — Metadata Database

PostgreSQL is used in this project for three distinct purposes:

| Purpose | Database | Tables |
|---------|----------|--------|
| Airflow metadata | `airflow` | All Airflow internal tables (DAG runs, task instances, XCom, connections) |
| AI Agent memory | `airflow` (shared) | `ai_agent_memory` (custom table) |
| Keycloak backend | `keycloak` (separate) | Keycloak identity tables |

**Why shared database for Airflow and AI agent?**  
Reducing the number of PostgreSQL instances simplifies operations. The AI agent table is isolated by table name (not schema), which is acceptable for development. In production, separate databases with separate users would be preferred.

**AI Agent memory table schema:**
```sql
CREATE TABLE ai_agent_memory (
    id          SERIAL PRIMARY KEY,
    session_id  TEXT NOT NULL,          -- Browser session (UUID from Streamlit)
    role        TEXT NOT NULL,          -- 'user' or 'assistant'
    content     TEXT NOT NULL,          -- Message text
    agent       TEXT,                   -- Which specialist agent replied (e.g., 'insight')
    metadata    JSONB DEFAULT '{}',     -- Extensible: model, tool_calls, latency
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_ai_agent_memory_session ON ai_agent_memory (session_id);
```

**Why persist conversation memory in PostgreSQL?**  
LangChain/LangGraph agents are stateless by default — each call is independent. Storing conversation history in PostgreSQL enables:
- Multi-turn conversations (the agent knows what was discussed previously)
- Session isolation (each browser tab has its own session_id and history)
- History inspection (admins can query conversation logs)
- Memory survives container restarts (unlike in-memory state)

### 11.2 Apache Superset — BI Visualization

**Connection to ClickHouse:**  
Superset connects to ClickHouse via the `clickhouse-sqlalchemy` dialect, installed during the init step:
```
clickhouse+native://default:Click@2024@clickhouse:9000/gold
```

This connection string uses the **native TCP protocol** (port 9000), not HTTP. The native protocol is more efficient for large result sets.

**Dashboard refresh flow:**  
The Airflow DAG's final task calls `PUT /api/v1/dataset/{id}/refresh` for each dataset connected to a gold table. This tells Superset to invalidate its SQL result cache, ensuring dashboards show the latest data from the just-completed pipeline run.

---

## 12. Security Layer — Vault and Keycloak

### 12.1 HashiCorp Vault — Secret Management

**Dev mode vs. production:** This project runs Vault in development mode (`VAULT_DEV_ROOT_TOKEN_ID=root`). Dev mode stores all secrets in memory and automatically unseals. In production:
1. Vault starts sealed (data is encrypted on disk)
2. Operators must provide unseal keys (Shamir's Secret Sharing) to decrypt
3. Access is controlled by policies (not root token)

**Where credentials should come from in production:**  
```
Developer local: .env file (gitignored)
CI/CD pipeline: Environment variables injected by platform (GitHub Actions secrets)
Docker Compose: ${VAR} references to environment variables
Vault: Dynamic secrets (DB passwords generated per-service, expire after use)
```

### 12.2 Keycloak — OAuth2 and OIDC

**Current usage in this project:**  
Keycloak is deployed but not yet fully integrated with service authentication. Its presence demonstrates the architectural intent: in production, Superset, Grafana, and Airflow would delegate login to Keycloak, enabling:
- Single Sign-On: one login across all tools
- LDAP/Active Directory integration
- MFA enforcement
- Role mapping (Keycloak roles → Superset/Airflow roles)

**Airbyte OAuth2:** The Airbyte client credentials flow uses Airbyte's built-in OAuth2 server (not Keycloak). Credentials (`AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`) are generated in the Airbyte UI and stored in `.env`.

---

## 13. Observability Layer — Prometheus and Grafana

### 13.1 Prometheus — Metrics Collection

**How it works:**  
Prometheus uses a **pull model** — it periodically scrapes HTTP endpoints that expose metrics in Prometheus format:

```
# HELP airflow_dag_run_duration_seconds Duration of DAG runs
# TYPE airflow_dag_run_duration_seconds gauge
airflow_dag_run_duration_seconds{dag_id="shopflow_datalake_pipeline"} 423.7
```

**Scrape targets configured:**
- ClickHouse: exposes query metrics, table sizes, memory usage
- Airflow: DAG run durations, task failure rates
- MinIO: Object storage I/O, bucket sizes
- Node Exporter (host): CPU, memory, disk I/O

**Retention:** Default 15-day local storage. For longer retention, remote storage adapters (Thanos, Cortex) would be configured.

### 13.2 Grafana — Visualization

**Dashboard categories relevant to this project:**
- **Pipeline health:** DAG run duration trend, task failure rate, last successful run
- **Data freshness:** Row count over time per gold table (increasing = data flowing)
- **ClickHouse performance:** Query duration percentiles, active queries, table sizes
- **MinIO storage:** Bucket growth over time, object count per prefix
- **Infrastructure:** Container CPU/memory (especially ClickHouse and Airflow)

---

## 14. AI Agent Layer — LangGraph and Groq

### 14.1 Architecture Overview

```
User (Browser) ──► Streamlit UI (app.py)
                        │
                        ▼
              ┌─────────────────────┐
              │  LangGraph Supervisor│  ← pipeline_graph.py
              │  (keyword routing)   │
              └─────┬───────────────┘
                    │
        ┌───────────┼────────────────────────────────┐
        ▼           ▼           ▼           ▼         ▼
    Airbyte     Ingestion   Quality   Orchestration  Insight
     Agent       Agent       Agent      Agent        Agent
        │           │           │           │         │
    airbyte_    minio_      clickhouse_ airflow_  clickhouse_
    tools.py    tools.py    tools.py    tools.py  tools.py
        │           │           │           │         │
        ▼           ▼           ▼           ▼         ▼
    Airbyte     MinIO       ClickHouse  Airflow   ClickHouse
    REST API    S3 API      Native      REST API  Native
                            Protocol              Protocol
```

### 14.2 Supervisor Routing — Keyword-Based (No LLM Cost)

The supervisor doesn't call the LLM to decide which agent handles a message. Instead, it uses a deterministic keyword-matching function:

```python
def _route(state: AgentState) -> str:
    msg = state["message"].lower()
    
    # Airbyte checked FIRST — its keywords overlap with ingestion
    if any(w in msg for w in [
        "airbyte", "connection", "connector", "sync job",
        "source connector", "destination connector", "abctl",
    ]):
        return "airbyte"
    
    if any(w in msg for w in [
        "sync", "minio", "ingest", "landed", "files",
        "mysql cdc", "sap api", "rest api", "parquet", "raw bucket",
    ]):
        return "ingestion"
    
    # ... etc.
    return "insight"  # Default: business intelligence
```

**Why keyword routing instead of LLM routing?**  
- **Speed:** LLM routing adds ~500ms + token cost for every message. Keyword matching is microseconds.
- **Predictability:** "Check my Airbyte sync" always routes to the Airbyte agent. With LLM routing, occasional misroutes occur.
- **Cost:** Free. The first LLM call happens inside the specialist agent, not before.
- **Debuggability:** Keyword sets are visible in code; LLM decisions are opaque.

**Ordering matters:** "airbyte" is checked before "ingestion" because both keywords can appear in ingestion-related questions. The more specific check runs first.

### 14.3 Specialist Agent Pattern

Each specialist agent is a LangChain `AgentExecutor` with:
1. A system prompt that defines its role and ClickHouse SQL constraints
2. A set of tools (≤4 per agent — Groq function-calling breaks with 5+)
3. The base LLM (`create_groq_llm()`) with `streaming=False`

```python
# agents/base.py
def get_llm() -> ChatGroq:
    return ChatGroq(
        model=os.getenv("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct"),
        api_key=os.getenv("GROQ_API_KEY"),
        temperature=0,           # Deterministic: analytics requires exact answers
        streaming=False,         # Critical: streaming breaks Groq tool calling
        max_retries=2,
    )

def create_agent(system_prompt: str, tools: list) -> AgentExecutor:
    llm = get_llm()
    return create_react_agent(llm, tools, prompt=...) | AgentExecutor(...)
```

**Temperature = 0:** For an analytics agent, creative/varied answers are undesirable. The same question should always produce the same SQL query. Temperature 0 makes the model deterministic.

### 14.4 Retry Logic in the Graph

```python
def _make_runner(name: str, factory):
    def run(state: AgentState) -> AgentState:
        _agent = factory()
        
        for attempt in range(3):
            try:
                result = _agent.invoke({"input": state["message"]})
                return {**state, "agent": name, "response": result["output"]}
            except Exception as e:
                if "Failed to call a function" in str(e) and attempt < 2:
                    time.sleep(1 + attempt)           # 1s, then 2s backoff
                    if attempt == 1:
                        # Simplify message on second retry
                        state = {**state, "message": state["message"].split("?")[0]}
                    continue
                return {**state, "response": f"⚠️ Error: {str(e)}"}
    return run
```

**"Failed to call a function" error:** This is a Groq-specific issue where the model emits malformed function call JSON, often triggered by:
- Complex messages with multiple questions
- Long conversation history (exceeds context window)
- Rate limiting causing partial responses

**The second-retry message truncation** (`split("?")[0]`) removes follow-up questions from compound queries. `"How many orders were placed? Also show me by country?"` becomes `"How many orders were placed"` — a simpler input that's less likely to confuse the model.

### 14.5 RAG Knowledge Base

**What RAG is:** Retrieval-Augmented Generation — before the LLM generates a response, relevant context documents are retrieved from a vector database and included in the prompt.

**File:** `services/ai-agent/rag/knowledge_base.py`

```python
# Seeded at startup with 9 static documents
PIPELINE_DOCS = [
    {
        "id": "arch_overview",
        "text": "ShopFlow Data Lake: MySQL CDC → Airbyte → MinIO → ClickHouse bronze → dbt silver → dbt gold → Superset",
        "metadata": {"category": "architecture"}
    },
    {
        "id": "schemas",
        "text": "ClickHouse schemas: bronze (src_mysql_*, src_sap_*, src_rest_*), staging (stg_customers, stg_orders...), gold (dim_customers SCD2, dim_products, fct_orders...)",
        "metadata": {"category": "schemas"}
    },
    # ... 7 more documents
]

def seed_knowledge_base():
    client = chromadb.PersistentClient(path=_CHROMA_DIR)
    collection = client.get_or_create_collection("pipeline_docs")
    
    # Idempotent: check existing IDs, only add new ones
    existing = set(collection.get()["ids"])
    new_docs = [d for d in PIPELINE_DOCS if d["id"] not in existing]
    
    collection.add(
        ids=[d["id"] for d in new_docs],
        documents=[d["text"] for d in new_docs],
        metadatas=[d["metadata"] for d in new_docs]
    )

def query_knowledge(question: str, n_results: int = 3) -> list[str]:
    collection = client.get_collection("pipeline_docs")
    results = collection.query(query_texts=[question], n_results=n_results)
    return results["documents"][0]  # List of matching doc texts
```

**ChromaDB:** An open-source vector database. ChromaDB converts text to embedding vectors (numerical representations of semantic meaning) and stores them locally. When you query with a question, ChromaDB finds the documents whose vectors are closest to the question's vector — semantically similar content, not just keyword matches.

**Why static documentation, not dynamic data?**  
The knowledge base contains architectural documentation that changes rarely. For dynamic data (current row counts, last sync time), agents use live tools (query_clickhouse, check_minio_bucket_size). The knowledge base is for questions like "what schemas exist?" or "how is the pipeline structured?" — context that would otherwise require reading the CLAUDE.md manually.

### 14.6 Tool Implementations

#### `clickhouse_tools.py` — Direct ClickHouse Queries

```python
from clickhouse_driver import Client

def _get_client():
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", 9000)),  # Native protocol, NOT 8123
        user="default",
        password="Click@2024"
    )

@tool
def query_clickhouse(sql: str) -> str:
    """Execute a ClickHouse SQL query and return results as a formatted table."""
    client = _get_client()
    rows, columns = client.execute(sql, with_column_types=True)
    col_names = [col[0] for col in columns]
    return format_as_table(col_names, rows[:20])  # Cap at 20 rows
```

**Port 9000 (native) vs 8123 (HTTP):** Inside Docker Compose, containers communicate over the internal network. The native protocol (port 9000) is more efficient than HTTP for the AI agent's interactive queries — lower latency, binary encoding, streaming results.

**ClickHouse SQL constraints baked into system prompts:**

| Constraint | Reason |
|------------|--------|
| `is_current = 1` not `= true` | ClickHouse stores this as `UInt8`, not `Boolean`. `= true` compiles but returns 0 rows. |
| `count()` not `COUNT(*)` | ClickHouse style convention; both work but `count()` is idiomatic. |
| No `LAG()` — use self-join | ClickHouse supports `LAG()` but it can be unreliable on MergeTree with deduplication. Self-join is guaranteed correct. |
| `toString(round(amount, 2))` before `concat()` | `concat()` does not accept `Decimal64` directly. Explicit cast required. |
| `toStartOfMonth()` for date truncation | ClickHouse-specific function; `DATE_TRUNC` may not work in all contexts. |

#### `airbyte_tools.py` — OAuth2 Token Caching

```python
_token_cache = {}  # Module-level dict persists across tool calls in same container

def _get_airbyte_token() -> str:
    now = time.time()
    
    if "token" in _token_cache and now < _token_cache["expires_at"] - 30:
        return _token_cache["token"]   # Use cached token if > 30s remaining
    
    resp = requests.post(
        f"{AIRBYTE_BASE_URL}/api/v1/applications/token",
        json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
              "grant_type": "client_credentials"}
    )
    token = resp.json()["access_token"]
    _token_cache["token"] = token
    _token_cache["expires_at"] = now + 3300  # 55 minutes (tokens expire at 60)
    return token
```

**Why module-level caching?** Airbyte tokens expire in 60 minutes. The AI agent makes many Airbyte tool calls in a single session. Re-authenticating on every tool call:
- Adds ~200ms latency per call
- Consumes rate-limited OAuth2 endpoints
- Is unnecessary since tokens are valid for an hour

The 30-second buffer (`expires_at - 30`) prevents using a token that might expire mid-request.

**Airbyte API quirk:** All Airbyte API calls use POST, not GET, even for read operations:
```python
# Wrong: GET /api/v1/connections/{id}  ← returns 404
# Correct:
requests.post("/api/v1/connections/get", json={"connectionId": conn_id})
requests.post("/api/v1/connections/list", json={"workspaceId": workspace_id})
requests.post("/api/v1/jobs/list", json={"configId": conn_id, "configTypes": ["SYNC"]})
```
This is an Airbyte API design choice (not a REST convention) — all operations go through POST with a JSON body.

---

## 15. Data Modeling Deep Dive

### 15.1 Star Schema Design

The gold layer implements a classical **star schema**: a central fact table surrounded by dimension tables.

```
         dim_customers (SCD2)
              │
              │ customer_key
              ▼
dim_products ← fct_orders → dim_vendors (via fct_procurement)
              │
              │ product_key
              ▼
         dim_products
```

**Why star schema over 3NF (normalized) schema?**

| Criterion | 3NF | Star Schema |
|-----------|-----|-------------|
| Write performance | Better (no denormalization) | Worse |
| Read performance for analytics | Slower (many joins) | Faster (fewer joins, wide rows) |
| BI tool compatibility | Poor | Excellent (Superset generates simple joins) |
| Storage | Less (normalized) | More (denormalized values repeated) |
| ClickHouse optimization | Sub-optimal | Optimal (column scans on wide tables) |

For an analytical data lake where reads vastly outnumber writes, star schema is always preferred.

### 15.2 Fact Table Design — `fct_orders`

```sql
{{ config(
    materialized = 'table',
    schema = 'gold',
    engine = 'MergeTree()',
    order_by = '(order_date_day, order_id)',  -- Sort key: date first for range queries
    partition_by = 'toYYYYMM(order_date_day)' -- Optional: partition by month
) }}

SELECT
    o.order_id,
    o.customer_id,                                             -- Source NK (natural key)
    o.product_id,
    lower(hex(MD5(toString(o.customer_id)))) AS customer_key, -- FK → dim_customers
    lower(hex(MD5(toString(o.product_id))))  AS product_key,  -- FK → dim_products
    o.amount,
    o.revenue,
    o.status,
    o.order_date,
    o.order_date_day,
    -- Denormalized dimension attributes (for single-table BI queries):
    c.segment              AS customer_segment,
    c.country              AS customer_country,
    p.category             AS product_category,
    p.price_tier           AS product_price_tier,
    p.name                 AS product_name
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c
    ON lower(hex(MD5(toString(o.customer_id)))) = c.customer_key
    AND c.is_current = 1    -- Always join to current dimension version
LEFT JOIN {{ ref('dim_products') }} p
    ON lower(hex(MD5(toString(o.product_id)))) = p.product_key
```

**`ORDER BY (order_date_day, order_id)`:**  
ClickHouse stores data sorted by this key on disk. Queries filtering by date range (`WHERE order_date_day BETWEEN '2024-01-01' AND '2024-03-31'`) perform sequential reads on a small slice of disk — extremely fast. This is ClickHouse's primary performance optimization mechanism.

**Denormalized columns:** `customer_segment`, `product_category`, `price_tier` are repeated in every order row. This is intentional — it allows:
```sql
-- Revenue by customer segment — no JOIN required
SELECT customer_segment, sum(revenue)
FROM gold.fct_orders
WHERE status = 'completed'
GROUP BY customer_segment
```
Without denormalization, this query would require joining `fct_orders → dim_customers` on every execution.

### 15.3 SaaS Domain Models

The SaaS domain follows the same medallion pattern but uses PostgreSQL as the source (not MySQL CDC):

**Airflow task extracts directly from PostgreSQL:**
```python
# saas_pipeline.py — runs before dbt silver
def _extract_saas_data():
    pg_conn = psycopg2.connect(POSTGRES_DSN)
    cursor = pg_conn.cursor()
    
    cursor.execute("SELECT * FROM saas_users")
    users = cursor.fetchall()
    
    # Write to ClickHouse raw schema
    ch_client.execute("INSERT INTO raw.saas_users VALUES", users)
```

**Gold marts for SaaS:**
- `dim_users` — User dimension (SCD2, ReplacingMergeTree)
- `fct_daily_active_users` — Grain: (user_id, event_date). Count of events per user per day.
- `fct_event_funnel` — Grain: (user_id, funnel_stage). Conversion tracking.
- `fct_mrr` — Grain: (subscription_month). Monthly Recurring Revenue.

---

## 16. End-to-End Data Lineage

This section traces a single customer record from source to dashboard.

### Step 1: Source Data Created
```sql
-- MySQL, shopflow.customers
INSERT INTO customers (first_name, email, segment, country)
VALUES ('Alice', 'alice@example.com', 'VIP', 'USA');
-- customer_id = 42 (auto-increment)
-- updated_at = '2024-06-15 10:00:00' (auto-set)
```

### Step 2: Airbyte CDC Captures the Change
MySQL binlog records the insert. Airbyte's Debezium connector reads the binlog entry and emits:
```json
{
  "customer_id": 42,
  "email": "alice@example.com",
  "segment": "VIP",
  "country": "USA",
  "_ab_cdc_updated_at": "2024-06-15T10:00:00Z",
  "_ab_cdc_deleted_at": null,
  "_airbyte_emitted_at": "2024-06-15T10:15:00Z"
}
```

### Step 3: Written to MinIO as Parquet
Airbyte's S3 destination writes to:
```
s3://raw/airbyte/mysql/customers/2024-06-15T10-15-00_sync_14.parquet
```
The Parquet file contains the full current state of the customers table (append_dedup mode).

### Step 4: Bronze View Queries MinIO
```sql
-- ClickHouse bronze layer (s3() view)
SELECT customer_id, lower(email), segment, country
FROM s3('http://minio:9000/raw/airbyte/mysql/customers/*.parquet', 'admin', 'Minio@2024', 'Parquet')
WHERE _ab_cdc_deleted_at IS NULL
-- Returns: 42, 'alice@example.com', 'VIP', 'USA'
```

### Step 5: dbt Silver Transform
```sql
-- stg_customers: clean and standardize
SELECT
    42                              AS customer_id,
    'alice@example.com'             AS email,       -- already lowercase
    'VIP'                           AS segment,     -- already valid enum
    'USA'                           AS country,     -- already uppercase
    dateDiff('day', created_at, now()) AS days_since_signup
-- dbt writes this to staging.stg_customers
```

### Step 6: dbt Gold Dimension
```sql
-- dim_customers: surrograte key + SCD2 columns
SELECT
    '6512bd43d9caa6e02c990b0a82652dca'   AS customer_key,  -- MD5(42)
    42                                    AS customer_id,
    'VIP'                                 AS segment,
    'USA'                                 AS country,
    '2024-06-15 10:00:00'                 AS valid_from,
    '9999-12-31 23:59:59'                 AS valid_to,
    1                                     AS is_current
-- Stored in gold.dim_customers with ReplacingMergeTree
```

### Step 7: Fact Table Join
When orders arrive for customer 42:
```sql
-- fct_orders includes denormalized customer data
SELECT
    o.order_id,
    '6512bd43d9caa6e02c990b0a82652dca' AS customer_key,
    'VIP'                               AS customer_segment,  -- denormalized
    'USA'                               AS customer_country   -- denormalized
FROM stg_orders o
LEFT JOIN dim_customers c ON c.customer_key = MD5('42') AND c.is_current = 1
```

### Step 8: Superset Dashboard Query
```sql
-- Superset executes this when rendering "Revenue by Segment" chart:
SELECT customer_segment, sum(revenue) as total_revenue
FROM gold.fct_orders
WHERE status = 'completed'
GROUP BY customer_segment
ORDER BY total_revenue DESC
-- Result: [('VIP', 125000.00), ('B2B', 87500.00), ('B2C', 43200.00)]
```

Alice's VIP status flows from MySQL → Airbyte → MinIO → ClickHouse bronze → dbt silver → dbt gold → Superset chart, with full lineage trackable at each step.

---

## 17. Deployment Runbook

### First-Time Setup

```bash
# 1. Copy environment file
cp .env.example .env
# Edit .env with your GROQ_API_KEY

# 2. Start base infrastructure (networks + volumes)
make base

# 3. Start supporting layers (order matters for dependencies)
make security      # Vault + Keycloak must start before services that read secrets
make storage       # MinIO must start before Airflow (DAG writes Parquet there)
make transform     # ClickHouse must start before dbt can run
make serving       # PostgreSQL must start before Airflow (metadata DB)
make governance    # Airflow (depends on PostgreSQL + ClickHouse)
make observability # Prometheus + Grafana (depends on other services being up to scrape)
make visualization # Superset + Redis (depends on ClickHouse)
make sources       # MySQL + SAP API (data sources)

# 4. Seed MySQL with synthetic data
make seed-mysql

# 5. Start Airbyte in Kubernetes (separate from Docker Compose)
abctl local install

# 6. Configure Airbyte connections (one-time)
python scripts/setup_airbyte_phase2.py       # MySQL CDC connection
python scripts/setup_airbyte_http_sources.py # SAP + REST connections

# 7. Run first Airbyte sync manually from UI or:
# Trigger via Airflow DAG UI at http://localhost:8080

# 8. After first sync, create ClickHouse bronze layer
python scripts/setup_clickhouse_bronze.py

# 9. Run dbt from host machine
cd transformation/dbt/datalake_transforms
dbt run      # Builds silver and gold layers
dbt test     # Validates all models

# 10. (Optional) Start AI Agent
make ai-agent
```

### Common Operations

```bash
# Check what's running
make ps

# Tail all container logs
make logs

# Check resource consumption
make ram

# Rebuild just the Airflow image (after Dockerfile changes)
docker compose -f infrastructure/docker/docker-compose.governance.yml build
make governance

# Run a specific dbt model
dbt run --select stg_customers   # Just the customers staging model
dbt run --select +fct_orders     # fct_orders and all its upstream dependencies

# Debug ClickHouse query
curl -u default:Click@2024 \
  "http://localhost:8123/?query=SELECT+count()+FROM+gold.fct_orders"

# Check Airbyte sync status
curl -s http://localhost:8000/api/v1/jobs/list \
  -H "Authorization: Bearer <token>" \
  -d '{"configId": "9993f6c9-040d-47bb-830b-31de9137a477", "configTypes": ["SYNC"]}' \
  | python -m json.tool

# Teardown (preserve data)
make down    # Stop containers, volumes remain

# Full reset (destroy all data)
make nuke    # Stop containers + remove volumes
```

### Troubleshooting

**Airflow tasks fail with "dbt not found":**  
dbt is installed in the custom Airflow image (`Dockerfile.airflow`). Rebuild the image: `docker compose -f infrastructure/docker/docker-compose.governance.yml build airflow-webserver`.

**ClickHouse bronze views return 0 rows:**  
Check that Airbyte has successfully synced: `list_objects("raw", prefix="airbyte/mysql/")`. If empty, trigger Airbyte sync and wait.

**AI Agent "Failed to call a function" error:**  
This is a Groq rate limit or context length issue. Try: (1) rephrasing the question, (2) clearing chat history, (3) checking GROQ_API_KEY is valid.

**Airflow Orchestration Agent returns 401:**  
Verify `AIRFLOW__API__AUTH_BACKENDS` is set in `docker-compose.governance.yml`. Restart Airflow webserver.

**Airbyte sync fails with connection refused:**  
Verify MySQL/MinIO are accessible from k8s pods at `172.17.0.1`. Test: `kubectl exec -it <airbyte-worker-pod> -- curl http://172.17.0.1:3306`.

---

## 18. Design Decisions and Alternatives Considered

### Why not Apache Kafka for CDC?

**Kafka setup:** MySQL → Debezium → Kafka → ClickHouse consumer

This is a valid production architecture for high-throughput, low-latency CDC. It was not chosen because:
- Kafka requires ZooKeeper (additional stateful service)
- Schema Registry required for Avro serialization
- Consumer groups, offset management, and partition tuning are complex
- For daily batch analytics (this project's use case), Airbyte's batch CDC is sufficient
- Kafka makes sense when latency must be sub-minute; this pipeline runs daily

### Why not Apache Flink instead of dbt?

Flink handles streaming transformations (millisecond latency, stateful processing). dbt handles batch transformations (runs on a schedule, full table scans). Since this pipeline runs daily (not in real-time), dbt is appropriate. Flink would be the right choice if gold tables needed to update within seconds of source changes.

### Why not Snowflake or BigQuery instead of ClickHouse?

- **Cost:** Snowflake and BigQuery charge per-query and per-storage. ClickHouse on-premise has zero marginal cost.
- **Learning:** Understanding ClickHouse's storage engines, merge trees, and S3 integration teaches fundamental OLAP concepts that cloud warehouses abstract away.
- **Portability:** This stack runs identically on a developer laptop, a bare-metal server, or Kubernetes.

### Why not Kubernetes for the full stack?

Kubernetes adds operational complexity (Helm charts, ingress controllers, pod networking, persistent volume claims, RBAC) that obscures the data engineering concepts this project demonstrates. Docker Compose is the right abstraction level for a learning/development stack. The Airbyte exception exists because Airbyte's architecture genuinely requires Kubernetes for connector pod management.

### Why not Delta Lake or Apache Iceberg instead of raw Parquet?

Delta Lake and Iceberg add ACID transactions, schema evolution tracking, and time travel on top of Parquet. These are valuable for production lakes with concurrent writers. This project has a single writer per prefix (Airbyte or one Airflow task), so the overhead of a table format is unnecessary. ClickHouse's `ReplacingMergeTree` handles the "latest version wins" deduplication that Delta/Iceberg's merge operations provide.

### Why PostgreSQL as both the Airflow metadata DB and AI agent memory DB?

Running two separate PostgreSQL instances for Airflow and the AI agent would consume ~400MB additional memory for identical functionality. For a development stack, sharing a PostgreSQL instance with separate databases (or in this case, a separate table) is a pragmatic decision. In production, isolation between operational and analytical databases is recommended.

---

*This documentation is version-controlled alongside the code. When making significant architectural changes, update the relevant sections of this document in the same commit.*
