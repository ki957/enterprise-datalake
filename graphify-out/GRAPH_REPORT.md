# Graph Report - .  (2026-04-28)

## Corpus Check
- 107 files · ~50,000 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1254 nodes · 1788 edges · 64 communities detected
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 195 edges (avg confidence: 0.78)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_AI Agent Core & Cost Tracking|AI Agent Core & Cost Tracking]]
- [[_COMMUNITY_Airbyte Agent & Sync Tools|Airbyte Agent & Sync Tools]]
- [[_COMMUNITY_Airbyte Connections & OAuth2|Airbyte Connections & OAuth2]]
- [[_COMMUNITY_RAG, Bronze Layer & Serialisation|RAG, Bronze Layer & Serialisation]]
- [[_COMMUNITY_Integration Tests & ClickHouse Fixtures|Integration Tests & ClickHouse Fixtures]]
- [[_COMMUNITY_ShopFlow Ingestion Pipeline|ShopFlow Ingestion Pipeline]]
- [[_COMMUNITY_FastAPI Server & Pydantic Models|FastAPI Server & Pydantic Models]]
- [[_COMMUNITY_Audit Store & Self-Healing|Audit Store & Self-Healing]]
- [[_COMMUNITY_Agent Routing & Airflow DB|Agent Routing & Airflow DB]]
- [[_COMMUNITY_MySQL Data Generator|MySQL Data Generator]]
- [[_COMMUNITY_dbt Runner DAG & Manifest|dbt Runner DAG & Manifest]]
- [[_COMMUNITY_React Frontend Components|React Frontend Components]]
- [[_COMMUNITY_SaaS Data Generator|SaaS Data Generator]]
- [[_COMMUNITY_Airbyte Tools Layer|Airbyte Tools Layer]]
- [[_COMMUNITY_Architecture Docs & ADRs|Architecture Docs & ADRs]]
- [[_COMMUNITY_Community 15 (20 nodes)|Community 15 (20 nodes)]]
- [[_COMMUNITY_Community 16 (20 nodes)|Community 16 (20 nodes)]]
- [[_COMMUNITY_Community 17 (19 nodes)|Community 17 (19 nodes)]]
- [[_COMMUNITY_Community 18 (18 nodes)|Community 18 (18 nodes)]]
- [[_COMMUNITY_Community 19 (16 nodes)|Community 19 (16 nodes)]]
- [[_COMMUNITY_Community 20 (16 nodes)|Community 20 (16 nodes)]]
- [[_COMMUNITY_Community 21 (15 nodes)|Community 21 (15 nodes)]]
- [[_COMMUNITY_Community 22 (14 nodes)|Community 22 (14 nodes)]]
- [[_COMMUNITY_Community 23 (14 nodes)|Community 23 (14 nodes)]]
- [[_COMMUNITY_Community 24 (14 nodes)|Community 24 (14 nodes)]]
- [[_COMMUNITY_Community 25 (14 nodes)|Community 25 (14 nodes)]]
- [[_COMMUNITY_Community 26 (14 nodes)|Community 26 (14 nodes)]]
- [[_COMMUNITY_Community 27 (13 nodes)|Community 27 (13 nodes)]]
- [[_COMMUNITY_Community 28 (13 nodes)|Community 28 (13 nodes)]]
- [[_COMMUNITY_Community 29 (12 nodes)|Community 29 (12 nodes)]]
- [[_COMMUNITY_Community 30 (12 nodes)|Community 30 (12 nodes)]]
- [[_COMMUNITY_Community 31 (12 nodes)|Community 31 (12 nodes)]]
- [[_COMMUNITY_Community 32 (11 nodes)|Community 32 (11 nodes)]]
- [[_COMMUNITY_Community 33 (11 nodes)|Community 33 (11 nodes)]]
- [[_COMMUNITY_Community 34 (10 nodes)|Community 34 (10 nodes)]]
- [[_COMMUNITY_Community 35 (9 nodes)|Community 35 (9 nodes)]]
- [[_COMMUNITY_Community 36 (9 nodes)|Community 36 (9 nodes)]]
- [[_COMMUNITY_Community 37 (9 nodes)|Community 37 (9 nodes)]]
- [[_COMMUNITY_Community 38 (9 nodes)|Community 38 (9 nodes)]]
- [[_COMMUNITY_Community 39 (8 nodes)|Community 39 (8 nodes)]]
- [[_COMMUNITY_Community 40 (8 nodes)|Community 40 (8 nodes)]]
- [[_COMMUNITY_Community 41 (8 nodes)|Community 41 (8 nodes)]]
- [[_COMMUNITY_Community 42 (7 nodes)|Community 42 (7 nodes)]]
- [[_COMMUNITY_Community 43 (6 nodes)|Community 43 (6 nodes)]]
- [[_COMMUNITY_Community 44 (6 nodes)|Community 44 (6 nodes)]]
- [[_COMMUNITY_Community 45 (6 nodes)|Community 45 (6 nodes)]]
- [[_COMMUNITY_Community 46 (6 nodes)|Community 46 (6 nodes)]]
- [[_COMMUNITY_Community 47 (6 nodes)|Community 47 (6 nodes)]]
- [[_COMMUNITY_Community 48 (5 nodes)|Community 48 (5 nodes)]]
- [[_COMMUNITY_Community 49 (5 nodes)|Community 49 (5 nodes)]]
- [[_COMMUNITY_Community 50 (4 nodes)|Community 50 (4 nodes)]]
- [[_COMMUNITY_Community 51 (4 nodes)|Community 51 (4 nodes)]]
- [[_COMMUNITY_Community 52 (4 nodes)|Community 52 (4 nodes)]]
- [[_COMMUNITY_Community 53 (3 nodes)|Community 53 (3 nodes)]]
- [[_COMMUNITY_Community 57 (3 nodes)|Community 57 (3 nodes)]]
- [[_COMMUNITY_Community 59 (3 nodes)|Community 59 (3 nodes)]]
- [[_COMMUNITY_Community 84 (1 nodes)|Community 84 (1 nodes)]]
- [[_COMMUNITY_Community 85 (1 nodes)|Community 85 (1 nodes)]]
- [[_COMMUNITY_Community 86 (1 nodes)|Community 86 (1 nodes)]]
- [[_COMMUNITY_Community 87 (1 nodes)|Community 87 (1 nodes)]]
- [[_COMMUNITY_Community 88 (1 nodes)|Community 88 (1 nodes)]]
- [[_COMMUNITY_Community 89 (1 nodes)|Community 89 (1 nodes)]]
- [[_COMMUNITY_Community 90 (1 nodes)|Community 90 (1 nodes)]]
- [[_COMMUNITY_Community 91 (1 nodes)|Community 91 (1 nodes)]]

## God Nodes (most connected - your core abstractions)
1. `get_llm() singleton factory` - 25 edges
2. `AI Layer (LangGraph Multi-Agent System)` - 25 edges
3. `ch_conn()` - 24 edges
4. `TestMySQLGenerator` - 20 edges
5. `AI Agent Architecture (11 Specialist Agents)` - 19 edges
6. `Apache Airflow (Orchestration)` - 16 edges
7. `TestSaaSGenerator` - 15 edges
8. `MinIO raw bucket — Parquet landing zone` - 15 edges
9. `audit_store.py — self-healing action audit log` - 15 edges
10. `Zustand Global Store (useStore)` - 13 edges

## Surprising Connections (you probably didn't know these)
- `get_dag_status()` --semantically_similar_to--> `get_failed_task_logs tool`  [INFERRED] [semantically similar]
  /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py → services/ai-agent/agents/self_healing_agent.py
- `saas_data_pipeline DAG` --rationale_for--> `Rationale: Custom Dockerfile.airflow bakes dbt/pyarrow/minio for reproducibility`  [INFERRED]
  orchestration/airflow/dags/saas_pipeline.py → docs/PROJECT_DOCUMENTATION.md
- `Integration Test Fixtures (conftest.py)` --references--> `Test Docker Compose Stack`  [EXTRACTED]
  tests/integration/conftest.py → infrastructure/docker/docker-compose.test.yml
- `Data Generator Unit Tests` --references--> `Development & Testing Dependencies (requirements-dev.txt)`  [INFERRED]
  tests/test_data_generators.py → requirements-dev.txt
- `SaaS Pipeline Logic Unit Tests` --references--> `Development & Testing Dependencies (requirements-dev.txt)`  [INFERRED]
  tests/test_saas_pipeline_logic.py → requirements-dev.txt

## Hyperedges (group relationships)
- **ClickHouse Decision Cluster: ADR, rationales, and SQL constraints form a unified technology decision record** — adr_001_clickhouse, rationale_clickhouse_ram_efficiency, clickhouse_ansi_sql_constraints [EXTRACTED 1.00]
- **TLS Three-Layer Architecture: Traefik ingress + Vault PKI mTLS + encryption at rest form the production security posture** — traefik_tls_ingress, vault_pki_internal_ca, tls_production_architecture [EXTRACTED 1.00]

## Communities

### Community 0 - "AI Agent Core & Cost Tracking"
Cohesion: 0.03
Nodes (115): agent_cost_log Table (PostgreSQL), agents/base.py, AI Agent Architecture (11 Specialist Agents), ai_agent_memory Table (PostgreSQL), AI Layer (LangGraph Multi-Agent System), Airbyte (k8s Ingestion), Airbyte Agent, airbyte_health.py DAG (+107 more)

### Community 1 - "Airbyte Agent & Sync Tools"
Cohesion: 0.03
Nodes (73): create_airbyte_agent(), create_airbyte_agent(), get_latest_sync_job tool, list_airbyte_connections tool, trigger_airbyte_sync tool, _auth(), _base(), get_failed_task_logs tool (+65 more)

### Community 2 - "Airbyte Connections & OAuth2"
Cohesion: 0.04
Nodes (62): Airbyte MySQL CDC → MinIO Parquet connection, Airbyte OAuth2 token cache (55-min TTL), airbyte_tools.py — Airbyte OAuth2 API tools, Airflow REST API, airflow_tools.py — Airflow REST API tools, audit_store.py — self-healing action audit log, ClickHouse bronze schema (S3 views over MinIO), ClickHouse gold schema (+54 more)

### Community 3 - "RAG, Bronze Layer & Serialisation"
Cohesion: 0.04
Nodes (57): POST /api/rag/reload — AI Agent ChromaDB re-seed, models/bronze/sources.yml — dbt source definitions, _ch_insert TSV Serialisation Helper, DAG catchup=False Invariant, DAG Retries Required Invariant, data_quality_check Function, transformation/dbt/datalake_transforms (dbt project), extract_postgres_to_clickhouse Function (+49 more)

### Community 4 - "Integration Tests & ClickHouse Fixtures"
Cohesion: 0.04
Nodes (36): ch_conn(), clickhouse_schema(), pg_conn(), postgres_schema(), Integration test fixtures ========================== Provides session-scoped con, Create the SaaS source tables in the test PostgreSQL instance., Session-scoped ClickHouse query helper; skips if service unreachable., Session-scoped PostgreSQL connection; skips if service unreachable. (+28 more)

### Community 5 - "ShopFlow Ingestion Pipeline"
Cohesion: 0.06
Nodes (58): extract_customers(), extract_orders(), extract_products(), get_minio(), get_mysql(), main(), upload(), _airbyte_trigger_sync() (+50 more)

### Community 6 - "FastAPI Server & Pydantic Models"
Cohesion: 0.05
Nodes (50): BaseModel, compute_cost(), _conn(), _ensure_table(), get_daily_costs(), _get_pool(), get_recent_calls(), get_summary() (+42 more)

### Community 7 - "Audit Store & Self-Healing"
Cohesion: 0.08
Nodes (37): _conn(), create_incident(), _ensure_tables(), _get_pool(), get_recent_incidents(), log_action(), queue_for_approval(), Audit store for self-healing agent actions.  Three tables (auto-created on first (+29 more)

### Community 8 - "Agent Routing & Airflow DB"
Cohesion: 0.07
Nodes (40): _AGENT_KEYWORDS — routing keyword table, AgentState TypedDict, Airflow PostgreSQL DB — shared by memory, cost, audit, app.py — Streamlit Chat UI (legacy), chart_tools.py — Plotly chart generation tool, POST /api/chat — SSE streaming chat endpoint, check_slow_queries() @tool, ChromaDB collection: shopflow_knowledge_v3 (+32 more)

### Community 9 - "MySQL Data Generator"
Cohesion: 0.1
Nodes (14): bulk_insert_chunked(), customer_rows(), fetch_ids(), get_connection(), main(), order_rows(), product_rows(), rand_date() (+6 more)

### Community 10 - "dbt Runner DAG & Manifest"
Cohesion: 0.09
Nodes (32): target/manifest.json — dbt compiled manifest, _cleanup(), dbt_standalone_runner DAG, generate_and_publish_docs(), generate_and_publish_docs() — dbt docs → nginx target/, dbt Standalone Runner ====================== DAG: dbt_standalone_runner Schedule, Run all dbt tests across every layer., Check source data freshness against SLA thresholds. (+24 more)

### Community 11 - "React Frontend Components"
Cohesion: 0.13
Nodes (32): AgentBadge Component, AgentSwitchDivider Component, AGENTS / AGENT_ORDER Data, App Root Component, ChatArea Component, CommandPalette UI Component, CopyButton UI Component, CostDashboard UI Component (+24 more)

### Community 12 - "SaaS Data Generator"
Cohesion: 0.11
Nodes (11): bulk_insert_chunked(), event_rows(), fetch_ids(), get_connection(), main(), rand_dt(), subscription_rows(), user_rows() (+3 more)

### Community 13 - "Airbyte Tools Layer"
Cohesion: 0.15
Nodes (25): _base(), get_airbyte_connection_status(), get_airbyte_source_info(), get_latest_sync_job(), _get_token(), _headers(), list_airbyte_connections(), _no_credentials_msg() (+17 more)

### Community 14 - "Architecture Docs & ADRs"
Cohesion: 0.09
Nodes (26): ADR-001: ClickHouse as OLAP Engine, ADR-002: Groq + Llama 4 Scout as LLM Backend, CLAUDE.md — Project Guidance for Claude Code, ClickHouse SQL Dialect Constraints (no LAG/LEAD, toStartOfMonth, etc.), ClickHouse OLAP Engine, Test Docker Compose Stack, ELT (not ETL) Design Philosophy, Groq ≤4 Tools Per Agent Constraint (+18 more)

### Community 15 - "Community 15 (20 nodes)"
Cohesion: 0.16
Nodes (19): _collect_dag_files(), _get_dags_from_module(), _load_module(), DAG Integrity Tests ==================== Verifies that all Airflow DAGs:   - Par, catchup=True causes backfill storms on first deploy — must be disabled., All DAGs should configure retries — prevents transient failures from failing the, Recursively find all .py files under the dags root, excluding __pycache__., Import a DAG file as a Python module. (+11 more)

### Community 16 - "Community 16 (20 nodes)"
Cohesion: 0.15
Nodes (19): build_spark_session(), build_unified_profile(), ch_exec(), ensure_output_table(), main(), Unified Customer Profile ========================= PySpark cross-domain join: Sh, Create gold.unified_customers if it doesn't exist., Read current ShopFlow customers from ClickHouse. (+11 more)

### Community 17 - "Community 17 (19 nodes)"
Cohesion: 0.16
Nodes (18): check_slow_queries(), describe_table(), _execute(), _format_as_markdown_table(), get_ch_client(), get_table_row_counts(), _is_safe_sql(), _make_client() (+10 more)

### Community 18 - "Community 18 (18 nodes)"
Cohesion: 0.18
Nodes (17): _docs_hash(), force_reload(), get_collection(), _get_join_partner_docs(), maybe_reload_knowledge_base(), query_knowledge(), query_knowledge_graph(), ChromaDB knowledge base — GraphRAG-lite with relationship traversal.  Architectu (+9 more)

### Community 19 - "Community 19 (16 nodes)"
Cohesion: 0.18
Nodes (15): _ch(), _ch_rows(), cross_table_consistency_check(), null_rate_check(), quality_report(), Data Quality Suite =================== DAG: data_quality_suite Schedule: daily a, Verify referential integrity between fact and dimension tables:     - All fct_or, Run the Great Expectations datalake checkpoint via run_checkpoint.py. (+7 more)

### Community 20 - "Community 20 (16 nodes)"
Cohesion: 0.23
Nodes (15): _api(), create_charts(), create_clickhouse_database(), create_dashboard(), create_datasets(), export_dashboard(), _get_csrf_token(), _get_token() (+7 more)

### Community 21 - "Community 21 (15 nodes)"
Cohesion: 0.3
Nodes (14): api(), api_public(), check_source(), create_connection(), create_source(), discover_catalog(), get_stream_stats(), get_token() (+6 more)

### Community 22 - "Community 22 (14 nodes)"
Cohesion: 0.19
Nodes (9): ch(), main(), s3(), data_quality_check(), Row-count and freshness checks across all Gold tables., Row-count and freshness checks across all Gold tables., SaaS Pipeline Logic Tests ========================== Tests the pure-Python helpe, Tests for data_quality_check — validates assertion logic without DB. (+1 more)

### Community 23 - "Community 23 (14 nodes)"
Cohesion: 0.21
Nodes (13): _ensure_snapshot_table(), _pg_conn(), Schema Evolution DAG ===================== DAG: schema_evolution Schedule: daily, Compare MySQL information_schema.columns against Postgres schema_snapshots., For each new column detected, append it to the appropriate sources.yml.     Uses, Run dbt compile to verify sources.yml is valid.     Rolls back sources.yml if co, POST to the AI Agent FastAPI server to force ChromaDB re-seed.     Runs only whe, Create schema_snapshots table in Postgres if it doesn't exist. (+5 more)

### Community 24 - "Community 24 (14 nodes)"
Cohesion: 0.14
Nodes (5): CommandPalette(), InputBar(), PersonaCard(), QuickStartPanel(), useChat()

### Community 25 - "Community 25 (14 nodes)"
Cohesion: 0.25
Nodes (13): _admin_token(), create_grafana_client(), create_realm(), create_superset_client(), create_test_user(), _http(), main(), Keycloak Setup Script ====================== Creates the 'datalake' realm with a (+5 more)

### Community 26 - "Community 26 (14 nodes)"
Cohesion: 0.19
Nodes (13): check_airbyte_connections(), check_airbyte_connections() — latest job status check, airbyte_connection_health DAG, _get_airbyte_token(), _get_airbyte_token() — OAuth2 client credentials, list_recent_jobs() — audit trail last 10 jobs, Airbyte Connection Health Monitor ================================== DAG: airbyt, List the last 10 jobs across all connections for audit trail. (+5 more)

### Community 27 - "Community 27 (13 nodes)"
Cohesion: 0.21
Nodes (12): _ch(), consume_and_enrich(), _enrich_batch_with_groq(), _ensure_raw_table(), Streaming Enrichment DAG ======================== DAG: streaming_enrichment Sche, Poll Kafka for product CDC events, enrich via Groq, write to ClickHouse.     Tra, Run dbt mart_product_enrichment model to land enriched data in gold.     Only ru, POST /api/rag/reload so the Insight Agent immediately knows about     gold.mart_ (+4 more)

### Community 28 - "Community 28 (13 nodes)"
Cohesion: 0.28
Nodes (12): cleanup_old_sessions(), clear_session(), _conn(), _ensure_table(), get_history(), _get_pool(), PostgreSQL-backed conversation memory. Reuses the existing Airflow PostgreSQL in, Return the last `limit` messages for a session, oldest first. (+4 more)

### Community 29 - "Community 29 (12 nodes)"
Cohesion: 0.26
Nodes (11): _ch(), collect_column_usage(), flag_deprecations(), generate_contracts(), _generate_table_contract(), Auto-Contract DAG ================== DAG: auto_contract Schedule: daily at 09:00, For each gold table with at least one hot column (query_count >= HOT_THRESHOLD),, Profile a table and write a rule-based GE expectation suite. (+3 more)

### Community 30 - "Community 30 (12 nodes)"
Cohesion: 0.3
Nodes (9): _filter_by_cursor(), get_cost_centers(), get_purchase_orders(), get_vendors(), _paginate(), _parse_cursor(), ShopFlow Data Lake AI Agent — Chat-first Streamlit UI., Render an agent response that may contain embedded Plotly chart blocks.      Spl (+1 more)

### Community 31 - "Community 31 (12 nodes)"
Cohesion: 0.17
Nodes (12): collect_column_usage() — system.query_log analysis, auto_contract DAG, flag_deprecations() — zero-usage column report, generate_contracts() — GE suite writer for hot tables, _generate_table_contract() — GE expectation suite builder, null_rate_check() — 5% threshold on critical columns, row_count_anomaly_check() — 7-day rolling average, run_great_expectations() — calls run_checkpoint.py (+4 more)

### Community 32 - "Community 32 (11 nodes)"
Cohesion: 0.35
Nodes (10): ensure_bucket(), fetch_all_pages(), get_minio_client(), http_get(), ingest_jsonplaceholder(), ingest_sap(), main(), Convert records list to Parquet and upload to MinIO. (+2 more)

### Community 33 - "Community 33 (11 nodes)"
Cohesion: 0.35
Nodes (10): api(), api_public(), create_connection(), create_minio_destination(), create_mysql_source(), discover_catalog(), get_token(), main() (+2 more)

### Community 34 - "Community 34 (10 nodes)"
Cohesion: 0.27
Nodes (10): AI Agent v2 Frontend (React PWA), Central Hub Node (Circle), Cyan/Teal Brand Color (#5BC8D4), Dark Navy Background (#0A0F1E), Data Pipeline / Datalake Brand Identity, AI Agent Favicon (SVG), Graph/Network Topology Design Intent, Hexagon Network Visual Motif (+2 more)

### Community 35 - "Community 35 (9 nodes)"
Cohesion: 0.22
Nodes (3): App(), useKeyboard(), useToastAutoDismiss()

### Community 36 - "Community 36 (9 nodes)"
Cohesion: 0.31
Nodes (8): _af_auth(), _af_base(), _ch_client(), get_airflow_run_history(), profile_table_stats(), Profiling tools shared by the anomaly detection and data contract agents.  profi, Fetch the last N DAG run records from Airflow: state, start time, duration., Profile a ClickHouse table: null rate, distinct cardinality, and numeric     per

### Community 37 - "Community 37 (9 nodes)"
Cohesion: 0.31
Nodes (8): check_minio_bucket_size(), _client(), list_minio_buckets(), list_minio_files(), MinIO tools for the AI agent.  Singleton client: a single module-level Minio ins, List files in the MinIO 'raw' bucket under the given prefix.     Use to verify A, Get the total file count and size for a MinIO bucket.     Use for storage capaci, List all MinIO buckets. Use to confirm bucket structure is correct.

### Community 38 - "Community 38 (9 nodes)"
Cohesion: 0.25
Nodes (9): /api/chat SSE endpoint (FastAPI at :8502), AGENTS constant (frontend agent registry), QUICK_ACTIONS constant (persona-based quick prompts), SERVICES constant (infrastructure service links), Zustand store (global frontend state), useChat hook (SSE message streaming), useKeyboard hook (Cmd+K command palette shortcut), useToast / useToastAutoDismiss hooks (+1 more)

### Community 39 - "Community 39 (8 nodes)"
Cohesion: 0.32
Nodes (3): CostDashboard(), fmt$(), fmtTokens()

### Community 40 - "Community 40 (8 nodes)"
Cohesion: 0.29
Nodes (7): Quick smoke-test: run 2 questions against model_agent (nl_dbt) and contracts_age, Score a model-agent answer 0-100. Returns (score, issues)., Score a contracts-agent answer 0-100. Returns (score, issues)., run_agent(), run_suite(), score_contract_answer(), score_model_answer()

### Community 41 - "Community 41 (8 nodes)"
Cohesion: 0.32
Nodes (7): get_dbt_model_sql(), Run dbt transformation models. Use select= to target a layer:     'silver' for S, Run all dbt schema and data quality tests.     Returns pass/fail summary and det, Read the SQL source for a specific dbt model.     Use to understand or debug tra, _run_dbt(), run_dbt_models(), run_dbt_tests()

### Community 42 - "Community 42 (7 nodes)"
Cohesion: 0.52
Nodes (6): _get_csrf(), _get_token(), import_zip(), main(), Import a dashboard ZIP via the Superset REST API., wait_for_superset()

### Community 43 - "Community 43 (6 nodes)"
Cohesion: 0.4
Nodes (2): MessageBubble(), parseContent()

### Community 44 - "Community 44 (6 nodes)"
Cohesion: 0.53
Nodes (5): generate_cost_centers(), generate_purchase_orders(), generate_vendors(), _rand_dt(), SAP OData synthetic data generator. Produces: 50 vendors, 300 purchase orders, 3

### Community 45 - "Community 45 (6 nodes)"
Cohesion: 0.47
Nodes (5): build_spark_session(), main(), profile_dataframe(), MinIO Bronze/Silver Data Profiler =================================== PySpark jo, Return a list of per-column profile dicts for the given DataFrame.

### Community 46 - "Community 46 (6 nodes)"
Cohesion: 0.4
Nodes (6): PIPELINE_DOCS — architecture and service URL docs, RELATIONSHIP_DOCS — FK join pattern docs, SQL_PATTERN_DOCS — ClickHouse SQL pattern docs, TABLE_DOCS — static schema doc set for RAG, TOOL_FORMAT_DOCS — tool output format docs, seed_knowledge_base() — ChromaDB upsert

### Community 47 - "Community 47 (6 nodes)"
Cohesion: 0.4
Nodes (6): import_superset_dashboards.py (restores dashboard ZIPs to Superset), Keycloak datalake realm (TOTP MFA + OIDC clients), run_checkpoint.py (executes Great Expectations datalake checkpoint), setup_keycloak.py (creates datalake realm + Grafana/Superset OIDC clients), setup_superset.py (configures ClickHouse datasets + dashboard + export), Superset ClickHouse gold layer datasource

### Community 48 - "Community 48 (5 nodes)"
Cohesion: 0.6
Nodes (3): BrandIcon(), Header(), HealthDots()

### Community 49 - "Community 49 (5 nodes)"
Cohesion: 0.5
Nodes (5): ADR-003: TLS Strategy for Production Deployment, Rationale: Vault PKI selected for mTLS because Vault is already deployed, TLS Production Architecture, Traefik Reverse Proxy TLS Ingress, Vault PKI Secrets Engine as Internal CA

### Community 50 - "Community 50 (4 nodes)"
Cohesion: 0.5
Nodes (2): KeycloakSecurityManager, SupersetSecurityManager

### Community 51 - "Community 51 (4 nodes)"
Cohesion: 0.5
Nodes (3): Spark Data Profiler DAG ======================== DAG: spark_data_profiler Schedu, spark-submit the profiler job and stream output to Airflow logs., submit_spark_profiler()

### Community 52 - "Community 52 (4 nodes)"
Cohesion: 0.5
Nodes (1): getOrCreateSessionId()

### Community 53 - "Community 53 (3 nodes)"
Cohesion: 0.67
Nodes (1): Unified Customer Profile DAG ============================== DAG: unified_custome

### Community 57 - "Community 57 (3 nodes)"
Cohesion: 0.67
Nodes (1): Sidebar()

### Community 59 - "Community 59 (3 nodes)"
Cohesion: 0.67
Nodes (3): superset_config.py — Keycloak SSO + Redis cache config, KeycloakSecurityManager — maps preferred_username/email, OAUTH_PROVIDERS — keycloak OpenID Connect config

### Community 84 - "Community 84 (1 nodes)"
Cohesion: 1.0
Nodes (1): GET /api/health — service port ping health check

### Community 85 - "Community 85 (1 nodes)"
Cohesion: 1.0
Nodes (1): get_table_row_counts() @tool

### Community 86 - "Community 86 (1 nodes)"
Cohesion: 1.0
Nodes (1): list_expectation_suites (tool)

### Community 87 - "Community 87 (1 nodes)"
Cohesion: 1.0
Nodes (1): run_dbt_models (tool)

### Community 88 - "Community 88 (1 nodes)"
Cohesion: 1.0
Nodes (1): run_dbt_tests (tool)

### Community 89 - "Community 89 (1 nodes)"
Cohesion: 1.0
Nodes (1): get_dbt_model_sql (tool)

### Community 90 - "Community 90 (1 nodes)"
Cohesion: 1.0
Nodes (1): AI Agent v2 Backend Requirements

### Community 91 - "Community 91 (1 nodes)"
Cohesion: 1.0
Nodes (1): PostCSS Config

## Knowledge Gaps
- **399 isolated node(s):** `SaaS Data Pipeline =================== DAG: saas_data_pipeline Schedule: daily a`, `Extract saas_users, saas_events, saas_subscriptions from PostgreSQL → ClickHouse`, `Diff gold SaaS mart row counts against previous run.     Writes results to pipel`, `Send a Slack message via webhook. Silently skips if URL not configured.`, `on_failure_callback — fires on any task failure.` (+394 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 43 (6 nodes)`** (6 nodes): `MarkdownContent()`, `MessageBubble()`, `parseContent()`, `PlotlyChart()`, `ToolTrace()`, `MessageBubble.jsx`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 50 (4 nodes)`** (4 nodes): `superset_config.py`, `KeycloakSecurityManager`, `.oauth_user_info()`, `SupersetSecurityManager`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 52 (4 nodes)`** (4 nodes): `index.js`, `createFreshSessionId()`, `getOrCreateSessionId()`, `index.js`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 53 (3 nodes)`** (3 nodes): `unified_profile.py`, `Unified Customer Profile DAG ============================== DAG: unified_custome`, `submit_unified_profile()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 57 (3 nodes)`** (3 nodes): `Sidebar.jsx`, `Sidebar.jsx`, `Sidebar()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 84 (1 nodes)`** (1 nodes): `GET /api/health — service port ping health check`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 85 (1 nodes)`** (1 nodes): `get_table_row_counts() @tool`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 86 (1 nodes)`** (1 nodes): `list_expectation_suites (tool)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 87 (1 nodes)`** (1 nodes): `run_dbt_models (tool)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 88 (1 nodes)`** (1 nodes): `run_dbt_tests (tool)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 89 (1 nodes)`** (1 nodes): `get_dbt_model_sql (tool)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 90 (1 nodes)`** (1 nodes): `AI Agent v2 Backend Requirements`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 91 (1 nodes)`** (1 nodes): `PostCSS Config`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `shopflow_datalake_pipeline DAG` connect `Airbyte Connections & OAuth2` to `Community 26 (14 nodes)`, `RAG, Bronze Layer & Serialisation`?**
  _High betweenness centrality (0.036) - this node is a cross-community bridge._
- **Why does `saas_data_pipeline DAG` connect `RAG, Bronze Layer & Serialisation` to `Airbyte Connections & OAuth2`?**
  _High betweenness centrality (0.033) - this node is a cross-community bridge._
- **Are the 12 inferred relationships involving `get_llm() singleton factory` (e.g. with `_prewarm()` and `create_orchestration_agent()`) actually correct?**
  _`get_llm() singleton factory` has 12 INFERRED edges - model-reasoned connections that need verification._
- **Are the 2 inferred relationships involving `AI Layer (LangGraph Multi-Agent System)` (e.g. with `pandas>=2.0.0` and `python-dotenv==1.0.0`) actually correct?**
  _`AI Layer (LangGraph Multi-Agent System)` has 2 INFERRED edges - model-reasoned connections that need verification._
- **Are the 21 inferred relationships involving `ch_conn()` (e.g. with `.test_select_one()` and `.test_server_returns_version()`) actually correct?**
  _`ch_conn()` has 21 INFERRED edges - model-reasoned connections that need verification._
- **What connects `SaaS Data Pipeline =================== DAG: saas_data_pipeline Schedule: daily a`, `Extract saas_users, saas_events, saas_subscriptions from PostgreSQL → ClickHouse`, `Diff gold SaaS mart row counts against previous run.     Writes results to pipel` to the rest of the system?**
  _399 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `AI Agent Core & Cost Tracking` be split into smaller, more focused modules?**
  _Cohesion score 0.03 - nodes in this community are weakly interconnected._