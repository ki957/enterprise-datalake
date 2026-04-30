---
type: community
cohesion: 0.03
members: 98
---

# AI Agent Memory & Cost System

**Cohesion:** 0.03 - loosely connected
**Members:** 98 nodes

## Members
- [[AI Layer (LangGraph Multi-Agent System)]] - document - CLAUDE.md
- [[Airbyte (Ingestion via k8s)]] - document - CLAUDE.md
- [[Airbyte Agent]] - document - CLAUDE.md
- [[Alertmanager]] - document - CLAUDE.md
- [[Anomaly Detection Agent]] - document - CLAUDE.md
- [[Apache Airflow (Orchestration)]] - document - CLAUDE.md
- [[Apache Spark (Batch Processing)]] - document - CLAUDE.md
- [[Apache Superset (BI Visualization)]] - document - CLAUDE.md
- [[Auto-Contracts from Query Traffic (Improvement Opportunity)]] - document - CLAUDE.md
- [[Bronze Layer (S3 Views over MinIO)]] - document - CLAUDE.md
- [[CI Workflow (ci.yml) - 12 jobs]] - document - CLAUDE.md
- [[ChromaDB (RAG Vector Store)]] - document - CLAUDE.md
- [[ClickHouse (OLAP Engine)]] - document - CLAUDE.md
- [[Contract Agent]] - document - CLAUDE.md
- [[Debezium (Kafka Connect CDC)]] - document - CLAUDE.md
- [[Deploy Workflow (deploy.yml)]] - document - CLAUDE.md
- [[Enterprise Data Lake Project]] - document - CLAUDE.md
- [[FastAPI (AI Agent API Server)]] - document - CLAUDE.md
- [[Gold Layer (Dimensions + Facts + Marts)]] - document - CLAUDE.md
- [[Grafana (Observability Dashboards)]] - document - CLAUDE.md
- [[Great Expectations (Data Quality Framework)]] - document - CLAUDE.md
- [[Groq (LLM Provider)]] - document - CLAUDE.md
- [[HashiCorp Vault (Secrets Management)]] - document - CLAUDE.md
- [[Ingestion Agent]] - document - CLAUDE.md
- [[Insight Agent]] - document - CLAUDE.md
- [[Kafka (Streaming with KRaft)]] - document - CLAUDE.md
- [[Keycloak (SSOOIDC)]] - document - CLAUDE.md
- [[KeycloakSecurityManager (Superset)]] - document - CLAUDE.md
- [[LangGraph (Agent Orchestration Framework)]] - document - CLAUDE.md
- [[Loki (Log Aggregation)]] - document - CLAUDE.md
- [[MCP Server (Improvement Opportunity)]] - document - CLAUDE.md
- [[Medallion Architecture (BronzeSilverGold)_1]] - document - CLAUDE.md
- [[MinIO (Object Storage)]] - document - CLAUDE.md
- [[MySQL (ShopFlow CDC Source)]] - document - CLAUDE.md
- [[NL-to-dbt Agent]] - document - CLAUDE.md
- [[OpenMetadata (Metadata Catalog)]] - document - CLAUDE.md
- [[Orchestration Agent]] - document - CLAUDE.md
- [[Performance Agent]] - document - CLAUDE.md
- [[Pipeline Graph Router (graphpipeline_graph.py)]] - document - CLAUDE.md
- [[PostgreSQL (SaaS Source + Airflow DB)]] - document - CLAUDE.md
- [[Prometheus (Metrics Collection)]] - document - CLAUDE.md
- [[Quality Agent]] - document - CLAUDE.md
- [[Rationale =4 tools per agent due to Groq function-calling limit]] - document - CLAUDE.md
- [[Rationale Airbyte runs in k8s via abctl, not Docker Compose]] - document - CLAUDE.md
- [[Rationale Bronze layer is imperatively managed, not by dbt]] - document - CLAUDE.md
- [[Rationale Intent-specific agents weighted 3x in routing]] - document - CLAUDE.md
- [[Rationale NL-dbt agent writes only to modelsmarts for safety]] - document - CLAUDE.md
- [[Rationale No Cloud Services — Full Local Docker Compose]] - document - CLAUDE.md
- [[Rationale Vault in dev mode with root token for simplicity]] - document - CLAUDE.md
- [[Rationale streaming=False for Groq to avoid function-call failures]] - document - CLAUDE.md
- [[Raw Layer (SaaS Extracts)]] - document - CLAUDE.md
- [[React 18 + Vite Frontend (AI Agent UI)]] - document - CLAUDE.md
- [[Redis (Superset Cache)]] - document - CLAUDE.md
- [[Retrieval-Augmented Analytics (Improvement Opportunity)]] - document - CLAUDE.md
- [[SAP OData API (Flask 5001)]] - document - CLAUDE.md
- [[SaaS Domain (Subscriptions)]] - document - CLAUDE.md
- [[Schema Agent]] - document - CLAUDE.md
- [[Self-Healing Agent]] - document - CLAUDE.md
- [[ShopFlow Domain (E-Commerce)]] - document - CLAUDE.md
- [[StagingSilver Layer]] - document - CLAUDE.md
- [[VaultBackend (Airflow Secrets)]] - document - CLAUDE.md
- [[agent_cost_log Table (PostgreSQL)]] - document - CLAUDE.md
- [[agentsbase.py]] - document - CLAUDE.md
- [[ai_agent_memory Table (PostgreSQL)]] - document - CLAUDE.md
- [[airbyte_health.py DAG]] - document - CLAUDE.md
- [[audit_store Table (PostgreSQL)]] - document - CLAUDE.md
- [[auto_contract_dag.py DAG]] - document - CLAUDE.md
- [[chromadb==0.5.23]] - document - services/ai-agent/requirements.txt
- [[clickhouse-driver==0.2.9]] - document - services/ai-agent/requirements.txt
- [[clickhouse_tools.py_1]] - document - CLAUDE.md
- [[data_quality_suite.py DAG]] - document - CLAUDE.md
- [[dbt (Data Transformation)]] - document - CLAUDE.md
- [[dbt Semantic Layer (MetricFlow)]] - document - CLAUDE.md
- [[dbt_runner.py DAG]] - document - CLAUDE.md
- [[langchain-community==0.3.23]] - document - services/ai-agent/requirements.txt
- [[langchain-groq==0.3.2]] - document - services/ai-agent/requirements.txt
- [[langchain==0.3.25]] - document - services/ai-agent/requirements.txt
- [[langgraph==0.2.76]] - document - services/ai-agent/requirements.txt
- [[macrosgenerate_schema_name.sql]] - document - CLAUDE.md
- [[meta-llamallama-4-scout-17b-16e-instruct]] - document - CLAUDE.md
- [[metadata_sync.py DAG]] - document - CLAUDE.md
- [[metricflow_time_spine.sql]] - document - CLAUDE.md
- [[minio==7.2.0]] - document - services/ai-agent/requirements.txt
- [[pandas=2.0.0]] - document - services/ai-agent/requirements.txt
- [[plotly=5.18.0]] - document - services/ai-agent/requirements.txt
- [[psycopg2-binary==2.9.9]] - document - services/ai-agent/requirements.txt
- [[pydantic=2.0.0]] - document - services/ai-agent/requirements.txt
- [[python-dotenv==1.0.0]] - document - services/ai-agent/requirements.txt
- [[requests==2.31.0]] - document - services/ai-agent/requirements.txt
- [[saas_pipeline.py DAG]] - document - CLAUDE.md
- [[schema_evolution_dag.py DAG]] - document - CLAUDE.md
- [[server.py (FastAPI AI Agent Server)]] - document - CLAUDE.md
- [[setup_clickhouse_bronze.py_1]] - document - CLAUDE.md
- [[shopflow_pipeline.py DAG]] - document - CLAUDE.md
- [[spark_profiler.py DAG]] - document - CLAUDE.md
- [[streaming_enrichment_dag.py DAG]] - document - CLAUDE.md
- [[toolsdbt_write_tools.py]] - document - CLAUDE.md
- [[unified_profile.py DAG]] - document - CLAUDE.md

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/AI_Agent_Memory_&_Cost_System
SORT file.name ASC
```
