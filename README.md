# Enterprise Data Lake

A modular, Docker Compose-based Enterprise Data Lake with AI-powered orchestration.

## Architecture

```
ShopFlow (MySQL CDC) → Airbyte → MinIO → ClickHouse → dbt → Superset
SaaS (PostgreSQL)    → Airflow → ClickHouse → dbt → Grafana
                                         ↓
                                   AI Agent (11 LangGraph Agents)
```

## Quick Start

```bash
make base          # Create networks + volumes
make all            # Full stack (security → visualization)
make ui             # AI Agent v2 (React + FastAPI)
```

## Service Access

| Service | URL | Credentials |
|---------|-----|--------------|
| AI Agent UI | http://localhost:3001 | — |
| AI Agent API | http://localhost:8502 | — |
| Airflow | http://localhost:8080 | admin / Airflow@2024 |
| Superset | http://localhost:8088 | admin / Superset@2024 |
| Grafana | http://localhost:3000 | admin / Grafana@2024 |
| ClickHouse | http://localhost:8123 | default / Click@2024 |
| MinIO Console | http://localhost:9001 | admin / Minio@2024 |

## Project Structure

```
enterprise-datalake/
├── services/ai-agent/          # AI Layer (11 agents, FastAPI)
├── orchestration/airflow/      # 11 Active DAGs
├── transformation/dbt/         # dbt models (bronze/silver/gold)
├── infrastructure/            # Docker, Prometheus, Grafana
├── governance/               # Great Expectations, ADRs
└── tests/                    # Unit + integration tests
```

## Documentation

- **Full Docs**: [docs/PROJECT_DOCUMENTATION.md](docs/PROJECT_DOCUMENTATION.md)
- **AI Agent Graph**: [graphify-out/graph.html](graphify-out/graph.html)
- **Architecture Decisions**: [docs/adr/](docs/adr/)
