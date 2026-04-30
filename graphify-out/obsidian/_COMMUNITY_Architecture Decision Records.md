---
type: community
cohesion: 0.09
members: 26
---

# Architecture Decision Records

**Cohesion:** 0.09 - loosely connected
**Members:** 26 nodes

## Members
- [[ADR-001 ClickHouse as OLAP Engine]] - document - docs/adr/ADR-001-clickhouse.md
- [[ADR-002 Groq + Llama 4 Scout as LLM Backend]] - document - docs/adr/ADR-002-groq-llm.md
- [[CLAUDE.md — Project Guidance for Claude Code]] - document - CLAUDE.md
- [[ClickHouse Integration Tests]] - code - tests/integration/test_clickhouse.py
- [[ClickHouse OLAP Engine]] - document - docs/adr/ADR-001-clickhouse.md
- [[ClickHouse SQL Dialect Constraints (no LAGLEAD, toStartOfMonth, etc.)]] - document - docs/adr/ADR-001-clickhouse.md
- [[ELT (not ETL) Design Philosophy]] - document - docs/PROJECT_DOCUMENTATION.md
- [[Enterprise Data Lake — Complete Technical Documentation]] - document - docs/PROJECT_DOCUMENTATION.md
- [[Groq API LLM Backend]] - document - docs/adr/ADR-002-groq-llm.md
- [[Groq streaming=False Requirement]] - document - docs/adr/ADR-002-groq-llm.md
- [[Groq ≤4 Tools Per Agent Constraint]] - document - docs/adr/ADR-002-groq-llm.md
- [[Integration Test Fixtures (conftest.py)]] - code - tests/integration/conftest.py
- [[Layered Composable Deployment Philosophy]] - document - docs/PROJECT_DOCUMENTATION.md
- [[Medallion Architecture (BronzeSilverGold)]] - document - docs/PROJECT_DOCUMENTATION.md
- [[Rationale Airbyte uses k8s abctl to avoid Docker Compose networking conflicts with connector pods]] - document - docs/PROJECT_DOCUMENTATION.md
- [[Rationale ClickHouse chosen for RAM efficiency over Druid]] - document - docs/adr/ADR-001-clickhouse.md
- [[Rationale ELT preserves raw data, transformations versioned in Git]] - document - docs/PROJECT_DOCUMENTATION.md
- [[Rationale Groq free tier covers daily development usage]] - document - docs/adr/ADR-002-groq-llm.md
- [[Rationale Groq ~500 toks on H100 for near-instant tool-call round trips]] - document - docs/adr/ADR-002-groq-llm.md
- [[Rationale dbt-clickhouse maturity and MergeTree engine configs]] - document - docs/adr/ADR-001-clickhouse.md
- [[Rationale generate_schema_name macro strips default prefix so schemas are staginggold not default_staging]] - document - docs/PROJECT_DOCUMENTATION.md
- [[ReplacingMergeTree for SCD Type 2]] - document - docs/adr/ADR-001-clickhouse.md
- [[Test Docker Compose Stack]] - code - infrastructure/docker/docker-compose.test.yml
- [[raw.saas_events ClickHouse Table]] - code - tests/integration/conftest.py
- [[raw.saas_subscriptions ClickHouse Table]] - code - tests/integration/conftest.py
- [[raw.saas_users ClickHouse Table]] - code - tests/integration/conftest.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Architecture_Decision_Records
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_Backend API & Storage Layer]]

## Top bridge nodes
- [[Integration Test Fixtures (conftest.py)]] - degree 6, connects to 1 community