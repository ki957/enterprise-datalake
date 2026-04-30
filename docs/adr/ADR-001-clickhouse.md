# ADR-001: ClickHouse as the OLAP Engine

**Status:** Accepted  
**Date:** 2025-04-15  
**Deciders:** Data Platform Team

---

## Context

The enterprise data lake needs an analytical query engine to power:
- dbt transformations across bronze → silver → gold layers
- Real-time BI dashboards (Grafana, Superset)
- Ad-hoc queries over 500K+ order records, 200K+ SaaS events
- An AI Agent that runs natural-language SQL queries

Candidates evaluated:
1. **ClickHouse** — columnar OLAP, open-source, HTTP + native protocols
2. **Apache Druid** — sub-second queries, heavy operational burden
3. **DuckDB** — fast in-process, no server, no distributed write path
4. **BigQuery / Redshift** — managed cloud DWH, not compatible with local-first goal
5. **Apache Hive/Spark SQL** — mature but high latency for interactive queries

---

## Decision

Use **ClickHouse 23.8** as the primary OLAP engine for all gold-layer tables.

---

## Rationale

| Criterion | ClickHouse | Druid | DuckDB |
|-----------|-----------|-------|--------|
| Query latency (1M rows) | < 100ms | < 100ms | < 200ms |
| Docker image size | ~350MB | ~1.5GB | N/A (library) |
| RAM footprint (idle) | ~300MB | ~2GB+ | N/A |
| HTTP API | Yes (:8123) | Yes | No |
| dbt adapter | `dbt-clickhouse` (mature) | `dbt-druid` (experimental) | `dbt-duckdb` |
| Incremental / MergeTree | Yes | Yes | No |
| SCD support | `ReplacingMergeTree` | Lookups | No |
| Local Docker deployment | Trivial | Complex (ZK + Broker + Historical) | N/A |

### Why ClickHouse wins
1. **RAM efficiency** — the full stack runs on ~5GB headroom; Druid is excluded on resource grounds alone.
2. **dbt-clickhouse maturity** — incremental strategies, custom schema macros, and MergeTree engine configs are all first-class.
3. **ReplacingMergeTree** — natively supports SCD Type 2 deduplication for `dim_*` tables without complex merge logic.
4. **HTTP interface** — Grafana ClickHouse plugin, Superset adapter, and the AI Agent all use simple HTTP POST over port 8123, avoiding driver complexity.
5. **MergeTree partitioning** — `ORDER BY` and `PARTITION BY order_month` on `fct_orders` allows sub-second range scans even at 500K rows.

---

## Consequences

- **Positive:** Fast interactive queries; lightweight Docker image; battle-tested dbt adapter.
- **Positive:** `ReplacingMergeTree` + `FINAL` dedup pattern handles slowly-changing dimensions without custom Python.
- **Negative:** ClickHouse SQL dialect diverges from ANSI SQL — no `LAG()`, different date functions. The AI Agent system prompt encodes these constraints (see CLAUDE.md).
- **Negative:** No transactional writes — not suitable for OLTP use cases (PostgreSQL handles that for SaaS source).
- **Mitigation:** The `generate_schema_name` dbt macro strips the default-schema prefix so schemas publish as `staging` and `gold` (not `default_staging`).

---

## Alternatives Rejected

- **Druid**: Excluded due to ~2GB RAM overhead per node — exceeds local resource budget.
- **DuckDB**: No server mode; unsuitable for multi-user Superset + Grafana + AI Agent concurrent queries.
- **BigQuery**: Requires GCP account; violates local-first / air-gapped deployment goal.
