---
type: community
cohesion: 0.16
members: 19
---

# ClickHouse Query Tools

**Cohesion:** 0.16 - loosely connected
**Members:** 19 nodes

## Members
- [[Check ClickHouse system.query_log for slow queries ( 1 second) from today.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[ClickHouse tools for the AI agent.  Connection pooling a single module-level Cl]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[Describe columns of a ClickHouse table as a markdown table (name, type, default)]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[Execute a query, retrying once on a broken connection.     clickhouse-driver rai]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[Format query results as a markdown table for clean LLM output.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[Get row counts for all tables in a ClickHouse schema.     Use for data quality c]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[Reject DDL  DML statements — this agent is read-only.     Returns (is_safe, rea]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[Return a cached Client for the given database.     Creates a new connection only]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[Run a SELECT query on ClickHouse. Use for business analytics and ad-hoc inspecti]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[_execute()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[_format_as_markdown_table()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[_is_safe_sql()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[_make_client()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[check_slow_queries()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[clickhouse_tools.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[describe_table()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[get_ch_client()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[get_table_row_counts()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py
- [[query_clickhouse()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/clickhouse_tools.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/ClickHouse_Query_Tools
SORT file.name ASC
```
