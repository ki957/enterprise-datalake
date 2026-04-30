---
type: community
cohesion: 0.08
members: 41
---

# PostgreSQL Audit Store

**Cohesion:** 0.08 - loosely connected
**Members:** 41 nodes

## Members
- [[Audit store for self-healing agent actions.  Three tables (auto-created on first]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[Clear and retry a specific failed Airflow task without triggering a full DAG run]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[List all existing Great Expectations suites.     Shows the suite name and how ma]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/contract_tools.py
- [[Open a new incident record. Returns the incident ID.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[Park a high-risk action until a human approves it. Returns pending ID.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[Queue a high-risk or irreversible action for human approval.     Use for rollba]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[Record every action attempt — success, failure, or blocked.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[Return (allowed, reason). Checks guardrail before any write.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[Return the last 10 incidents detected and acted on by the self-healing agent.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[Return the most recent incidents, newest first.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[Sanitise model name lowercase, alphanumeric + underscore only.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/dbt_write_tools.py
- [[Strip any LLM-written config blocks and convert direct table refs to {{ ref() }}]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/dbt_write_tools.py
- [[Tools for the AI data contract agent.  The contract engine works in 3 steps   1]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/contract_tools.py
- [[Trigger an Airbyte sync for a connection. Provide the connection UUID and a brie]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[Write AI-generated Great Expectations to disk for a table.     Backs up any exis]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/contract_tools.py
- [[Write a new dbt SQL model to modelsmarts and validate it with dbt compile.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/dbt_write_tools.py
- [[Write tools for the NL → dbt agent.  create_dbt_model — write SQL to modelsmart]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/dbt_write_tools.py
- [[Write tools for the self-healing agent.  Every tool that mutates system state]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[_af_auth()_1]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[_af_base()_1]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[_conn()_2]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[_ensure_tables()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[_get_pool()_2]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[_is_allowed()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[_normalise_sql()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/dbt_write_tools.py
- [[_safe_name()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/dbt_write_tools.py
- [[audit_store.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[contract_tools.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/contract_tools.py
- [[create_dbt_model()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/dbt_write_tools.py
- [[create_incident()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[dbt_write_tools.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/dbt_write_tools.py
- [[get_recent_incidents()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[get_recent_incidents_summary()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[healing_tools.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[list_expectation_suites()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/contract_tools.py
- [[log_action()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[queue_for_approval()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/memory/audit_store.py
- [[request_approval()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[restart_airflow_task()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[trigger_airbyte_sync_safe()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/healing_tools.py
- [[write_expectations()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/contract_tools.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/PostgreSQL_Audit_Store
SORT file.name ASC
```

## Connections to other communities
- 2 edges to [[_COMMUNITY_Airbyte OAuth2 API Client]]
- 1 edge to [[_COMMUNITY_FastAPI Server & Cost Tracking]]
- 1 edge to [[_COMMUNITY_AI Agent Tool Integration Layer]]

## Top bridge nodes
- [[_conn()_2]] - degree 8, connects to 1 community
- [[trigger_airbyte_sync_safe()]] - degree 7, connects to 1 community
- [[write_expectations()]] - degree 4, connects to 1 community