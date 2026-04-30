---
type: community
cohesion: 0.04
members: 83
---

# AI Agent Tool Integration Layer

**Cohesion:** 0.04 - loosely connected
**Members:** 83 nodes

## Members
- [[AI data contract agent — auto-generates Great Expectations from ClickHouse stats]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/contract_agent.py
- [[Anomaly detection agent — LLM-based contextual pipeline anomaly detection (4).]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/anomaly_agent.py
- [[Chart generation tool for the AI agents.  Creates a Plotly chart from structured]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/chart_tools.py
- [[ChatGroq singleton (thread-safe, streaming=False)]] - code - services/ai-agent/agents/base.py
- [[Create a Plotly chart from query data. Returns an embeddable chart block.      c]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/chart_tools.py
- [[Force the singleton to be rebuilt on next call — use after rotating GROQ_API_KEY]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/base.py
- [[Get logs from failed Airflow tasks. Provide dag_id and run_id.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[Get recent run history for an Airflow DAG, including failed task logs.     Retur]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[List all Airflow DAGs with their activepaused state.     Use for a pipeline ove]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[NL → dbt agent — natural language to production dbt model (2).  Flow   1. User]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/nl_dbt_agent.py
- [[Return a compiled LangGraph ReAct agent for AI data contract generation.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/contract_agent.py
- [[Return a compiled LangGraph ReAct agent for NL → dbt model generation.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/nl_dbt_agent.py
- [[Return a compiled LangGraph ReAct agent for contextual anomaly detection.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/anomaly_agent.py
- [[Return a compiled LangGraph ReAct agent for self-healing.]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/self_healing_agent.py
- [[Self-healing agent diagnose pipeline failures and act autonomously within guard]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/self_healing_agent.py
- [[Shared LLM factory for all agents. streaming=False is critical — Groq streaming]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/agents/base.py
- [[Trigger an Airflow DAG to run immediately.     Use when pipeline is stale or nee]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[_auth()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[_base()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[airbyte_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/airbyte_agent.py
- [[airflow_tools.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[anomaly_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/anomaly_agent.py
- [[base.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/base.py
- [[chart_tools.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/chart_tools.py
- [[check_minio_bucket_size tool]] - code - services/ai-agent/agents/ingestion_agent.py
- [[check_slow_queries tool]] - code - services/ai-agent/agents/performance_agent.py
- [[contract_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/contract_agent.py
- [[create_airbyte_agent()_1]] - code - services/ai-agent/agents/airbyte_agent.py
- [[create_airbyte_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/airbyte_agent.py
- [[create_anomaly_agent()_1]] - code - services/ai-agent/agents/anomaly_agent.py
- [[create_anomaly_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/anomaly_agent.py
- [[create_chart()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/chart_tools.py
- [[create_contract_agent()_1]] - code - services/ai-agent/agents/contract_agent.py
- [[create_contract_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/contract_agent.py
- [[create_dbt_model tool]] - code - services/ai-agent/agents/nl_dbt_agent.py
- [[create_ingestion_agent()_1]] - code - services/ai-agent/agents/ingestion_agent.py
- [[create_ingestion_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/ingestion_agent.py
- [[create_insight_agent()_1]] - code - services/ai-agent/agents/insight_agent.py
- [[create_insight_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/insight_agent.py
- [[create_nl_dbt_agent()_1]] - code - services/ai-agent/agents/nl_dbt_agent.py
- [[create_nl_dbt_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/nl_dbt_agent.py
- [[create_orchestration_agent()_1]] - code - services/ai-agent/agents/orchestration_agent.py
- [[create_orchestration_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/orchestration_agent.py
- [[create_performance_agent()_1]] - code - services/ai-agent/agents/performance_agent.py
- [[create_performance_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/performance_agent.py
- [[create_quality_agent()_1]] - code - services/ai-agent/agents/quality_agent.py
- [[create_quality_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/quality_agent.py
- [[create_react_agent (LangGraph prebuilt)]] - code - services/ai-agent/agents/airbyte_agent.py
- [[create_schema_agent()_1]] - code - services/ai-agent/agents/schema_agent.py
- [[create_schema_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/schema_agent.py
- [[create_self_healing_agent()_1]] - code - services/ai-agent/agents/self_healing_agent.py
- [[create_self_healing_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/self_healing_agent.py
- [[describe_table tool]] - code - services/ai-agent/agents/insight_agent.py
- [[get_airflow_run_history tool]] - code - services/ai-agent/agents/anomaly_agent.py
- [[get_dag_status()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[get_dbt_model_sql tool]] - code - services/ai-agent/agents/nl_dbt_agent.py
- [[get_failed_task_logs tool]] - code - services/ai-agent/agents/self_healing_agent.py
- [[get_failed_task_logs()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[get_latest_sync_job tool]] - code - services/ai-agent/agents/airbyte_agent.py
- [[get_llm()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/base.py
- [[get_recent_incidents_summary tool]] - code - services/ai-agent/agents/self_healing_agent.py
- [[get_table_row_counts tool]] - code - services/ai-agent/agents/anomaly_agent.py
- [[ingestion_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/ingestion_agent.py
- [[insight_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/insight_agent.py
- [[list_airbyte_connections tool]] - code - services/ai-agent/agents/airbyte_agent.py
- [[list_all_dags tool]] - code - services/ai-agent/agents/orchestration_agent.py
- [[list_all_dags()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py
- [[list_expectation_suites tool]] - code - services/ai-agent/agents/contract_agent.py
- [[list_minio_buckets tool]] - code - services/ai-agent/agents/ingestion_agent.py
- [[list_minio_files tool]] - code - services/ai-agent/agents/ingestion_agent.py
- [[nl_dbt_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/nl_dbt_agent.py
- [[orchestration_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/orchestration_agent.py
- [[performance_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/performance_agent.py
- [[profile_table_stats tool]] - code - services/ai-agent/agents/contract_agent.py
- [[quality_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/quality_agent.py
- [[query_clickhouse tool]] - code - services/ai-agent/agents/anomaly_agent.py
- [[reset_llm()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/base.py
- [[restart_airflow_task tool]] - code - services/ai-agent/agents/self_healing_agent.py
- [[run_dbt_models tool]] - code - services/ai-agent/agents/nl_dbt_agent.py
- [[schema_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/schema_agent.py
- [[self_healing_agent.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/agents/self_healing_agent.py
- [[trigger_airbyte_sync tool]] - code - services/ai-agent/agents/airbyte_agent.py
- [[trigger_dag()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/tools/airflow_tools.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/AI_Agent_Tool_Integration_Layer
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_ChromaDB RAG Knowledge Base]]
- 1 edge to [[_COMMUNITY_Audit & Governance Store]]

## Top bridge nodes
- [[get_llm()]] - degree 25, connects to 1 community
- [[create_contract_agent()_1]] - degree 7, connects to 1 community