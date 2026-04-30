---
source_file: "services/ai-agent/agents/self_healing_agent.py"
type: "code"
community: "AI Agent Tool Integration Layer"
location: "line 55"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/AI_Agent_Tool_Integration_Layer
---

# create_self_healing_agent()

## Connections
- [[create_orchestration_agent()_1]] - `semantically_similar_to` [INFERRED]
- [[create_react_agent (LangGraph prebuilt)]] - `calls` [EXTRACTED]
- [[get_dag_status()]] - `references` [EXTRACTED]
- [[get_failed_task_logs tool]] - `references` [EXTRACTED]
- [[get_llm()]] - `calls` [EXTRACTED]
- [[get_recent_incidents_summary tool]] - `references` [EXTRACTED]
- [[restart_airflow_task tool]] - `references` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/AI_Agent_Tool_Integration_Layer