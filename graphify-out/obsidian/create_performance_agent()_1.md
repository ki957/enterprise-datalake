---
source_file: "services/ai-agent/agents/performance_agent.py"
type: "code"
community: "AI Agent Tool Integration Layer"
location: "line 52"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/AI_Agent_Tool_Integration_Layer
---

# create_performance_agent()

## Connections
- [[check_minio_bucket_size tool]] - `references` [EXTRACTED]
- [[check_slow_queries tool]] - `references` [EXTRACTED]
- [[create_chart()]] - `references` [EXTRACTED]
- [[create_react_agent (LangGraph prebuilt)]] - `calls` [EXTRACTED]
- [[get_llm()]] - `calls` [EXTRACTED]
- [[query_clickhouse tool]] - `references` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/AI_Agent_Tool_Integration_Layer