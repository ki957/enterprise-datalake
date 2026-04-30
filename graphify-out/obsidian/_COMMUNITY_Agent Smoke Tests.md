---
type: community
cohesion: 0.29
members: 8
---

# Agent Smoke Tests

**Cohesion:** 0.29 - loosely connected
**Members:** 8 nodes

## Members
- [[Quick smoke-test run 2 questions against model_agent (nl_dbt) and contracts_age]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/test_agents.py
- [[Score a contracts-agent answer 0-100. Returns (score, issues).]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/test_agents.py
- [[Score a model-agent answer 0-100. Returns (score, issues).]] - rationale - /home/kishore/enterprise-datalake/services/ai-agent/test_agents.py
- [[run_agent()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/test_agents.py
- [[run_suite()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/test_agents.py
- [[score_contract_answer()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/test_agents.py
- [[score_model_answer()]] - code - /home/kishore/enterprise-datalake/services/ai-agent/test_agents.py
- [[test_agents.py]] - code - /home/kishore/enterprise-datalake/services/ai-agent/test_agents.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Agent_Smoke_Tests
SORT file.name ASC
```
