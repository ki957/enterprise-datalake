---
source_file: "/home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py"
type: "code"
community: "Backend API & Storage Layer"
location: "L45"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Backend_API_&_Storage_Layer
---

# notify_failure()

## Connections
- [[Slack webhook alerting (_send_slack pattern)]] - `implements` [EXTRACTED]
- [[_send_slack()]] - `calls` [EXTRACTED]
- [[saas_data_pipeline DAG]] - `calls` [EXTRACTED]
- [[saas_pipeline.py]] - `contains` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Backend_API_&_Storage_Layer