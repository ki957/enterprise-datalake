---
type: community
cohesion: 0.18
members: 16
---

# Data Quality Suite (GE)

**Cohesion:** 0.18 - loosely connected
**Members:** 16 nodes

## Members
- [[Check that critical columns don't exceed a 5% null rate.     Higher null rates i]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[Data Quality Suite =================== DAG data_quality_suite Schedule daily a]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[Detect anomalies by comparing today's row counts to the 7-day rolling average.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[Run a ClickHouse query and return results as a list of dicts.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[Run the Great Expectations datalake checkpoint via run_checkpoint.py.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[Summarise quality run results.]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[Verify referential integrity between fact and dimension tables     - All fct_or]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[_ch()_2]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[_ch_rows()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[_send_slack()_4]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[cross_table_consistency_check()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[data_quality_suite.py]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[null_rate_check()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[quality_report()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[row_count_anomaly_check()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py
- [[run_great_expectations()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/quality_dags/data_quality_suite.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Data_Quality_Suite_(GE)
SORT file.name ASC
```
