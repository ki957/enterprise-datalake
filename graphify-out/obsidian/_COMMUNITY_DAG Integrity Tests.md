---
type: community
cohesion: 0.16
members: 20
---

# DAG Integrity Tests

**Cohesion:** 0.16 - loosely connected
**Members:** 20 nodes

## Members
- [[All DAGs should configure retries — prevents transient failures from failing the]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[DAG Integrity Tests ==================== Verifies that all Airflow DAGs   - Par]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[DAG file must be importable — catches syntax errors and bad imports.]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[Every DAG file must define at least one DAG.]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[Every DAG must have a non-empty description — required for discoverability.]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[Every DAG must have at least one tag — used for Airflow UI filtering.]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[Extract all DAG objects from a loaded module.]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[Import a DAG file as a Python module.]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[Recursively find all .py files under the dags root, excluding __pycache__.]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[_collect_dag_files()]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[_get_dags_from_module()]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[_load_module()]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[catchup=True causes backfill storms on first deploy — must be disabled.]] - rationale - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[test_dag_catchup_disabled()]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[test_dag_default_args_has_retries()]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[test_dag_has_at_least_one_dag_object()]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[test_dag_has_description()]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[test_dag_has_tags()]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[test_dag_imports_without_error()]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py
- [[test_dag_integrity.py]] - code - /home/kishore/enterprise-datalake/tests/test_dag_integrity.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/DAG_Integrity_Tests
SORT file.name ASC
```
