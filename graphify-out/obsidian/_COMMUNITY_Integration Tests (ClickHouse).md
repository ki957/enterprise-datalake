---
type: community
cohesion: 0.04
members: 65
---

# Integration Tests (ClickHouse)

**Cohesion:** 0.04 - loosely connected
**Members:** 65 nodes

## Members
- [[.test_bulk_insert()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_correct_event_count()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_correct_user_count()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_event_user_ids_in_range()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_insert_user_row_and_read_back()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_mrr_values_are_numeric()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_null_handling()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_ping_returns_ok()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_plan_values_are_valid()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_quality_check_passes()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_raw_database_exists()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_rerunning_does_not_double_events()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_rerunning_does_not_double_users()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_saas_events_has_required_columns()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_saas_events_table_exists()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_saas_subscriptions_table_exists()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_saas_users_has_required_columns()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_saas_users_mrr_is_float()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_saas_users_table_exists()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_select_one()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_server_returns_version()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[.test_user_emails_preserved()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_xcom_push_is_called_once()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_xcom_push_reports_event_count()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[.test_xcom_push_reports_user_count()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[All event.user_id values should reference one of the 10 seeded users.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[Basic smoke tests — if these fail, something fundamental is broken.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[Can insert 100 rows in a single TSV batch.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[ClickHouse Integration Tests ============================= Verifies connectivity]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[Create the SaaS source tables in the test PostgreSQL instance.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[Create the raw.saas_ tables in the test ClickHouse instance.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[Extract saas_users, saas_events, saas_subscriptions from PostgreSQL → ClickHouse]] - rationale - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[Insert 10 users + 25 events into PostgreSQL; clean up before seeding.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[Integration test fixtures ========================== Provides session-scoped con]] - rationale - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[Nullable columns accept NULL values without error.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[Re-running the extract must not accumulate duplicate rows.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[Run the extract exactly once for this module.     Returns the mock task instance]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[SaaS Pipeline End-to-End Integration Tests =====================================]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[Session-scoped ClickHouse query helper; skips if service unreachable.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[Session-scoped PostgreSQL connection; skips if service unreachable.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[Spot-check that field values survive the TSV serialisation round-trip.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[TestClickHouseConnectivity]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[TestExtractDataIntegrity]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[TestExtractRowCounts]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[TestIdempotency]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[TestInsertAndQuery]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[TestQualityCheck_1]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[TestRawSchemaLayout]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[TestXComIntegration]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[Verify that INSERT + SELECT round-trips work correctly.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[Verify the correct number of rows land in ClickHouse after extract.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[Verify the extract task pushes correct metadata via Airflow XCom.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[Verify the raw schema and SaaS tables were created by the conftest fixture.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[_ch_exec()]] - code - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[after_extract()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[ch_conn()]] - code - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[clickhouse_schema()]] - code - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[conftest.py]] - code - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[data_quality_check() should pass after a successful extract.]] - rationale - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[extract_postgres_to_clickhouse()]] - code - /home/kishore/enterprise-datalake/orchestration/airflow/dags/saas_pipeline.py
- [[pg_conn()]] - code - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[postgres_schema()]] - code - /home/kishore/enterprise-datalake/tests/integration/conftest.py
- [[seeded_postgres()]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py
- [[test_clickhouse.py]] - code - /home/kishore/enterprise-datalake/tests/integration/test_clickhouse.py
- [[test_saas_pipeline_e2e.py]] - code - /home/kishore/enterprise-datalake/tests/integration/test_saas_pipeline_e2e.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Integration_Tests_(ClickHouse)
SORT file.name ASC
```

## Connections to other communities
- 3 edges to [[_COMMUNITY_Backend API & Storage Layer]]
- 1 edge to [[_COMMUNITY_ClickHouse Bronze Setup]]

## Top bridge nodes
- [[extract_postgres_to_clickhouse()]] - degree 7, connects to 1 community
- [[.test_quality_check_passes()]] - degree 2, connects to 1 community