---
type: community
cohesion: 0.11
members: 31
---

# SaaS PostgreSQL Data Generator

**Cohesion:** 0.11 - loosely connected
**Members:** 31 nodes

## Members
- [[.setup_method()_1]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.setup_method()_2]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_event_references_valid_user()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_event_row_has_required_fields()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_event_type_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_n_events_default_is_large()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_plan_mrr_mapping_complete()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_rand_dt_returns_datetime()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_subscription_billing_cycle_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_subscription_row_has_required_fields()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_user_email_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_user_mrr_is_non_negative()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_user_plan_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_user_row_has_required_fields()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_user_status_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_vault_write_constructs_correct_url()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_vault_write_sends_token_header()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_vault_write_wraps_data_in_kv2_envelope()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[Data Generator Unit Tests ========================== Tests the pure-Python logic]] - rationale - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[TestSaaSGenerator]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[TestVaultSetup]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[bulk_insert_chunked()]] - code - /home/kishore/enterprise-datalake/scripts/generate_saas_data.py
- [[event_rows()]] - code - /home/kishore/enterprise-datalake/scripts/generate_saas_data.py
- [[fetch_ids()]] - code - /home/kishore/enterprise-datalake/scripts/generate_saas_data.py
- [[generate_saas_data.py]] - code - /home/kishore/enterprise-datalake/scripts/generate_saas_data.py
- [[get_connection()]] - code - /home/kishore/enterprise-datalake/scripts/generate_saas_data.py
- [[main()_4]] - code - /home/kishore/enterprise-datalake/scripts/generate_saas_data.py
- [[rand_dt()]] - code - /home/kishore/enterprise-datalake/scripts/generate_saas_data.py
- [[subscription_rows()]] - code - /home/kishore/enterprise-datalake/scripts/generate_saas_data.py
- [[test_data_generators.py]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[user_rows()]] - code - /home/kishore/enterprise-datalake/scripts/generate_saas_data.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/SaaS_PostgreSQL_Data_Generator
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_ShopFlow MySQL Data Generator]]

## Top bridge nodes
- [[test_data_generators.py]] - degree 4, connects to 1 community