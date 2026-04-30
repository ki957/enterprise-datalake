---
type: community
cohesion: 0.10
members: 34
---

# MySQL Data Generator

**Cohesion:** 0.10 - loosely connected
**Members:** 34 nodes

## Members
- [[.setup_method()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_chunk_size_reasonable()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_customer_country_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_customer_email_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_customer_row_has_required_fields()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_customer_rows_generates_correct_count()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_customer_segment_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_n_customers_default_is_large()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_n_orders_default_is_large()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_order_amount_is_positive()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_order_references_valid_customer()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_order_row_has_required_fields()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_order_status_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_product_category_is_valid()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_product_price_is_positive()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_product_row_has_required_fields()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_rand_date_range_respected()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_rand_date_returns_datetime()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[.test_weighted_country_returns_valid_country()]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[Stream rows from generator and insert CHUNK_SIZE at a time.]] - rationale - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[TestMySQLGenerator]] - code - /home/kishore/enterprise-datalake/tests/test_data_generators.py
- [[Yield customer dicts, guaranteeing unique emails at scale.]] - rationale - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[Yield order dicts.  Revenue is correlated with segment via customer lookup.]] - rationale - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[Yield product dicts with unique SKUs.]] - rationale - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[bulk_insert_chunked()_1]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[customer_rows()]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[fetch_ids()_1]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[generate_mysql_data.py]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[get_connection()_1]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[main()_5]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[order_rows()]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[product_rows()]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[rand_date()]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py
- [[weighted_country()]] - code - /home/kishore/enterprise-datalake/scripts/generate_mysql_data.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/MySQL_Data_Generator
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_SaaS Data Generator]]

## Top bridge nodes
- [[TestMySQLGenerator]] - degree 20, connects to 1 community