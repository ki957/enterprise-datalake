---
type: community
cohesion: 0.15
members: 20
---

# Spark Unified Customer Profile

**Cohesion:** 0.15 - loosely connected
**Members:** 20 nodes

## Members
- [[Aggregate ShopFlow orders per customer — total revenue and order count.]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[Create gold.unified_customers if it doesn't exist.]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[Execute a DDLDML statement against ClickHouse HTTP API.]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[Full outer join on email so we capture all three customer types       cross_dom]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[Read SaaS users from ClickHouse.]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[Read current ShopFlow customers from ClickHouse.]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[Unified Customer Profile ========================= PySpark cross-domain join Sh]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[Write Parquet snapshot to MinIO for audit and lineage.]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[Write unified profile to ClickHouse via JDBC.]] - rationale - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[build_spark_session()_1]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[build_unified_profile()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[ch_exec()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[ensure_output_table()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[main()_1]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[read_dim_customers()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[read_dim_users()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[read_order_aggregates()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[unified_customer_profile.py]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[write_to_clickhouse()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py
- [[write_to_minio()]] - code - /home/kishore/enterprise-datalake/services/spark/jobs/unified_customer_profile.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Spark_Unified_Customer_Profile
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_ShopFlow Ingestion Pipeline]]

## Top bridge nodes
- [[ch_exec()]] - degree 4, connects to 1 community