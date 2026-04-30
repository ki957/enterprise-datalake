---
type: community
cohesion: 0.53
members: 6
---

# SAP Data Generator

**Cohesion:** 0.53 - moderately connected
**Members:** 6 nodes

## Members
- [[SAP OData synthetic data generator. Produces 50 vendors, 300 purchase orders, 3]] - rationale - /home/kishore/enterprise-datalake/services/sap-api/data_generator.py
- [[_rand_dt()]] - code - /home/kishore/enterprise-datalake/services/sap-api/data_generator.py
- [[data_generator.py]] - code - /home/kishore/enterprise-datalake/services/sap-api/data_generator.py
- [[generate_cost_centers()]] - code - /home/kishore/enterprise-datalake/services/sap-api/data_generator.py
- [[generate_purchase_orders()]] - code - /home/kishore/enterprise-datalake/services/sap-api/data_generator.py
- [[generate_vendors()]] - code - /home/kishore/enterprise-datalake/services/sap-api/data_generator.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/SAP_Data_Generator
SORT file.name ASC
```
