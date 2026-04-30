export const QUICK_ACTIONS = {
  'Data Engineer': [
    {
      label: 'Pipeline SLA check',
      question: 'Did the shopflow_datalake_pipeline DAG complete within 30 minutes today? Show start and end time.',
    },
    {
      label: 'Data freshness audit',
      question: 'Show row counts for all gold tables and the min and max order_date in fct_orders. Flag any table with zero rows.',
    },
    {
      label: 'Airbyte sync health',
      question: 'List all Airbyte connections, show their status and the result of the latest sync job for each.',
    },
    {
      label: 'Gold layer quality check',
      question: 'Check data quality on gold tables: count nulls in fct_orders customer_key and product_key, check for duplicate order_ids, and count active dim_customers where is_current=1.',
    },
    {
      label: 'Row count anomaly check',
      question: 'Show current row counts for all gold tables. Flag any table with fewer than 50 rows as critical and any below 200 rows as a warning.',
    },
    {
      label: 'MinIO storage audit',
      question: 'List all MinIO buckets and show the total file count and size for each bucket. Flag any bucket over 100MB.',
    },
  ],
  'Data Scientist': [
    {
      label: 'Revenue trend + growth',
      question: 'Show monthly revenue for the last 6 months with month-over-month growth rate as a percentage.',
    },
    {
      label: 'Customer segment analysis',
      question: 'Break down total revenue and order count by customer segment. Which segment is most valuable?',
    },
    {
      label: 'Product category performance',
      question: 'Which product categories generate the most revenue? Show top 5 with average order value.',
    },
    {
      label: 'Geographic revenue map',
      question: 'Show total revenue by country, top 15. Include average order value per country.',
    },
    {
      label: 'Vendor spend analysis',
      question: 'Who are the top 10 vendors by procurement spend? Show total amount and number of purchase orders.',
    },
    {
      label: 'SaaS MRR & churn',
      question: 'Show MRR trend and churn rate from the SaaS gold layer. Is revenue growing or shrinking?',
    },
  ],
  Operations: [
    {
      label: 'Full stack health check',
      question: 'Run a complete health check: check the Airflow DAG status, gold table row counts, and MinIO file counts.',
    },
    {
      label: 'Debug last pipeline failure',
      question: 'Check if the shopflow_datalake_pipeline had any failures recently. If yes, show the error logs.',
    },
    {
      label: 'Slow query report',
      question: 'List all ClickHouse queries that took more than 1 second today. Suggest optimizations.',
    },
    {
      label: 'Airbyte connection audit',
      question: 'Show all Airbyte connections. For each active one, show the last sync status and record count.',
    },
    {
      label: 'Schema inspection',
      question: 'Describe all tables in the gold schema — show columns, types, and row counts in one report.',
    },
    {
      label: 'Trigger full pipeline',
      question: 'Trigger the shopflow_datalake_pipeline DAG to run now and confirm it started.',
    },
  ],
}

export const PERSONA_ORDER = ['Data Engineer', 'Data Scientist', 'Operations']
