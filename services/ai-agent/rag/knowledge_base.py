"""
ChromaDB knowledge base — GraphRAG-lite with relationship traversal.

Architecture:
  - One document per table (fine-grained, high retrieval precision)
  - Relationship docs for FK join patterns (graph edges)
  - SQL query patterns as separate retrievable documents
  - Multi-query retrieval for Insight Agent (3 search variants, deduped)
  - Graph expansion: table hit → fetch join-partner docs automatically
  - Live schema sync from ClickHouse on startup (gold + staging + raw)
  - Distance-threshold gating so irrelevant docs are never injected
  - Collection version v3: bumped to force re-seed with new GraphRAG structure
  - 1h re-seed cache (was 24h — too stale after schema changes)
"""

import hashlib
import os
import threading
import time

import chromadb
from chromadb.config import Settings

_CHROMA_DIR = os.getenv(
    "CHROMA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "chroma_db"),
)

_reload_state = {
    "last_reload_ts": 0.0,
    "docs_hash":      "",
    "lock":           threading.Lock(),
}

# ── Schema graph — FK relationships for graph expansion ──────────────────────
# When a vector search hits a table, we also fetch its join partners.
# This ensures JOIN queries always retrieve both sides.

_SCHEMA_GRAPH: dict[str, dict] = {
    "fct_orders": {
        "joins": ["dim_customers", "dim_products"],
        "domain": "shopflow",
        "kind": "fact",
    },
    "dim_customers": {
        "joins": ["fct_orders", "unified_customers"],
        "domain": "shopflow",
        "kind": "dimension",
        "scd2": True,
    },
    "dim_products": {
        "joins": ["fct_orders", "fct_reviews", "mart_product_enrichment"],
        "domain": "shopflow",
        "kind": "dimension",
    },
    "dim_vendors": {
        "joins": ["fct_procurement"],
        "domain": "shopflow",
        "kind": "dimension",
    },
    "fct_procurement": {
        "joins": ["dim_vendors"],
        "domain": "shopflow",
        "kind": "fact",
    },
    "fct_reviews": {
        "joins": ["dim_products"],
        "domain": "shopflow",
        "kind": "fact",
    },
    "dim_users": {
        "joins": ["unified_customers"],
        "domain": "saas",
        "kind": "dimension",
    },
    "fct_daily_active_users": {
        "joins": [],
        "domain": "saas",
        "kind": "fact",
    },
    "fct_event_funnel": {
        "joins": [],
        "domain": "saas",
        "kind": "fact",
    },
    "fct_mrr": {
        "joins": [],
        "domain": "saas",
        "kind": "fact",
    },
    "unified_customers": {
        "joins": ["dim_customers", "dim_users"],
        "domain": "cross_domain",
        "kind": "fact",
    },
    # Idea 3: enrichment mart (added proactively so graph knows about it)
    "mart_product_enrichment": {
        "joins": ["dim_products"],
        "domain": "shopflow",
        "kind": "mart",
    },
}

# ── Table documents ───────────────────────────────────────────────────────────

TABLE_DOCS = [
    {
        "id": "table_fct_orders",
        "text": (
            "Table: gold.fct_orders — ShopFlow orders fact table (~23,500 rows).\n"
            "Columns: order_id, customer_key (FK→dim_customers), product_key (FK→dim_products), "
            "customer_id, product_id, amount (gross), quantity, revenue (net after discounts), "
            "status (pending/completed/cancelled), order_date (Date), order_date_day, "
            "order_month, order_year, updated_at, customer_segment, customer_country, "
            "product_category, product_price_tier, product_name.\n"
            "Use for: revenue totals, MoM/YoY trends, orders by segment/country/category, "
            "product performance, sales growth, conversion analysis.\n"
            "Domain: ShopFlow e-commerce. Joins: dim_customers (customer_key), dim_products (product_key)."
        ),
        "metadata": {"category": "schema", "table": "fct_orders", "schema": "gold", "domain": "shopflow"},
    },
    {
        "id": "table_dim_customers",
        "text": (
            "Table: gold.dim_customers — ShopFlow customer dimension, SCD Type 2 (~1,400 active rows).\n"
            "Columns: customer_key (surrogate PK), customer_id (natural key), first_name, last_name, "
            "full_name, email, phone, segment, country, city, days_since_signup, "
            "valid_from, valid_to, is_current (1=active record), version, updated_at.\n"
            "CRITICAL: always filter WHERE is_current = 1 to get current records only.\n"
            "Use for: customer demographics, segmentation, country breakdown, customer lists.\n"
            "Domain: ShopFlow. Joins: fct_orders (customer_key), unified_customers (email)."
        ),
        "metadata": {"category": "schema", "table": "dim_customers", "schema": "gold", "domain": "shopflow"},
    },
    {
        "id": "table_dim_products",
        "text": (
            "Table: gold.dim_products — ShopFlow product dimension (~5,000 rows).\n"
            "Columns: product_key (surrogate PK), product_id, name, category, price, price_tier, "
            "current_stock_quantity, sku, is_in_stock.\n"
            "Use for: product catalogue, inventory levels, pricing tiers, category analysis.\n"
            "Domain: ShopFlow. Joins: fct_orders (product_key), fct_reviews (product_key), mart_product_enrichment (product_id)."
        ),
        "metadata": {"category": "schema", "table": "dim_products", "schema": "gold", "domain": "shopflow"},
    },
    {
        "id": "table_dim_vendors",
        "text": (
            "Table: gold.dim_vendors — SAP vendor dimension (~50 rows).\n"
            "Columns: vendor_key, vendor_id, name, contact_name, email, phone, country, city, "
            "category, payment_terms, is_active.\n"
            "Use for: vendor lists, procurement supplier analysis.\n"
            "Domain: ShopFlow/SAP. Joins: fct_procurement (vendor_key)."
        ),
        "metadata": {"category": "schema", "table": "dim_vendors", "schema": "gold", "domain": "shopflow"},
    },
    {
        "id": "table_fct_procurement",
        "text": (
            "Table: gold.fct_procurement — SAP purchase orders fact table (~300 rows).\n"
            "Columns: po_id, vendor_key (FK→dim_vendors), vendor_id, amount, fulfilled_amount, "
            "line_items, lead_time_days, status, currency, po_date, po_date_day, "
            "delivery_date, vendor_name, vendor_country, vendor_category, payment_terms.\n"
            "Use for: procurement spend, vendor performance, lead time analysis, fulfilment rate.\n"
            "Domain: ShopFlow/SAP. Joins: dim_vendors (vendor_key)."
        ),
        "metadata": {"category": "schema", "table": "fct_procurement", "schema": "gold", "domain": "shopflow"},
    },
    {
        "id": "table_fct_reviews",
        "text": (
            "Table: gold.fct_reviews — product reviews fact table (~100 rows).\n"
            "Columns: review_id, user_id, product_key (FK→dim_products), product_id, "
            "rating (1-5), summary, review_text, created_at, product_name, product_category, price_tier.\n"
            "Use for: average ratings, sentiment by category, top-rated products.\n"
            "Domain: ShopFlow. Joins: dim_products (product_key)."
        ),
        "metadata": {"category": "schema", "table": "fct_reviews", "schema": "gold", "domain": "shopflow"},
    },
    {
        "id": "table_dim_users",
        "text": (
            "Table: gold.dim_users — SaaS user/subscriber dimension (~10,000 rows).\n"
            "Columns: user_id, email, plan (free/starter/pro/enterprise), mrr (monthly recurring revenue per user), "
            "status, country, company, created_at, days_since_signup, billing_cycle (monthly/annual), "
            "expires_at, is_churned (1=churned), is_free (1=free plan).\n"
            "Use for: SaaS subscriber analysis, MRR per user, churn rate, plan distribution.\n"
            "Domain: SaaS. Joins: unified_customers (email)."
        ),
        "metadata": {"category": "schema", "table": "dim_users", "schema": "gold", "domain": "saas"},
    },
    {
        "id": "table_fct_daily_active_users",
        "text": (
            "Table: gold.fct_daily_active_users — daily SaaS engagement metrics (~134 rows).\n"
            "Columns: event_date (use this — NOT 'date'), dau (daily active users), "
            "total_events, logins, signups, upgrades, payment_failures.\n"
            "Use for: DAU trends, user engagement, signup rate, upgrade funnel, payment health.\n"
            "Domain: SaaS. No joins needed — standalone aggregate."
        ),
        "metadata": {"category": "schema", "table": "fct_daily_active_users", "schema": "gold", "domain": "saas"},
    },
    {
        "id": "table_fct_event_funnel",
        "text": (
            "Table: gold.fct_event_funnel — SaaS funnel aggregate (~11 rows).\n"
            "Columns: event_type (use this — NOT 'event_name'), total_events, unique_users, "
            "first_seen, last_seen.\n"
            "Use for: conversion funnel, event volume by type, funnel drop-off analysis.\n"
            "Domain: SaaS. No joins needed — standalone aggregate."
        ),
        "metadata": {"category": "schema", "table": "fct_event_funnel", "schema": "gold", "domain": "saas"},
    },
    {
        "id": "table_fct_mrr",
        "text": (
            "Table: gold.fct_mrr — SaaS MRR aggregated by plan × billing_cycle (6 rows).\n"
            "Columns: plan, billing_cycle, total_subscribers, total_mrr, avg_mrr, "
            "active_subscribers, churned_subscribers.\n"
            "IMPORTANT: NO 'month' column and NO 'mrr_usd' column in this table.\n"
            "For MRR by plan: SELECT plan, total_mrr FROM gold.fct_mrr.\n"
            "For per-user MRR over time: use gold.dim_users where mrr > 0.\n"
            "Domain: SaaS. No joins needed — standalone aggregate."
        ),
        "metadata": {"category": "schema", "table": "fct_mrr", "schema": "gold", "domain": "saas"},
    },
    {
        "id": "table_unified_customers",
        "text": (
            "Table: gold.unified_customers — cross-domain customer join (ShopFlow × SaaS).\n"
            "Columns: unified_customer_id, email, full_name, "
            "customer_type ('cross_domain'|'shopflow_only'|'saas_only'), "
            "shopflow_customer_key, saas_user_id, country, saas_plan, saas_mrr, "
            "total_shopflow_revenue, billing_cycle.\n"
            "Use for: cross-domain customer count, combined revenue, plan vs purchase behaviour.\n"
            "Domain: Cross-domain. Joins: dim_customers (shopflow_customer_key), dim_users (saas_user_id)."
        ),
        "metadata": {"category": "schema", "table": "unified_customers", "schema": "gold", "domain": "cross_domain"},
    },
    {
        "id": "table_mart_product_enrichment",
        "text": (
            "Table: gold.mart_product_enrichment — AI-enriched product features from real-time pipeline.\n"
            "Columns: product_id, product_name, original_category, enriched_category, "
            "sentiment (positive/neutral/negative), quality_tier (premium/standard/budget), "
            "keywords (Array), enriched_at.\n"
            "Populated by: Kafka CDC → Groq llama-4-scout enrichment → dbt mart.\n"
            "Use for: sentiment analysis by category, quality tier distribution, enriched product queries.\n"
            "Domain: ShopFlow enrichment. Joins: dim_products (product_id)."
        ),
        "metadata": {"category": "schema", "table": "mart_product_enrichment", "schema": "gold", "domain": "shopflow"},
    },
    {
        "id": "table_column_usage_stats",
        "text": (
            "Table: raw.column_usage_stats — daily column-level query traffic from ClickHouse system.query_log.\n"
            "Columns: schema_name, table_name, column_name, query_count_24h, last_queried_at, captured_at.\n"
            "Use for: identifying unused/deprecated columns, data contract prioritisation, "
            "understanding which gold columns are most queried.\n"
            "Populated by: auto_contract_dag daily. Domain: Governance."
        ),
        "metadata": {"category": "schema", "table": "column_usage_stats", "schema": "raw", "domain": "governance"},
    },
]

# ── Relationship documents (graph edges) ─────────────────────────────────────
# These are retrieved when a user asks a JOIN question.
# They provide exact JOIN syntax including SCD2 filters and key columns.

RELATIONSHIP_DOCS = [
    {
        "id": "rel_orders_customers",
        "text": (
            "JOIN pattern: gold.fct_orders → gold.dim_customers\n"
            "ON fct_orders.customer_key = dim_customers.customer_key\n"
            "AND dim_customers.is_current = 1\n"
            "Use when: revenue by customer segment/country/demographics, customer purchase history.\n"
            "Example:\n"
            "SELECT dc.segment, dc.country, round(sum(fo.revenue), 2) AS revenue\n"
            "FROM gold.fct_orders fo\n"
            "JOIN gold.dim_customers dc ON fo.customer_key = dc.customer_key AND dc.is_current = 1\n"
            "GROUP BY dc.segment, dc.country ORDER BY revenue DESC"
        ),
        "metadata": {"category": "relationship", "tables": "fct_orders,dim_customers", "domain": "shopflow"},
    },
    {
        "id": "rel_orders_products",
        "text": (
            "JOIN pattern: gold.fct_orders → gold.dim_products\n"
            "ON fct_orders.product_key = dim_products.product_key\n"
            "Use when: revenue by product category/price tier, inventory vs sales correlation.\n"
            "Example:\n"
            "SELECT dp.category, dp.price_tier, count() AS orders, round(sum(fo.revenue), 2) AS revenue\n"
            "FROM gold.fct_orders fo\n"
            "JOIN gold.dim_products dp ON fo.product_key = dp.product_key\n"
            "GROUP BY dp.category, dp.price_tier ORDER BY revenue DESC"
        ),
        "metadata": {"category": "relationship", "tables": "fct_orders,dim_products", "domain": "shopflow"},
    },
    {
        "id": "rel_reviews_products",
        "text": (
            "JOIN pattern: gold.fct_reviews → gold.dim_products\n"
            "ON fct_reviews.product_key = dim_products.product_key\n"
            "Use when: average rating by category, top/worst rated products, sentiment by price tier.\n"
            "Example:\n"
            "SELECT dp.category, round(avg(fr.rating), 2) AS avg_rating, count() AS reviews\n"
            "FROM gold.fct_reviews fr\n"
            "JOIN gold.dim_products dp ON fr.product_key = dp.product_key\n"
            "GROUP BY dp.category ORDER BY avg_rating DESC"
        ),
        "metadata": {"category": "relationship", "tables": "fct_reviews,dim_products", "domain": "shopflow"},
    },
    {
        "id": "rel_procurement_vendors",
        "text": (
            "JOIN pattern: gold.fct_procurement → gold.dim_vendors\n"
            "ON fct_procurement.vendor_key = dim_vendors.vendor_key\n"
            "Use when: vendor spend analysis, lead time by vendor, fulfilment rate by category.\n"
            "Example:\n"
            "SELECT dv.name, dv.country, round(sum(fp.amount), 2) AS spend, avg(fp.lead_time_days) AS avg_lead_days\n"
            "FROM gold.fct_procurement fp\n"
            "JOIN gold.dim_vendors dv ON fp.vendor_key = dv.vendor_key\n"
            "GROUP BY dv.name, dv.country ORDER BY spend DESC"
        ),
        "metadata": {"category": "relationship", "tables": "fct_procurement,dim_vendors", "domain": "shopflow"},
    },
    {
        "id": "rel_cross_domain",
        "text": (
            "JOIN pattern: gold.unified_customers — cross-domain bridge table.\n"
            "ShopFlow domain tables: fct_orders, dim_customers, dim_products, fct_reviews, fct_procurement.\n"
            "SaaS domain tables: fct_daily_active_users, fct_event_funnel, fct_mrr, dim_users.\n"
            "unified_customers links both via email. customer_type values:\n"
            "  'cross_domain' — appears in both ShopFlow and SaaS\n"
            "  'shopflow_only' — e-commerce only\n"
            "  'saas_only' — subscription only\n"
            "Use for: combined revenue + MRR per customer, cross-sell opportunity analysis.\n"
            "Example:\n"
            "SELECT customer_type, count() AS customers,\n"
            "       round(sum(total_shopflow_revenue + saas_mrr * 12), 2) AS annual_value\n"
            "FROM gold.unified_customers GROUP BY customer_type"
        ),
        "metadata": {"category": "relationship", "tables": "unified_customers,dim_customers,dim_users", "domain": "cross_domain"},
    },
    {
        "id": "rel_enrichment_products",
        "text": (
            "JOIN pattern: gold.mart_product_enrichment → gold.dim_products\n"
            "ON mart_product_enrichment.product_id = dim_products.product_id\n"
            "Use when: combining AI-generated sentiment/quality_tier with pricing or sales data.\n"
            "Example — revenue by sentiment:\n"
            "SELECT mpe.sentiment, round(sum(fo.revenue), 2) AS revenue, count() AS orders\n"
            "FROM gold.fct_orders fo\n"
            "JOIN gold.dim_products dp ON fo.product_key = dp.product_key\n"
            "JOIN gold.mart_product_enrichment mpe ON dp.product_id = mpe.product_id\n"
            "GROUP BY mpe.sentiment ORDER BY revenue DESC"
        ),
        "metadata": {"category": "relationship", "tables": "mart_product_enrichment,dim_products,fct_orders", "domain": "shopflow"},
    },
]

# ── SQL pattern documents ─────────────────────────────────────────────────────

SQL_PATTERN_DOCS = [
    {
        "id": "sql_mom_growth",
        "text": (
            "SQL pattern: month-over-month (MoM) revenue growth in ClickHouse.\n"
            "ClickHouse has no LAG() window function — use a self-join instead:\n\n"
            "SELECT\n"
            "  curr.month,\n"
            "  curr.revenue,\n"
            "  prev.revenue AS prev_revenue,\n"
            "  if(prev.revenue > 0,\n"
            "     round((curr.revenue - prev.revenue) / prev.revenue * 100, 1),\n"
            "     NULL) AS mom_growth_pct\n"
            "FROM (\n"
            "  SELECT toStartOfMonth(order_date) AS month,\n"
            "         round(sum(revenue), 2) AS revenue\n"
            "  FROM gold.fct_orders\n"
            "  WHERE order_date >= subtractDays(today(), 180)\n"
            "  GROUP BY month\n"
            ") curr\n"
            "LEFT JOIN (\n"
            "  SELECT toStartOfMonth(order_date) AS month,\n"
            "         round(sum(revenue), 2) AS revenue\n"
            "  FROM gold.fct_orders\n"
            "  GROUP BY month\n"
            ") prev ON curr.month = addMonths(prev.month, 1)\n"
            "ORDER BY curr.month"
        ),
        "metadata": {"category": "sql_pattern", "pattern": "mom_growth"},
    },
    {
        "id": "sql_date_filtering",
        "text": (
            "SQL patterns for date filtering in ClickHouse:\n"
            "Last 30 days:  WHERE order_date >= subtractDays(today(), 30)\n"
            "Last 6 months: WHERE order_date >= subtractDays(today(), 180)\n"
            "Last 12 months: WHERE order_date >= subtractDays(today(), 365)\n"
            "This month:    WHERE toStartOfMonth(order_date) = toStartOfMonth(today())\n"
            "Group by month: GROUP BY toStartOfMonth(order_date) AS month ORDER BY month\n"
            "Group by week:  GROUP BY toMonday(order_date) AS week ORDER BY week\n"
            "Group by year:  GROUP BY toYear(order_date) AS year ORDER BY year"
        ),
        "metadata": {"category": "sql_pattern", "pattern": "date_filtering"},
    },
    {
        "id": "sql_scd2_filter",
        "text": (
            "SQL pattern: SCD Type 2 current-record filter for dim_customers.\n"
            "dim_customers uses SCD2 — always add WHERE is_current = 1 to get the latest record.\n"
            "Without this filter you get all historical versions of each customer.\n\n"
            "Correct:\n"
            "SELECT full_name, segment, country FROM gold.dim_customers WHERE is_current = 1\n\n"
            "Join with orders:\n"
            "SELECT dc.segment, sum(fo.revenue) AS revenue\n"
            "FROM gold.fct_orders fo\n"
            "JOIN gold.dim_customers dc ON fo.customer_key = dc.customer_key AND dc.is_current = 1\n"
            "GROUP BY dc.segment ORDER BY revenue DESC"
        ),
        "metadata": {"category": "sql_pattern", "pattern": "scd2"},
    },
    {
        "id": "sql_top_n",
        "text": (
            "SQL pattern: top N ranking queries in ClickHouse.\n"
            "Top 10 customers by revenue:\n"
            "SELECT dc.full_name, dc.country, round(sum(fo.revenue), 2) AS total_revenue\n"
            "FROM gold.fct_orders fo\n"
            "JOIN gold.dim_customers dc ON fo.customer_key = dc.customer_key AND dc.is_current = 1\n"
            "GROUP BY dc.full_name, dc.country ORDER BY total_revenue DESC LIMIT 10\n\n"
            "Top 10 products by sales:\n"
            "SELECT product_name, product_category, count() AS orders, sum(revenue) AS revenue\n"
            "FROM gold.fct_orders GROUP BY product_name, product_category\n"
            "ORDER BY revenue DESC LIMIT 10"
        ),
        "metadata": {"category": "sql_pattern", "pattern": "top_n"},
    },
    {
        "id": "sql_revenue_by_dimension",
        "text": (
            "SQL patterns: revenue breakdown by dimension.\n"
            "By country:\n"
            "SELECT customer_country, round(sum(revenue), 2) AS revenue, count() AS orders\n"
            "FROM gold.fct_orders GROUP BY customer_country ORDER BY revenue DESC\n\n"
            "By product category:\n"
            "SELECT product_category, round(sum(revenue), 2) AS revenue, count() AS orders\n"
            "FROM gold.fct_orders GROUP BY product_category ORDER BY revenue DESC\n\n"
            "By customer segment:\n"
            "SELECT customer_segment, round(sum(revenue), 2) AS revenue\n"
            "FROM gold.fct_orders GROUP BY customer_segment ORDER BY revenue DESC\n\n"
            "By status (completed vs cancelled):\n"
            "SELECT status, count() AS orders, round(sum(revenue), 2) AS revenue\n"
            "FROM gold.fct_orders GROUP BY status ORDER BY orders DESC"
        ),
        "metadata": {"category": "sql_pattern", "pattern": "revenue_breakdown"},
    },
    {
        "id": "sql_dau_trends",
        "text": (
            "SQL pattern: DAU (daily active users) and engagement trends.\n"
            "Last 30 days DAU trend:\n"
            "SELECT event_date, dau, total_events, logins, signups\n"
            "FROM gold.fct_daily_active_users\n"
            "WHERE event_date >= subtractDays(today(), 30)\n"
            "ORDER BY event_date\n\n"
            "Average DAU by month:\n"
            "SELECT toStartOfMonth(event_date) AS month, round(avg(dau), 0) AS avg_dau\n"
            "FROM gold.fct_daily_active_users\n"
            "GROUP BY month ORDER BY month\n\n"
            "IMPORTANT: column is event_date — never use 'date'."
        ),
        "metadata": {"category": "sql_pattern", "pattern": "dau_trends"},
    },
    {
        "id": "sql_mrr_churn",
        "text": (
            "SQL patterns: SaaS MRR and churn analysis.\n"
            "MRR by plan:\n"
            "SELECT plan, billing_cycle, total_mrr, total_subscribers, churned_subscribers\n"
            "FROM gold.fct_mrr ORDER BY total_mrr DESC\n\n"
            "Total active MRR:\n"
            "SELECT sum(total_mrr) AS total_mrr FROM gold.fct_mrr\n\n"
            "Churn rate:\n"
            "SELECT round(countIf(is_churned = 1) / count() * 100, 1) AS churn_pct,\n"
            "       countIf(is_churned = 1) AS churned, count() AS total\n"
            "FROM gold.dim_users\n\n"
            "Active subscribers by plan:\n"
            "SELECT plan, count() AS subscribers, round(sum(mrr), 2) AS mrr\n"
            "FROM gold.dim_users WHERE is_churned = 0 AND is_free = 0\n"
            "GROUP BY plan ORDER BY mrr DESC\n\n"
            "IMPORTANT: fct_mrr has NO 'month' column and NO 'mrr_usd' column."
        ),
        "metadata": {"category": "sql_pattern", "pattern": "mrr_churn"},
    },
    {
        "id": "sql_funnel",
        "text": (
            "SQL pattern: SaaS event funnel analysis.\n"
            "All funnel steps:\n"
            "SELECT event_type, total_events, unique_users,\n"
            "       round(unique_users / max(unique_users) OVER () * 100, 1) AS pct_of_top\n"
            "FROM gold.fct_event_funnel ORDER BY total_events DESC\n\n"
            "IMPORTANT: column is event_type — never use 'event_name'.\n"
            "Use fct_event_funnel for aggregate funnel. For raw events: use gold.stg_events (staging)."
        ),
        "metadata": {"category": "sql_pattern", "pattern": "funnel"},
    },
    {
        "id": "sql_clickhouse_rules",
        "text": (
            "ClickHouse SQL rules — follow these exactly to avoid query errors:\n"
            "1. No LAG() — ClickHouse lacks it; use self-join on addMonths() for period comparison.\n"
            "2. No concat() with Decimal columns — cast first: toString(round(amount, 2))\n"
            "3. Use count() not COUNT(*)\n"
            "4. SCD2: always WHERE is_current = 1 on dim_customers\n"
            "5. Qualify all tables: gold.fct_orders not just fct_orders\n"
            "6. Date truncation: toStartOfMonth(), toMonday(), toYear()\n"
            "7. DAU table: event_date column (never 'date')\n"
            "8. fct_mrr: use total_mrr column (never 'mrr_usd'); no 'month' column\n"
            "9. fct_event_funnel: event_type column (never 'event_name')\n"
            "10. If a query fails: read the error, fix the SQL, and retry immediately."
        ),
        "metadata": {"category": "sql_rules"},
    },
    {
        "id": "sql_cross_domain",
        "text": (
            "SQL pattern: cross-domain unified customer analysis.\n"
            "Count by customer type:\n"
            "SELECT customer_type, count() AS customers,\n"
            "       round(sum(saas_mrr), 2) AS total_saas_mrr,\n"
            "       round(sum(total_shopflow_revenue), 2) AS total_shopflow_rev\n"
            "FROM gold.unified_customers GROUP BY customer_type\n\n"
            "Top cross-domain customers by combined value:\n"
            "SELECT email, full_name, saas_plan, saas_mrr,\n"
            "       total_shopflow_revenue,\n"
            "       round(total_shopflow_revenue + saas_mrr * 12, 2) AS annual_value\n"
            "FROM gold.unified_customers\n"
            "WHERE customer_type = 'cross_domain'\n"
            "ORDER BY annual_value DESC LIMIT 10"
        ),
        "metadata": {"category": "sql_pattern", "pattern": "cross_domain"},
    },
    {
        "id": "sql_enrichment_queries",
        "text": (
            "SQL patterns: AI-enriched product analysis using mart_product_enrichment.\n"
            "Sentiment distribution:\n"
            "SELECT sentiment, count() AS products, round(avg(price), 2) AS avg_price\n"
            "FROM gold.mart_product_enrichment mpe\n"
            "JOIN gold.dim_products dp ON mpe.product_id = dp.product_id\n"
            "GROUP BY sentiment ORDER BY products DESC\n\n"
            "Revenue by quality tier:\n"
            "SELECT mpe.quality_tier, round(sum(fo.revenue), 2) AS revenue, count() AS orders\n"
            "FROM gold.fct_orders fo\n"
            "JOIN gold.dim_products dp ON fo.product_key = dp.product_key\n"
            "JOIN gold.mart_product_enrichment mpe ON dp.product_id = mpe.product_id\n"
            "GROUP BY mpe.quality_tier ORDER BY revenue DESC\n\n"
            "Worst sentiment by category:\n"
            "SELECT original_category, countIf(sentiment='negative') AS negative_products\n"
            "FROM gold.mart_product_enrichment\n"
            "GROUP BY original_category ORDER BY negative_products DESC LIMIT 10"
        ),
        "metadata": {"category": "sql_pattern", "pattern": "enrichment"},
    },
    {
        "id": "sql_column_usage",
        "text": (
            "SQL patterns: column usage and deprecation analysis.\n"
            "Most queried gold columns in last 24h:\n"
            "SELECT schema_name, table_name, column_name, query_count_24h\n"
            "FROM raw.column_usage_stats\n"
            "WHERE schema_name = 'gold' AND captured_at = today()\n"
            "ORDER BY query_count_24h DESC LIMIT 20\n\n"
            "Columns never queried (deprecation candidates):\n"
            "SELECT schema_name, table_name, column_name, last_queried_at\n"
            "FROM raw.column_usage_stats\n"
            "WHERE query_count_24h = 0 AND schema_name = 'gold'\n"
            "ORDER BY table_name, column_name"
        ),
        "metadata": {"category": "sql_pattern", "pattern": "column_usage"},
    },
]

# ── Tool format documents ─────────────────────────────────────────────────────

TOOL_FORMAT_DOCS = [
    {
        "id": "tool_format_minio_files",
        "text": (
            "Tool: list_minio_files(prefix) output format.\n"
            "Returns plain text listing of files under MinIO raw bucket.\n"
            "Each line: '  {object_path}  ({size_kb} KB, {date})'\n\n"
            "MinIO path structure for Airbyte syncs:\n"
            "  airbyte/mysql/shopflow.customers/   — MySQL CDC customers table\n"
            "  airbyte/mysql/shopflow.orders/       — MySQL CDC orders table\n"
            "  airbyte/mysql/shopflow.products/     — MySQL CDC products table\n"
            "  airbyte/sap/vendors/                 — SAP API vendors\n"
            "  airbyte/sap/purchase_orders/         — SAP API purchase orders\n"
            "  airbyte/rest/posts/                  — JSONPlaceholder posts\n"
            "  airbyte/rest/users/                  — JSONPlaceholder users\n\n"
            "File names: {uuid}_{timestamp}.parquet (Parquet format, Airbyte-compatible column naming).\n"
            "Staleness thresholds: ✅ < 25h | ⚠ 25-48h | ❌ missing or > 48h.\n"
            "No files in a prefix = Airbyte sync has never run or failed."
        ),
        "metadata": {"category": "tool_format", "tool": "list_minio_files"},
    },
    {
        "id": "tool_format_dag_logs",
        "text": (
            "Tool: get_failed_task_logs(dag_id, run_id) output format.\n"
            "Returns raw Airflow task log text (last 500 characters per failed task).\n"
            "Format: 'Task: {task_id}\\nLog (last 500 chars):\\n{raw_log_text}'\n\n"
            "How to extract the root error from raw Airflow logs:\n"
            "1. Look for lines containing 'ERROR', 'Exception', 'Error', 'Traceback'\n"
            "2. The last 'Exception:' or 'Error:' line before a blank line is usually the root cause\n"
            "3. 'Connection refused' → service is down\n"
            "4. 'Table ... doesn\\'t exist' → bronze views not created (run setup_clickhouse_bronze.py)\n"
            "5. 'Authentication failed' → credentials in Vault need rotation (make setup-vault)\n"
            "6. 'No module named' → Airflow image outdated (make governance)\n"
            "7. '429' or 'rate limit' → API quota hit, wait and retry\n"
            "8. 'Out of memory' → ClickHouse memory_limit too low"
        ),
        "metadata": {"category": "tool_format", "tool": "get_failed_task_logs"},
    },
    {
        "id": "tool_format_clickhouse_query",
        "text": (
            "Tool: query_clickhouse(sql) output format.\n"
            "On success: 'Query OK — N row(s):\\n\\n| col1 | col2 | ... |\\n|---|---|\\n| val | val |'\n"
            "On zero rows: 'Query returned 0 rows.'\n"
            "On error: 'Query failed: {clickhouse_error_message}'\n\n"
            "Common ClickHouse errors and fixes:\n"
            "- 'Table default.X doesn\\'t exist' → qualify with schema: gold.X not just X\n"
            "- 'Unknown column X' → run describe_table to check exact column names\n"
            "- 'Illegal type Decimal for function concat' → cast: toString(round(col, 2))\n"
            "- 'Cannot find aggregate function LAG' → use self-join pattern instead\n"
            "- 'Memory limit exceeded' → add LIMIT or narrow the WHERE clause\n"
            "- 'Code: 60. DB::Exception' → table or schema doesn\\'t exist\n\n"
            "When a query fails: read the error, fix the SQL, call query_clickhouse again immediately."
        ),
        "metadata": {"category": "tool_format", "tool": "query_clickhouse"},
    },
    {
        "id": "tool_format_describe_table",
        "text": (
            "Tool: describe_table(table, schema) output format.\n"
            "Returns a markdown table: '**Schema: {schema}.{table}**\\n\\n| Column | Type | Default |\\n|---|---|---|\\n| col | Type | — |'\n\n"
            "Common ClickHouse types you will see:\n"
            "- String — text values\n"
            "- UInt32, UInt64, Int32, Int64 — integers\n"
            "- Float32, Float64 — floats\n"
            "- Decimal(18, 4) — fixed precision; cast with toString(round(col, 2)) for concat\n"
            "- Date — date only (use in date filters and toStartOfMonth())\n"
            "- DateTime — timestamp\n"
            "- Nullable(T) — column can be NULL; use isNull() / isNotNull() to check\n"
            "- LowCardinality(String) — optimised string enum\n\n"
            "After describe_table, use the exact column names from the Type column in your SQL."
        ),
        "metadata": {"category": "tool_format", "tool": "describe_table"},
    },
    {
        "id": "tool_format_slow_queries",
        "text": (
            "Tool: check_slow_queries() output format.\n"
            "Returns a markdown table or '✅ No slow queries found today'.\n"
            "Columns: Duration (ms) | Rows Read | Memory | Query (first 100 chars)\n\n"
            "Thresholds: < 100 ms FAST | 100 ms – 1 s ACCEPTABLE | > 1 s SLOW\n\n"
            "Optimisation patterns:\n"
            "- High rows read, no WHERE clause → add date filter: WHERE order_date >= subtractDays(today(), 30)\n"
            "- SELECT * on a large table → select only needed columns\n"
            "- Slow GROUP BY with DISTINCT → use uniqCombined() instead of count(DISTINCT col)\n"
            "- Large raw MinIO bucket → run: make apply-ttl\n\n"
            "To chart slow query durations after this tool: use Duration (ms) column as values."
        ),
        "metadata": {"category": "tool_format", "tool": "check_slow_queries"},
    },
    {
        "id": "tool_format_airbyte",
        "text": (
            "Tool: list_airbyte_connections() output format.\n"
            "Returns: 'Airbyte connections (N):\\n  [uuid_short] name\\n    status={active|inactive} | schedule={X hours|manual}'\n\n"
            "Tool: get_latest_sync_job(connection_id) output format.\n"
            "Returns key-value lines: Job ID, Status (succeeded/failed/running/cancelled), "
            "Started, Duration (Xm Ys), Records synced, Bytes synced.\n\n"
            "All 4 Airbyte connection IDs:\n"
            "  9993f6c9 — MySQL CDC → MinIO (shopflow customers/orders/products)\n"
            "  fecf7237 — SAP OData API → MinIO (vendors, purchase_orders)\n"
            "  66c7e612 — JSONPlaceholder REST API → MinIO (posts, users)\n"
            "  d37118e9 — PostgreSQL → MinIO (inactive)\n\n"
            "Airbyte runs in Kubernetes (abctl). If unreachable:\n"
            "  kubectl get pods -n airbyte-abctl\n"
            "Fallback: trigger via Airflow DAG 'shopflow_datalake_pipeline'."
        ),
        "metadata": {"category": "tool_format", "tool": "airbyte_tools"},
    },
    {
        "id": "tool_format_dag_status",
        "text": (
            "Tool: get_dag_status(dag_id) output format.\n"
            "Returns key-value plain text:\n"
            "  DAG: {dag_id}\n"
            "  State: {success|failed|running|queued}\n"
            "  Run ID: {scheduled__{timestamp} or manual__{timestamp}}\n"
            "  Started: {ISO datetime}\n"
            "  Ended: {ISO datetime or 'still running'}\n\n"
            "How to compute duration: parse Started and Ended as ISO datetimes, subtract.\n"
            "SLA: shopflow_datalake_pipeline should complete within 30 minutes.\n\n"
            "Tool: list_all_dags() output format.\n"
            "Returns: 'All DAGs:\\n  {dag_id}: ACTIVE/PAUSED' — one line per DAG.\n\n"
            "Active DAGs: shopflow_datalake_pipeline, saas_data_pipeline, data_quality_suite, "
            "airbyte_connection_health, dbt_standalone_runner, metadata_sync, spark_data_profiler, "
            "unified_customer_profile, schema_evolution, auto_contract, streaming_enrichment.\n\n"
            "Tool: trigger_dag(dag_id) output format.\n"
            "Returns: 'DAG \\'{dag_id}\\' triggered. Run ID: {run_id}' on success.\n"
            "Returns error string on failure."
        ),
        "metadata": {"category": "tool_format", "tool": "get_dag_status"},
    },
    {
        "id": "tool_format_minio_bucket",
        "text": (
            "Tool: check_minio_bucket_size(bucket) output format.\n"
            "Returns: 'Bucket \\'{name}\\': {N} files, {X.X} MB total' (+ warning if > 1 GB)\n\n"
            "Tool: list_minio_buckets() output format.\n"
            "Returns: 'MinIO buckets:\\n  {name} (created {date})' — one line per bucket.\n\n"
            "Expected buckets: raw, silver, gold, checkpoints, logs.\n"
            "If a bucket is missing, check docker-compose.storage.yml and MinIO Console at :9001."
        ),
        "metadata": {"category": "tool_format", "tool": "minio_tools"},
    },
]

# ── Pipeline architecture documents ──────────────────────────────────────────

PIPELINE_DOCS = [
    {
        "id": "arch_overview",
        "text": (
            "ShopFlow Data Lake architecture:\n"
            "MySQL CDC → Airbyte → MinIO (raw/airbyte/mysql/) → ClickHouse bronze S3 views\n"
            "→ dbt silver (staging schema) → dbt gold (gold schema) → Superset dashboards.\n"
            "SaaS domain: PostgreSQL → Airflow DAG extract → ClickHouse raw → dbt staging/marts → gold.\n"
            "Real-time enrichment: MySQL CDC → Debezium → Kafka → Airflow consumer → Groq → raw.shopflow_products_enriched → dbt mart → gold.mart_product_enrichment.\n"
            "Orchestration: Airflow on port 8080. Main pipeline: shopflow_datalake_pipeline daily 06:00 UTC.\n"
            "Airbyte runs in Kubernetes (abctl), nginx proxy at localhost:8000."
        ),
        "metadata": {"category": "architecture"},
    },
    {
        "id": "service_ports",
        "text": (
            "Service URLs and ports:\n"
            "Airflow: http://localhost:8080 (admin/Airflow@2024)\n"
            "Superset: http://localhost:8088 (admin/Superset@2024)\n"
            "Grafana: http://localhost:3000 (admin/Grafana@2024)\n"
            "Prometheus: http://localhost:9090\n"
            "Keycloak: http://localhost:8180\n"
            "Vault: http://localhost:8200 (token: root)\n"
            "MinIO Console: http://localhost:9001 (admin/Minio@2024)\n"
            "ClickHouse HTTP: http://localhost:8123 (default/Click@2024)\n"
            "ClickHouse Native: localhost:9002\n"
            "Airbyte: http://localhost:8000\n"
            "Spark Master: http://localhost:8081\n"
            "dbt Docs: http://localhost:8082\n"
            "AI Agent: http://localhost:8502 (FastAPI) / http://localhost:3001 (React)\n"
            "Kafka: localhost:9092 (KRaft mode, no Zookeeper)\n"
            "Kafka Connect: http://localhost:8083"
        ),
        "metadata": {"category": "services"},
    },
    {
        "id": "minio_buckets",
        "text": (
            "MinIO buckets (console: http://localhost:9001):\n"
            "raw — Airbyte landing zone. Paths: airbyte/mysql/, airbyte/sap/, airbyte/rest/\n"
            "silver — intermediate dbt transforms\n"
            "gold — serving-ready data\n"
            "checkpoints — Spark streaming checkpoints\n"
            "logs — pipeline run logs and Spark profiling output (logs/profiling/)"
        ),
        "metadata": {"category": "storage"},
    },
    {
        "id": "airflow_dags",
        "text": (
            "Airflow DAGs (http://localhost:8080):\n"
            "shopflow_datalake_pipeline — daily 06:00 UTC, full ELT: Airbyte sync → dbt silver → gold → quality check\n"
            "saas_data_pipeline — daily 06:30 UTC, PostgreSQL extract → ClickHouse → dbt staging/marts\n"
            "data_quality_suite — daily 07:30 UTC, Great Expectations + row count anomaly checks\n"
            "airbyte_connection_health — every 6h, checks Airbyte connection status\n"
            "dbt_standalone_runner — manual trigger, full dbt run\n"
            "metadata_sync — daily 08:00 UTC, schema drift + audit log + schema evolution\n"
            "spark_data_profiler — weekly Sun 02:00 UTC, PySpark column profiling of MinIO\n"
            "unified_customer_profile — daily 08:00 UTC, Spark cross-domain join → gold.unified_customers\n"
            "schema_evolution — daily 08:30 UTC, MySQL information_schema → sources.yml auto-update\n"
            "auto_contract — daily 09:00 UTC, column usage stats + auto GE contract generation\n"
            "streaming_enrichment — every 5 min, Kafka consumer → Groq enrichment → ClickHouse"
        ),
        "metadata": {"category": "orchestration"},
    },
    {
        "id": "dbt_project",
        "text": (
            "dbt project: datalake_transforms. Target: ClickHouse localhost:8123.\n"
            "Model folders → ClickHouse schema:\n"
            "  models/silver/ → staging (incremental MergeTree)\n"
            "  models/gold/dimensions/ → gold (ReplacingMergeTree, SCD Type 2)\n"
            "  models/gold/facts/ → gold (MergeTree, incremental)\n"
            "  models/staging/ → staging (SaaS staging)\n"
            "  models/marts/ → gold (SaaS marts + AI-generated models + enrichment mart)\n"
            "Run dbt: cd transformation/dbt/datalake_transforms && dbt run\n"
            "Docs at http://localhost:8082. Freshness SLA: warn >25h, error >49h."
        ),
        "metadata": {"category": "dbt"},
    },
    {
        "id": "vault_credentials",
        "text": (
            "HashiCorp Vault (http://localhost:8200, token: root, dev mode).\n"
            "Credentials stored at:\n"
            "  secret/data/datalake/clickhouse — ClickHouse host/user/password\n"
            "  secret/data/datalake/minio — MinIO endpoint/key/secret\n"
            "  secret/data/datalake/postgres — PostgreSQL host/user/password\n"
            "  secret/data/airflow/connections/* — Airflow connections\n"
            "  secret/data/airflow/variables/* — Airflow variables (includes GROQ_API_KEY)\n"
            "Airflow reads secrets transparently via VaultBackend.\n"
            "Re-seed credentials: make setup-vault"
        ),
        "metadata": {"category": "security"},
    },
    {
        "id": "schema_evolution_info",
        "text": (
            "Schema Evolution Agent (DAG: schema_evolution, schedule: daily 08:30 UTC).\n"
            "Polls MySQL information_schema.columns for shopflow tables.\n"
            "Compares against Postgres schema_snapshots table.\n"
            "On new column detected: auto-updates models/bronze/sources.yml (safe YAML append).\n"
            "On removed column: logs to schema_drift_alerts (never auto-removes from sources.yml).\n"
            "After update: calls POST http://172.17.0.1:8502/api/rag/reload to refresh AI agent knowledge.\n"
            "After update: runs dbt compile to validate — rolls back if compile fails."
        ),
        "metadata": {"category": "governance"},
    },
    {
        "id": "auto_contract_info",
        "text": (
            "Auto-Contract Agent (DAG: auto_contract, schedule: daily 09:00 UTC).\n"
            "Queries system.query_log for last 24h to find which gold columns were queried.\n"
            "Stores results in raw.column_usage_stats.\n"
            "Hot columns (query_count > 5): auto-generates Great Expectations suites.\n"
            "Cold columns (query_count = 0): marks as deprecation candidates.\n"
            "GE suites written to: governance/great_expectations/expectations/{schema}_{table}.json\n"
            "To see deprecation candidates: query raw.column_usage_stats WHERE query_count_24h = 0"
        ),
        "metadata": {"category": "governance"},
    },
]

# ── All docs combined ─────────────────────────────────────────────────────────

_ALL_DOCS = TABLE_DOCS + RELATIONSHIP_DOCS + SQL_PATTERN_DOCS + TOOL_FORMAT_DOCS + PIPELINE_DOCS

# Schemas to sync live schema from ClickHouse
_LIVE_SYNC_SCHEMAS = ["gold", "raw", "staging"]


def _docs_hash() -> str:
    content = "".join(d["text"] for d in _ALL_DOCS)
    return hashlib.md5(content.encode()).hexdigest()  # noqa: S324


def get_collection():
    client = chromadb.PersistentClient(
        path=_CHROMA_DIR,
        settings=Settings(anonymized_telemetry=False),
    )
    # v3: bumped from v2 to force full re-seed with GraphRAG structure
    return client.get_or_create_collection("shopflow_knowledge_v3")


def seed_knowledge_base() -> int:
    """Upsert all docs into ChromaDB. Safe to call multiple times."""
    collection = get_collection()
    collection.upsert(
        ids=[d["id"] for d in _ALL_DOCS],
        documents=[d["text"] for d in _ALL_DOCS],
        metadatas=[d["metadata"] for d in _ALL_DOCS],
    )
    current_hash = _docs_hash()
    with _reload_state["lock"]:
        _reload_state["last_reload_ts"] = time.time()
        _reload_state["docs_hash"]      = current_hash
    return len(_ALL_DOCS)


def _sync_live_schemas() -> int:
    """
    Query ClickHouse for live schema info across gold, raw, staging schemas.
    Upserts as live_schema_{schema}_{table} documents.
    Falls back silently if ClickHouse is unreachable.
    Returns count of tables synced.
    """
    synced = 0
    try:
        import os as _os
        from clickhouse_driver import Client

        host = _os.getenv("CLICKHOUSE_HOST", "localhost")
        port = int(_os.getenv("CLICKHOUSE_PORT", 9002))
        user = _os.getenv("CLICKHOUSE_USER", "default")
        pw   = _os.getenv("CLICKHOUSE_PASSWORD", "")

        client = Client(host=host, port=port, user=user, password=pw,
                        connect_timeout=5, send_receive_timeout=15)

        collection   = get_collection()
        upsert_ids   = []
        upsert_docs  = []
        upsert_meta  = []

        for schema in _LIVE_SYNC_SCHEMAS:
            try:
                tables = [t[0] for t in client.execute(f"SHOW TABLES FROM {schema}")]
            except Exception:
                continue

            for table in tables:
                try:
                    cols  = client.execute(f"DESCRIBE TABLE {schema}.{table}")
                    count = client.execute(f"SELECT count() FROM {schema}.{table}")[0][0]
                    col_str = ", ".join(f"{r[0]} ({r[1]})" for r in cols)
                    graph_entry = _SCHEMA_GRAPH.get(table, {})
                    joins_str = (
                        f"\nJoins: {', '.join(graph_entry['joins'])}"
                        if graph_entry.get("joins") else ""
                    )
                    doc = (
                        f"Table: {schema}.{table} — live schema ({count:,} rows).\n"
                        f"Columns: {col_str}{joins_str}"
                    )
                    upsert_ids.append(f"live_schema_{schema}_{table}")
                    upsert_docs.append(doc)
                    upsert_meta.append({
                        "category": "schema",
                        "table":    table,
                        "schema":   schema,
                        "source":   "live",
                        "domain":   graph_entry.get("domain", schema),
                    })
                    synced += 1
                except Exception:
                    continue

        if upsert_ids:
            collection.upsert(ids=upsert_ids, documents=upsert_docs, metadatas=upsert_meta)

    except Exception:
        pass  # ClickHouse not reachable — static docs are sufficient

    return synced


def maybe_reload_knowledge_base(max_age_seconds: int = 3600) -> bool:
    """Reload if never loaded, doc content changed, or cache is stale (default 1h)."""
    current_hash = _docs_hash()
    with _reload_state["lock"]:
        age          = time.time() - _reload_state["last_reload_ts"]
        needs_reload = (
            _reload_state["last_reload_ts"] == 0
            or _reload_state["docs_hash"] != current_hash
            or age > max_age_seconds
        )

    if needs_reload:
        seed_knowledge_base()
        _sync_live_schemas()
        return True
    return False


def force_reload() -> dict:
    """Force an immediate full reload. Called by POST /api/rag/reload."""
    with _reload_state["lock"]:
        _reload_state["last_reload_ts"] = 0.0  # invalidate cache
    seed_count  = seed_knowledge_base()
    live_count  = _sync_live_schemas()
    return {"static_docs": seed_count, "live_tables": live_count}


# ── Graph expansion helpers ───────────────────────────────────────────────────

def _get_join_partner_docs(table_name: str, collection, already_seen: set) -> list[str]:
    """Fetch ChromaDB docs for join partners of a table (1-hop graph expansion)."""
    partners = _SCHEMA_GRAPH.get(table_name, {}).get("joins", [])
    extra_docs = []
    for partner in partners:
        if partner in already_seen:
            continue
        # Try live schema doc first, then static table doc
        for doc_id in [f"live_schema_gold_{partner}", f"table_{partner}"]:
            try:
                result = collection.get(ids=[doc_id], include=["documents"])
                docs = result.get("documents", [])
                if docs and docs[0]:
                    extra_docs.append(docs[0])
                    already_seen.add(partner)
                    break
            except Exception:
                continue
    return extra_docs


# ── Query functions ───────────────────────────────────────────────────────────

def query_knowledge(
    question: str,
    n_results: int = 2,
    distance_threshold: float = 1.3,
    category_filter: str | None = None,
) -> str:
    """
    Legacy single-query search. Still used as a building block.
    Returns formatted string of relevant docs.
    """
    collection = get_collection()
    where = {"category": category_filter} if category_filter else None
    try:
        results = collection.query(
            query_texts=[question],
            n_results=min(n_results, collection.count()),
            where=where,
            include=["documents", "distances", "metadatas"],
        )
    except Exception:
        return ""

    docs      = results.get("documents",  [[]])[0]
    distances = results.get("distances",  [[]])[0]
    metas     = results.get("metadatas",  [[]])[0]

    relevant_docs  = []
    relevant_metas = []
    for doc, dist, meta in zip(docs, distances, metas):
        if dist < distance_threshold:
            relevant_docs.append(doc)
            relevant_metas.append(meta)

    return relevant_docs, relevant_metas


def query_knowledge_graph(
    question: str,
    n_results: int = 2,
    distance_threshold: float = 1.3,
    category_filter: str | None = None,
    use_multi_query: bool = False,
) -> str:
    """
    GraphRAG-lite retrieval: vector search + graph expansion + optional multi-query.

    Steps:
    1. Vector search for top-k docs within distance threshold
    2. Graph expansion: for each table hit, also fetch join-partner docs
    3. If use_multi_query: also search with SQL-oriented and schema-oriented variants
    4. Deduplicate and return formatted string

    Returns empty string if nothing relevant found.
    """
    collection = get_collection()
    seen_docs  = {}   # doc_text → True (dedup by content)
    seen_tables = set()

    def _vector_search(q: str, cat: str | None) -> tuple[list, list]:
        where = {"category": cat} if cat else None
        try:
            results = collection.query(
                query_texts=[q],
                n_results=min(max(n_results + 2, 4), collection.count()),
                where=where,
                include=["documents", "distances", "metadatas"],
            )
            docs  = results.get("documents",  [[]])[0]
            dists = results.get("distances",  [[]])[0]
            metas = results.get("metadatas",  [[]])[0]
            return (
                [(d, m) for d, dist, m in zip(docs, dists, metas) if dist < distance_threshold],
            )
        except Exception:
            return ([],)

    # Step 1: primary vector search
    primary_hits = _vector_search(question, category_filter)[0]

    # Step 2: multi-query variants (for Insight Agent)
    if use_multi_query:
        sql_variant    = f"SQL query ClickHouse pattern for: {question}"
        schema_variant = f"table schema column definition: {question}"
        for variant in [sql_variant, schema_variant]:
            extra = _vector_search(variant, category_filter)[0]
            primary_hits = primary_hits + extra

    # Step 3: collect docs + graph expansion
    all_docs = []
    for doc, meta in primary_hits:
        if doc not in seen_docs:
            seen_docs[doc] = True
            all_docs.append(doc)
            # Graph expansion: if this is a schema doc, fetch join partners
            if meta.get("category") == "schema":
                table = meta.get("table", "")
                if table and table not in seen_tables:
                    seen_tables.add(table)
                    partner_docs = _get_join_partner_docs(table, collection, seen_tables)
                    for pd in partner_docs:
                        if pd not in seen_docs:
                            seen_docs[pd] = True
                            all_docs.append(pd)

    # Step 4: also search relationship docs if a table was found
    if seen_tables and not category_filter:
        try:
            rel_results = collection.query(
                query_texts=[question],
                n_results=min(2, collection.count()),
                where={"category": "relationship"},
                include=["documents", "distances"],
            )
            rel_docs  = rel_results.get("documents",  [[]])[0]
            rel_dists = rel_results.get("distances",  [[]])[0]
            for d, dist in zip(rel_docs, rel_dists):
                if dist < distance_threshold and d not in seen_docs:
                    seen_docs[d] = True
                    all_docs.append(d)
        except Exception:
            pass

    if not all_docs:
        return ""

    # Cap at n_results + 2 to avoid bloating the LLM context
    all_docs = all_docs[: n_results + 2]
    return "\n\n".join(f"[{i+1}] {doc}" for i, doc in enumerate(all_docs))
