-- ClickHouse TTL Policies for Enterprise Data Lake
-- =================================================
-- Run via: make apply-ttl
-- Or manually: clickhouse-client --multiquery < scripts/clickhouse_ttl.sql
--
-- Policy rationale:
--   bronze (staging.stg_*): raw/cleaned source data — 90-day TTL
--     Bronze tables are regenerated on every pipeline run via full-refresh
--     (or incremental). Keeping >90 days of staging data wastes disk.
--
--   gold facts: operational fact tables — 365-day TTL
--     Keep 1 year of gold facts for dashboards and trend analysis.
--     Older data can be archived to MinIO cold storage if needed.
--
--   gold dimensions: NO TTL — dimension history is kept indefinitely
--     SCD Type 2 dims must retain historical rows (valid_from/valid_to).
--     Deleting old dim rows would break fct→dim joins for historical reports.
--
-- NOTE: ALTER TABLE ... MODIFY TTL is non-destructive on existing data until
-- ClickHouse runs a background merge. Force a merge with:
--   OPTIMIZE TABLE <table> FINAL;

-- ── Silver / Staging layer — 90-day TTL ──────────────────────────────────────

ALTER TABLE staging.stg_orders
    MODIFY TTL updated_at + INTERVAL 90 DAY;

ALTER TABLE staging.stg_customers
    MODIFY TTL updated_at + INTERVAL 90 DAY;

ALTER TABLE staging.stg_products
    MODIFY TTL updated_at + INTERVAL 90 DAY;

ALTER TABLE staging.stg_vendors
    MODIFY TTL updated_at + INTERVAL 90 DAY;

ALTER TABLE staging.stg_purchase_orders
    MODIFY TTL updated_at + INTERVAL 90 DAY;

-- stg_reviews has no updated_at (JSONPlaceholder has no timestamps)
-- Using created_at as a proxy
ALTER TABLE staging.stg_reviews
    MODIFY TTL created_at + INTERVAL 90 DAY;

-- SaaS staging
ALTER TABLE staging.stg_events
    MODIFY TTL occurred_at + INTERVAL 90 DAY;

ALTER TABLE staging.stg_users
    MODIFY TTL updated_at + INTERVAL 90 DAY;

ALTER TABLE staging.stg_subscriptions
    MODIFY TTL created_at + INTERVAL 90 DAY;

-- ── Gold facts — 365-day TTL ──────────────────────────────────────────────────

ALTER TABLE gold.fct_orders
    MODIFY TTL order_date + INTERVAL 365 DAY;

ALTER TABLE gold.fct_procurement
    MODIFY TTL po_date + INTERVAL 365 DAY;

ALTER TABLE gold.fct_reviews
    MODIFY TTL created_at + INTERVAL 365 DAY;

ALTER TABLE gold.fct_daily_active_users
    MODIFY TTL event_date + INTERVAL 365 DAY;

-- fct_mrr and fct_event_funnel are aggregated, static tables — no date column for TTL.
-- Apply a long TTL using current time as a rough proxy (data refreshes daily anyway).

-- ── Gold dimensions — NO TTL (keep forever for SCD Type 2 history) ───────────
-- dim_customers, dim_products, dim_vendors: intentionally omitted.

-- ── Confirmation query — show all TTL policies ───────────────────────────────
SELECT
    database,
    name AS table,
    ttl_expression
FROM system.tables
WHERE database IN ('staging', 'gold')
  AND ttl_expression != ''
ORDER BY database, name;
