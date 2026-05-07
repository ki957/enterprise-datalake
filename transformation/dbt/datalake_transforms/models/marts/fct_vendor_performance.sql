-- Vendor performance metrics with on-time delivery rate
-- Reformatted for readability

{{ config(schema='gold', materialized='table', engine='MergeTree()', order_by='tuple()') }}

WITH vendor_performance AS (
    SELECT
        vendor_key,
        countIf(delivery_date <= po_date + INTERVAL lead_time_days DAY) AS on_time_deliveries,
        count() AS total_orders,
        sum(amount) AS total_amount,
        count(po_id) AS order_count
    FROM {{ ref('fct_procurement') }}
    GROUP BY vendor_key
)
SELECT
    vp.vendor_key,
    dv.name AS vendor_name,
    vp.total_orders,
    vp.on_time_deliveries,
    round(vp.on_time_deliveries / nullIf(vp.total_orders, 0) * 100, 1) AS on_time_delivery_rate,
    round(vp.total_amount / nullIf(vp.order_count, 0), 2) AS average_order_size
FROM vendor_performance vp
LEFT JOIN {{ ref('dim_vendors') }} dv
    ON vp.vendor_key = dv.vendor_key
