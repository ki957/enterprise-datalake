-- Assert: Completed orders must have positive revenue.
-- A completed order with revenue <= 0 indicates a calculation error.
-- Returns rows that violate the rule (test passes when result is empty).
SELECT
    order_id,
    status,
    amount,
    revenue
FROM {{ ref('fct_orders') }}
WHERE status = 'completed'
  AND revenue <= 0
