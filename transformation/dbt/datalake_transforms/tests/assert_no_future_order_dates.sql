-- Assert: No order should have an order_date in the future.
-- A future date indicates a data entry error or timezone bug.
-- Returns rows that violate the rule (test passes when result is empty).
SELECT
    order_id,
    order_date,
    status
FROM {{ ref('stg_orders') }}
WHERE order_date > now()
