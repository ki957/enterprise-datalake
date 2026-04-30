-- Assert: All orders must have a positive amount.
-- Returns rows that violate the rule (test passes when result is empty).
SELECT
    order_id,
    amount,
    status
FROM {{ ref('stg_orders') }}
WHERE amount <= 0
