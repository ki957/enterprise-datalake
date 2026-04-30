-- Assert: Each customer_id must have exactly one current record in dim_customers.
-- Multiple is_current=1 rows for the same customer_id indicates an SCD Type 2 bug.
-- Returns customer_ids that violate the rule (test passes when result is empty).
SELECT
    customer_id,
    count() AS current_count
FROM {{ ref('dim_customers') }}
WHERE is_current = 1
GROUP BY customer_id
HAVING current_count > 1
