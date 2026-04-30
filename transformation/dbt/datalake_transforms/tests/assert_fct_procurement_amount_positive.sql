-- Assert: All purchase orders must have a positive amount.
-- Returns rows that violate the rule (test passes when result is empty).
SELECT
    po_id,
    vendor_id,
    amount,
    status,
    currency
FROM {{ ref('fct_procurement') }}
WHERE amount <= 0
