-- Assert: MRR must never be negative.
-- A negative MRR indicates a data error in the subscription source.
-- Returns rows that violate the rule (test passes when result is empty).
SELECT
    subscription_id,
    user_id,
    plan,
    mrr,
    status
FROM {{ ref('stg_subscriptions') }}
WHERE mrr < 0
