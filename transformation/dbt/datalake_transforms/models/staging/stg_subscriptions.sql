
{{ config(materialized='table', engine='MergeTree()', order_by='subscription_id', schema='staging') }}
SELECT
    id AS subscription_id,
    user_id,
    lower(plan) AS plan,
    amount AS mrr,
    billing_cycle,
    started_at,
    expires_at,
    lower(status) AS status,
    created_at
FROM raw.saas_subscriptions
WHERE user_id IS NOT NULL
