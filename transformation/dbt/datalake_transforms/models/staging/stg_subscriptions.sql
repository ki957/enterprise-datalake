
{{ config(materialized='table', engine='MergeTree()', order_by='subscription_id', schema='staging') }}
SELECT
    id AS subscription_id,
    user_id,
    lower(plan) AS plan,
    mrr,
    billing_cycle,
    started_at,
    ended_at,
    lower(status) AS status
FROM raw.saas_subscriptions
WHERE user_id IS NOT NULL
