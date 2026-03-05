
{{ config(materialized='table', engine='ReplacingMergeTree()', order_by='user_id', schema='gold') }}
SELECT
    u.user_id,
    u.email,
    u.plan,
    u.mrr,
    u.status,
    u.country,
    u.company,
    u.created_at,
    u.days_since_signup,
    s.billing_cycle,
    s.expires_at,
    CASE WHEN u.status = 'churned' THEN 1 ELSE 0 END AS is_churned,
    CASE WHEN u.plan = 'free' THEN 1 ELSE 0 END AS is_free
FROM {{ ref('stg_users') }} u
LEFT JOIN {{ ref('stg_subscriptions') }} s ON u.user_id = s.user_id
