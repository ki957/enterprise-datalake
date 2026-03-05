
{{ config(materialized='table', engine='MergeTree()', order_by='user_id', schema='staging') }}
SELECT
    id AS user_id,
    email,
    lower(plan) AS plan,
    mrr,
    lower(status) AS status,
    country,
    company,
    created_at,
    updated_at,
    dateDiff('day', created_at, now()) AS days_since_signup
FROM raw.saas_users
WHERE email != '' AND id IS NOT NULL
