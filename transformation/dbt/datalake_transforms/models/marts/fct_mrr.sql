
{{ config(materialized='table', engine='MergeTree()', order_by='(plan, billing_cycle)', schema='gold') }}
SELECT
    plan,
    billing_cycle,
    count() AS total_subscribers,
    sum(mrr) AS total_mrr,
    avg(mrr) AS avg_mrr,
    countIf(status='active') AS active_subscribers,
    countIf(status='cancelled') AS churned_subscribers
FROM {{ ref('stg_subscriptions') }}
GROUP BY plan, billing_cycle
