-- Monthly customer churn rate derived from subscription cancellations
-- Replaces invalid is_churned column reference with subscription-based churn signal

{{ config(schema='gold', materialized='table', engine='MergeTree()', order_by='tuple()') }}

WITH monthly_activity AS (
    SELECT
        toStartOfMonth(s.started_at) AS month,
        s.user_id,
        s.status
    FROM {{ ref('stg_subscriptions') }} s
),
monthly_churn AS (
    SELECT
        month,
        countIf(status = 'cancelled') AS churned,
        count() AS total
    FROM monthly_activity
    GROUP BY month
)
SELECT
    month,
    round(churned / nullIf(total, 0) * 100, 1) AS churn_rate,
    churned,
    total
FROM monthly_churn
ORDER BY month
