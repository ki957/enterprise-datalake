-- Customer journey: signup → plan changes → subscription status
-- Replaced invalid is_churned references with status-based churn signal

{{ config(schema='gold', materialized='table', engine='MergeTree()', order_by='tuple()') }}

WITH user_signup AS (
    SELECT
        du.user_id,
        du.email,
        du.created_at AS signup_date,
        du.plan AS initial_plan,
        du.mrr AS initial_mrr
    FROM {{ ref('dim_users') }} du
),
plan_changes AS (
    SELECT
        uc.email,
        uc.saas_plan AS plan,
        uc.saas_mrr AS mrr,
        uc.total_shopflow_revenue,
        ROW_NUMBER() OVER (PARTITION BY uc.email ORDER BY uc.saas_mrr DESC) AS rn
    FROM gold.unified_customers uc
),
subscription_churn AS (
    SELECT
        du.email,
        du.status,
        du.ended_at
    FROM {{ ref('dim_users') }} du
    WHERE du.status = 'cancelled'
)
SELECT
    us.email,
    us.signup_date,
    us.initial_plan,
    us.initial_mrr,
    pc.plan,
    pc.mrr,
    pc.total_shopflow_revenue,
    sc.ended_at AS churn_date,
    if(sc.status = 'cancelled', 1, 0) AS is_churned
FROM user_signup us
LEFT JOIN plan_changes pc
    ON us.email = pc.email AND pc.rn = 1
LEFT JOIN subscription_churn sc
    ON us.email = sc.email
