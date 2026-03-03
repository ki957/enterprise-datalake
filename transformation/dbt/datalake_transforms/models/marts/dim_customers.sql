{{ config(
    materialized='table',
    engine='ReplacingMergeTree(updated_at)',
    order_by='id'
) }}

SELECT
    id,
    name,
    email,
    created_at,
    updated_at,
    dbt_loaded_at,
    dateDiff('day', created_at, now()) AS days_since_signup
FROM {{ ref('stg_customers') }}
