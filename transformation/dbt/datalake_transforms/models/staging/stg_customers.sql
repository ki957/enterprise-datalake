{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='id'
) }}

SELECT
    id,
    lower(trim(name)) AS name,
    lower(trim(email)) AS email,
    toDateTime(created_at) AS created_at,
    toDateTime(updated_at) AS updated_at,
    now() AS dbt_loaded_at
FROM {{ ref('raw_customers') }}
WHERE id IS NOT NULL
  AND email IS NOT NULL
