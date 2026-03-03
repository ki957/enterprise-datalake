{{ config(
    materialized='view',
    engine='Log()'
) }}

SELECT
    id,
    name,
    email,
    created_at,
    updated_at
FROM {{ source('raw', 'customers') }}
