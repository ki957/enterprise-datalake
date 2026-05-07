{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = 'customer_id',
        schema               = 'staging',
        engine               = 'MergeTree()',
        order_by             = '(assumeNotNull(customer_id))',
        settings             = {'allow_nullable_key': 1},
    )
}}

WITH raw AS (
    SELECT *
    FROM {{ source('bronze', 'src_mysql_customers') }}
    WHERE customer_id > 0
      AND email != ''
      AND email LIKE '%@%'

    {% if is_incremental() %}
      AND updated_at >= (
          SELECT toStartOfHour(max(updated_at)) - toIntervalHour(1)
          FROM {{ this }}
      )
    {% endif %}
),
deduped AS (
    SELECT
        customer_id,
        trim(first_name)                                                AS first_name,
        trim(last_name)                                                 AS last_name,
        concat(trim(first_name), ' ', trim(last_name))                  AS full_name,
        lower(trim(email))                                              AS email,
        CASE WHEN phone = '' THEN NULL ELSE trim(phone) END             AS phone,
        CASE
            WHEN upper(segment) IN ('B2B', 'B2C', 'VIP') THEN upper(segment)
            ELSE 'B2C'
        END                                                             AS segment,
        upper(trim(country))                                            AS country,
        CASE WHEN city = '' THEN NULL ELSE trim(city) END               AS city,
        created_at,
        updated_at,
        toDate(created_at)                                              AS created_date,
        dateDiff('day', created_at, now())                              AS days_since_signup,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
    FROM raw
)
SELECT
    customer_id, first_name, last_name, full_name, email, phone,
    segment, country, city, created_at, updated_at, created_date, days_since_signup
FROM deduped
WHERE rn = 1
