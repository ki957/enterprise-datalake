{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = 'event_date',
        engine               = 'MergeTree()',
        order_by             = 'event_date',
        schema               = 'gold',
    )
}}

SELECT
    event_date,
    count(DISTINCT user_id) AS dau,
    count()                 AS total_events,
    countIf(event_type='login')          AS logins,
    countIf(event_type='signup')         AS signups,
    countIf(event_type='upgrade')        AS upgrades,
    countIf(event_type='payment_failed') AS payment_failures
FROM {{ ref('stg_events') }}

{% if is_incremental() %}
-- Re-aggregate only the last 3 days to catch late-arriving events
WHERE event_date >= (
    SELECT max(event_date) - toIntervalDay(3)
    FROM {{ this }}
)
{% endif %}

GROUP BY event_date
ORDER BY event_date
