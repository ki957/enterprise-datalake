
{{ config(materialized='table', engine='MergeTree()', order_by='event_type', schema='gold') }}
SELECT
    event_type,
    count() AS total_events,
    count(DISTINCT user_id) AS unique_users,
    toDate(min(occurred_at)) AS first_seen,
    toDate(max(occurred_at)) AS last_seen
FROM {{ ref('stg_events') }}
GROUP BY event_type
ORDER BY total_events DESC
