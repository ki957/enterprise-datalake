-- Assert: Daily active users count must never be negative.
-- Returns rows that violate the rule (test passes when result is empty).
SELECT
    event_date,
    dau,
    total_events
FROM {{ ref('fct_daily_active_users') }}
WHERE dau < 0
   OR total_events < 0
