{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = 'event_id',
        engine               = 'MergeTree()',
        order_by             = 'event_id',
        schema               = 'staging',
    )
}}

SELECT
    id AS event_id,
    user_id,
    event_type,
    page,
    occurred_at,
    toDate(occurred_at) AS event_date
FROM raw.saas_events
WHERE user_id IS NOT NULL

{% if is_incremental() %}
  AND occurred_at >= (
      SELECT toStartOfHour(max(occurred_at)) - toIntervalHour(1)
      FROM {{ this }}
  )
{% endif %}
