{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = 'order_id',
        schema               = 'staging',
        engine               = 'MergeTree()',
        order_by             = '(assumeNotNull(order_id))',
        settings             = {'allow_nullable_key': 1},
    )
}}

SELECT
    order_id,
    customer_id,
    product_id,
    toDecimal64(amount, 2)                                          AS amount,
    quantity,
    CASE
        WHEN lower(status) IN ('pending', 'completed', 'cancelled') THEN lower(status)
        ELSE 'pending'
    END                                                             AS status,
    order_date,
    updated_at,
    toDate(order_date)                                              AS order_date_day,
    toStartOfMonth(order_date)                                      AS order_month,
    toYear(order_date)                                              AS order_year,
    multiIf(
        lower(status) = 'completed',  toDecimal64(amount, 2),
        toDecimal64(0, 2)
    )                                                               AS revenue
FROM {{ source('bronze', 'src_mysql_orders') }}
WHERE order_id    > 0
  AND customer_id > 0
  AND product_id  > 0
  AND amount      > 0

{% if is_incremental() %}
  -- Only process rows updated since the last run (with 1-hour overlap to handle late arrivals)
  AND updated_at >= (
      SELECT toStartOfHour(max(updated_at)) - toIntervalHour(1)
      FROM {{ this }}
  )
{% endif %}
