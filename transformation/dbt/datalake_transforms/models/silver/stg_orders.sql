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

WITH raw AS (
    SELECT *
    FROM {{ source('bronze', 'src_mysql_orders') }}
    WHERE order_id    > 0
      AND customer_id > 0
      AND product_id  > 0
      AND amount      > 0

    {% if is_incremental() %}
      AND updated_at >= (
          SELECT toStartOfHour(max(updated_at)) - toIntervalHour(1)
          FROM {{ this }}
      )
    {% endif %}
),
deduped AS (
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
        )                                                               AS revenue,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM raw
)
SELECT
    order_id, customer_id, product_id, amount, quantity, status,
    order_date, updated_at, order_date_day, order_month, order_year, revenue
FROM deduped
WHERE rn = 1
