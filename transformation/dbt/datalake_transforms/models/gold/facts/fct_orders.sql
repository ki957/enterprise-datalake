{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = 'order_id',
        schema               = 'gold',
        engine               = 'MergeTree()',
        order_by             = '(order_date_day, order_id)',
        settings             = {'allow_nullable_key': 1},
    )
}}

-- Flat JOIN — ClickHouse 23.8 multi-join subquery alias workaround.
-- Every column is explicitly aliased so the physical table has clean column names.
SELECT
    o.order_id                                                      AS order_id,
    lower(hex(MD5(toString(o.customer_id))))                        AS customer_key,
    lower(hex(MD5(toString(o.product_id))))                         AS product_key,
    o.customer_id                                                   AS customer_id,
    o.product_id                                                    AS product_id,
    o.amount                                                        AS amount,
    o.quantity                                                      AS quantity,
    o.revenue                                                       AS revenue,
    o.status                                                        AS status,
    o.order_date                                                    AS order_date,
    o.order_date_day                                                AS order_date_day,
    o.order_month                                                   AS order_month,
    o.order_year                                                    AS order_year,
    o.updated_at                                                    AS updated_at,
    c.segment                                                       AS customer_segment,
    c.country                                                       AS customer_country,
    p.category                                                      AS product_category,
    p.price_tier                                                    AS product_price_tier,
    p.name                                                          AS product_name
FROM {{ ref('stg_orders') }}       o
LEFT JOIN {{ ref('dim_customers') }} c
    ON lower(hex(MD5(toString(o.customer_id)))) = c.customer_key
    AND c.is_current = 1
LEFT JOIN {{ ref('dim_products') }}  p
    ON lower(hex(MD5(toString(o.product_id)))) = p.product_key

{% if is_incremental() %}
WHERE o.updated_at >= ifNull(
    (SELECT max(updated_at) - toIntervalHour(1) FROM {{ this }}),
    toDateTime('1970-01-01')
)
{% endif %}
