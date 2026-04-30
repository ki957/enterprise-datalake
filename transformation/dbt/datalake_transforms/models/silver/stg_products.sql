{{
    config(
        materialized = 'table',
        schema       = 'staging',
        engine       = 'MergeTree()',
        order_by     = '(assumeNotNull(product_id))',
        settings     = {'allow_nullable_key': 1},
    )
}}

SELECT
    product_id,
    trim(name)                                                      AS name,
    initcap(trim(category))                                         AS category,
    toDecimal64(price, 2)                                           AS price,
    CASE WHEN stock_qty < 0 THEN 0 ELSE stock_qty END               AS stock_qty,
    upper(trim(sku))                                                AS sku,
    CASE
        WHEN price < 50   THEN 'Budget'
        WHEN price < 200  THEN 'Mid-range'
        WHEN price < 500  THEN 'Premium'
        ELSE 'Luxury'
    END                                                             AS price_tier,
    created_at,
    updated_at
FROM {{ source('bronze', 'src_mysql_products') }}
WHERE product_id > 0
  AND price      > 0
  AND name       != ''
