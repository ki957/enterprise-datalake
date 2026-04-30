{{
    config(
        materialized = 'table',
        schema       = 'gold',
        engine       = 'ReplacingMergeTree(version)',
        order_by     = 'product_key',
        settings     = {'allow_nullable_key': 1},
    )
}}

SELECT
    lower(hex(MD5(toString(product_id))))               AS product_key,
    product_id,
    name,
    category,
    price,
    price_tier,
    stock_qty                                           AS current_stock_quantity,
    sku,
    CASE WHEN stock_qty = 0 THEN 0 ELSE 1 END           AS is_in_stock,
    created_at,
    updated_at,
    assumeNotNull(toUnixTimestamp(updated_at))           AS version
FROM {{ ref('stg_products') }}
