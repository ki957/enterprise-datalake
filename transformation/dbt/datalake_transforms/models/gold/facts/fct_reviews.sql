{{
    config(
        materialized = 'table',
        schema       = 'gold',
        engine       = 'MergeTree()',
        order_by     = '(product_id, review_id)',
        settings     = {'allow_nullable_key': 1},
    )
}}

WITH base AS (
    SELECT
        r.review_id,
        r.user_id,
        lower(hex(MD5(toString(r.product_id))))             AS product_key,
        r.product_id,
        r.rating,
        r.summary,
        r.review_text,
        r.created_at,
        p.name                                              AS product_name,
        p.category                                          AS product_category,
        p.price_tier
    FROM {{ ref('stg_reviews') }}        r
    LEFT JOIN {{ ref('dim_products') }}   p
        ON lower(hex(MD5(toString(r.product_id)))) = p.product_key
)

SELECT * FROM base
