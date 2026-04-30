{{
    config(
        materialized = 'table',
        schema       = 'staging',
        engine       = 'MergeTree()',
        order_by     = 'review_id',
        settings     = {'allow_nullable_key': 1},
    )
}}

-- Maps JSONPlaceholder posts → product reviews
-- post_id → review_id, userId → user_id, title → summary, body → review_text
-- Rating is simulated from post_id (no real rating in JSONPlaceholder)

SELECT
    post_id                                                         AS review_id,
    user_id,
    -- No product_id in JSONPlaceholder; assign via modulo of post_id
    ((post_id - 1) % 200) + 1                                       AS product_id,
    trim(title)                                                     AS summary,
    trim(body)                                                      AS review_text,
    -- Simulate 1–5 star rating from post_id
    ((post_id % 5) + 1)                                             AS rating,
    -- JSONPlaceholder has no timestamps; use current time as placeholder
    now()                                                           AS created_at
FROM {{ source('bronze', 'src_rest_posts') }}
WHERE post_id > 0
  AND user_id > 0
  AND body   != ''
