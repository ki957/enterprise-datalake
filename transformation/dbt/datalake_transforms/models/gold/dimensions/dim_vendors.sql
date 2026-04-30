{{
    config(
        materialized = 'table',
        schema       = 'gold',
        engine       = 'ReplacingMergeTree(version)',
        order_by     = 'vendor_key',
        settings     = {'allow_nullable_key': 1},
    )
}}

SELECT
    lower(hex(MD5(vendor_id)))                          AS vendor_key,
    vendor_id,
    name,
    contact_name,
    email,
    phone,
    country,
    city,
    category,
    payment_terms,
    is_active,
    created_at,
    updated_at,
    assumeNotNull(toUnixTimestamp(updated_at))           AS version
FROM {{ ref('stg_vendors') }}
