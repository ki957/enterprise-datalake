{{
    config(
        materialized = 'table',
        schema       = 'staging',
        engine       = 'MergeTree()',
        order_by     = 'vendor_id',
        settings     = {'allow_nullable_key': 1},
    )
}}

SELECT
    vendor_id,
    trim(name)                                                      AS name,
    trim(contact_name)                                              AS contact_name,
    lower(trim(email))                                              AS email,
    CASE WHEN phone = '' THEN NULL ELSE trim(phone) END             AS phone,
    upper(trim(country))                                            AS country,
    CASE WHEN city = '' THEN NULL ELSE trim(city) END               AS city,
    initcap(trim(category))                                         AS category,
    upper(trim(payment_terms))                                      AS payment_terms,
    is_active,
    created_at,
    updated_at
FROM {{ source('bronze', 'src_sap_vendors') }}
WHERE vendor_id != ''
  AND name      != ''
