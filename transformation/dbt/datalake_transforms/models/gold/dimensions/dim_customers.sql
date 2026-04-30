{{
    config(
        materialized = 'table',
        schema       = 'gold',
        engine       = 'ReplacingMergeTree(version)',
        order_by     = '(customer_key, valid_from)',
        settings     = {'allow_nullable_key': 1},
    )
}}

/*
 * dim_customers — SCD Type 2
 *
 * Tracks changes in: email, segment, country
 * Surrogate key: MD5(customer_id)
 * On initial load: all records are current (valid_to = 9999-12-31)
 * On subsequent CDC loads: new versions are inserted by the pipeline;
 * the ReplacingMergeTree engine deduplicates on (customer_key, valid_from)
 * keeping the highest version.
 */

WITH source AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        segment,
        country,
        city,
        created_at,
        updated_at,
        days_since_signup,
        -- Version for ReplacingMergeTree dedup (must be non-nullable integer)
        coalesce(toUnixTimestamp(updated_at), toUnixTimestamp(created_at), 0)  AS version
    FROM {{ ref('stg_customers') }}
    WHERE customer_id IS NOT NULL
),

with_scd AS (
    SELECT
        -- Surrogate key
        lower(hex(MD5(toString(customer_id))))          AS customer_key,
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        segment,
        country,
        city,
        days_since_signup,
        -- SCD Type 2 columns
        created_at                                      AS valid_from,
        toDateTime('9999-12-31 23:59:59')               AS valid_to,
        toUInt8(1)                                      AS is_current,
        version,
        updated_at
    FROM source
)

SELECT * FROM with_scd
