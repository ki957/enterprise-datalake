{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = 'po_id',
        schema               = 'gold',
        engine               = 'MergeTree()',
        order_by             = '(po_date_day, po_id)',
        settings             = {'allow_nullable_key': 1},
    )
}}

WITH base AS (
    SELECT
        po.po_id,
        lower(hex(MD5(po.vendor_id)))                       AS vendor_key,
        po.vendor_id,
        po.amount,
        po.fulfilled_amount,
        po.line_items,
        po.lead_time_days,
        po.status,
        po.currency,
        po.requested_by,
        po.description,
        po.po_date,
        po.po_date_day,
        po.delivery_date,
        po.updated_at,
        v.name                                              AS vendor_name,
        v.country                                           AS vendor_country,
        v.category                                          AS vendor_category,
        v.payment_terms
    FROM {{ ref('stg_purchase_orders') }} po
    LEFT JOIN {{ ref('dim_vendors') }}     v
        ON lower(hex(MD5(po.vendor_id))) = v.vendor_key

    {% if is_incremental() %}
    WHERE po.updated_at >= (
        SELECT toStartOfHour(max(updated_at)) - toIntervalHour(1)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT * FROM base
