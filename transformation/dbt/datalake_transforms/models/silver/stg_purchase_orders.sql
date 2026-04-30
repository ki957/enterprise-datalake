{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = 'po_id',
        schema               = 'staging',
        engine               = 'MergeTree()',
        order_by             = 'po_id',
        settings             = {'allow_nullable_key': 1},
    )
}}

SELECT
    po_id,
    vendor_id,
    toDecimal64(amount, 2)                                          AS amount,
    upper(trim(currency))                                           AS currency,
    CASE
        WHEN upper(status) IN ('OPEN','APPROVED','RECEIVED','CLOSED','CANCELLED')
             THEN upper(status)
        ELSE 'OPEN'
    END                                                             AS status,
    line_items,
    trim(description)                                               AS description,
    trim(requested_by)                                              AS requested_by,
    po_date,
    delivery_date,
    created_at,
    updated_at,
    toDate(po_date)                                                 AS po_date_day,
    dateDiff('day', po_date, delivery_date)                         AS lead_time_days,
    multiIf(
        upper(status) IN ('CLOSED', 'RECEIVED'), toDecimal64(amount, 2),
        toDecimal64(0, 2)
    )                                                               AS fulfilled_amount
FROM {{ source('bronze', 'src_sap_purchase_orders') }}
WHERE po_id     != ''
  AND vendor_id != ''
  AND amount    > 0

{% if is_incremental() %}
  AND updated_at >= (
      SELECT toStartOfHour(max(updated_at)) - toIntervalHour(1)
      FROM {{ this }}
  )
{% endif %}
