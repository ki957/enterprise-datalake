{{ config(
    schema='gold',
    materialized='table',
    engine='MergeTree()',
    order_by='date_day',
) }}

SELECT toDate(addDays(toDate('2020-01-01'), number)) AS date_day
FROM system.numbers
WHERE date_day BETWEEN toDate('2020-01-01') AND toDate('2030-12-31')
