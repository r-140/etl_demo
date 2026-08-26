{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='order_hk',
    tags=['data_vault', 'silver', 'satellite']
  )
}}

-- Satellite: Order Details

WITH source AS (
    SELECT
        order_id,
        quantity,
        unit_price,
        discount_percent,
        line_total,
        cost_amount,
        profit_amount,
        order_status,
        payment_method,
        load_date,
        record_source
    FROM {{ ref('stg_orders') }}
),

hashed AS (
    SELECT
        CAST(MD5(CAST(order_id AS String)) AS FixedString(32)) AS order_hk,
        quantity,
        unit_price,
        discount_percent,
        line_total,
        cost_amount,
        profit_amount,
        order_status,
        payment_method,
        CAST(MD5(CONCAT(
            COALESCE(CAST(quantity AS String), ''), '|',
            COALESCE(CAST(unit_price AS String), ''), '|',
            COALESCE(CAST(discount_percent AS String), ''), '|',
            COALESCE(CAST(line_total AS String), ''), '|',
            COALESCE(CAST(cost_amount AS String), ''), '|',
            COALESCE(CAST(profit_amount AS String), ''), '|',
            COALESCE(order_status, ''), '|',
            COALESCE(payment_method, '')
        )) AS FixedString(32)) AS hash_diff,
        load_date,
        record_source
    FROM source
)

SELECT * FROM hashed
{% if is_incremental() %}
WHERE (order_hk, hash_diff) NOT IN (SELECT order_hk, hash_diff FROM {{ this }})
{% endif %}
