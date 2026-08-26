{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='order_hk',
    tags=['data_vault', 'silver', 'hub']
  )
}}

-- Hub: Order
-- Contains only order business keys

WITH source AS (
    SELECT DISTINCT
        order_id,
        load_date,
        record_source
    FROM {{ ref('stg_orders') }}
    WHERE order_id IS NOT NULL
),

hashed AS (
    SELECT
        CAST(MD5(CAST(order_id AS String)) AS FixedString(32)) AS order_hk,
        order_id AS order_bk,
        load_date,
        record_source
    FROM source
)

SELECT * FROM hashed
{% if is_incremental() %}
WHERE order_hk NOT IN (SELECT order_hk FROM {{ this }})
{% endif %}
