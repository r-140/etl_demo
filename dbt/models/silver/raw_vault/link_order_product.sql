{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='link_hk',
    tags=['data_vault', 'silver', 'link']
  )
}}

-- Link: Order-Product
-- Many-to-many relationship between orders and products

WITH source AS (
    SELECT DISTINCT
        order_id,
        product_id,
        load_date,
        record_source
    FROM {{ ref('stg_orders') }}
    WHERE order_id IS NOT NULL 
      AND product_id IS NOT NULL
),

hashed AS (
    SELECT
        CAST(MD5(CONCAT(CAST(order_id AS String), '|', CAST(product_id AS String))) AS FixedString(32)) AS link_hk,
        CAST(MD5(CAST(order_id AS String)) AS FixedString(32)) AS order_hk,
        CAST(MD5(CAST(product_id AS String)) AS FixedString(32)) AS product_hk,
        order_id AS order_bk,
        product_id AS product_bk,
        load_date,
        record_source
    FROM source
)

SELECT * FROM hashed
{% if is_incremental() %}
WHERE link_hk NOT IN (SELECT link_hk FROM {{ this }})
{% endif %}
