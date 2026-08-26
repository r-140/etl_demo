{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='link_hk',
    tags=['data_vault', 'silver', 'link']
  )
}}

-- Link: Order-Customer
-- Represents relationship between Order and Customer hubs
-- Contains only hash keys of connected hubs + link hash key

WITH source AS (
    SELECT DISTINCT
        order_id,
        customer_id,
        load_date,
        record_source
    FROM {{ ref('stg_orders') }}
    WHERE order_id IS NOT NULL 
      AND customer_id IS NOT NULL
),

hashed AS (
    SELECT
        -- Link Hash Key: MD5 of concatenated business keys
        CAST(MD5(CONCAT(CAST(order_id AS String), '|', CAST(customer_id AS String))) AS FixedString(32)) AS link_hk,

        -- Foreign Hash Keys to Hubs
        CAST(MD5(CAST(order_id AS String)) AS FixedString(32)) AS order_hk,
        CAST(MD5(CAST(customer_id AS String)) AS FixedString(32)) AS customer_hk,

        -- Business keys (for debugging, optional in strict DV)
        order_id AS order_bk,
        customer_id AS customer_bk,

        -- Audit columns
        load_date,
        record_source

    FROM source
)

SELECT * FROM hashed
{% if is_incremental() %}
WHERE link_hk NOT IN (SELECT link_hk FROM {{ this }})
{% endif %}

-- Data Vault Link tests:
-- 1. link_hk is unique
-- 2. order_hk references hub_order.customer_hk
-- 3. customer_hk references hub_customer.customer_hk
-- 4. No descriptive attributes
