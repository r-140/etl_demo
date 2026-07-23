{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='product_hk',
    tags=['data_vault', 'bronze', 'hub']
  )
}}

-- Hub: Product
-- Contains only product business keys

WITH source AS (
    SELECT DISTINCT
        product_id,
        load_date,
        record_source
    FROM {{ ref('stg_products') }}
    WHERE product_id IS NOT NULL
),

hashed AS (
    SELECT
        CAST(MD5(CAST(product_id AS String)) AS FixedString(32)) AS product_hk,
        product_id AS product_bk,
        load_date,
        record_source
    FROM source
)

SELECT * FROM hashed
