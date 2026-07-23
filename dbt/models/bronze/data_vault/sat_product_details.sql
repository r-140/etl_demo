{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='product_hk',
    tags=['data_vault', 'bronze', 'satellite']
  )
}}

-- Satellite: Product Details

WITH source AS (
    SELECT
        product_id,
        product_name,
        sku,
        category_name,
        vendor_name,
        price,
        cost,
        margin,
        status,
        load_date,
        record_source
    FROM {{ ref('stg_products') }}
),

hashed AS (
    SELECT
        CAST(MD5(CAST(product_id AS String)) AS FixedString(32)) AS product_hk,
        product_name,
        sku,
        category_name,
        vendor_name,
        price,
        cost,
        margin,
        status,
        CAST(MD5(CONCAT(
            COALESCE(product_name, ''), '|',
            COALESCE(sku, ''), '|',
            COALESCE(category_name, ''), '|',
            COALESCE(vendor_name, ''), '|',
            COALESCE(CAST(price AS String), ''), '|',
            COALESCE(CAST(cost AS String), ''), '|',
            COALESCE(CAST(margin AS String), ''), '|',
            COALESCE(status, '')
        )) AS FixedString(32)) AS hash_diff,
        load_date,
        record_source
    FROM source
)

SELECT * FROM hashed
