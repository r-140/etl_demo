{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='product_vendor_hk',
    tags=['data_vault', 'link', 'bronze']
) }}

-- Link: Product-Vendor relationship

WITH source AS (
    SELECT DISTINCT
        product_id,
        vendor_id,
        load_date,
        record_source
    FROM {{ ref('stg_products') }}
    WHERE vendor_id IS NOT NULL
),

hashed AS (
    SELECT
        CAST(MD5(CONCAT(CAST(product_id AS String), '|', CAST(vendor_id AS String))) AS FixedString(32)) AS product_vendor_hk,
        CAST(MD5(CAST(product_id AS String)) AS FixedString(32)) AS product_hk,
        CAST(MD5(CAST(vendor_id AS String)) AS FixedString(32)) AS vendor_hk,
        load_date,
        record_source
    FROM source
)

SELECT * FROM hashed
