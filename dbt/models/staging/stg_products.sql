{{
  config(
    materialized='view',
    tags=['staging', 'products']
  )
}}

-- Staging model for product dimension

WITH source AS (
    SELECT * FROM {{ source('olap', 'dim_product') }}
),

current_records AS (
    SELECT
        product_key,
        product_id,
        name AS product_name,
        description,
        sku,
        category_name,
        category_path,
        vendor_name,
        vendor_id,
        price,
        cost,
        margin,
        status,
        valid_from,
        valid_to,
        is_current,

        -- Derived fields
        CASE 
            WHEN margin > 0.5 THEN 'High Margin'
            WHEN margin > 0.3 THEN 'Medium Margin'
            WHEN margin > 0.1 THEN 'Low Margin'
            ELSE 'Loss Leader'
        END AS margin_category,

        price - cost AS absolute_margin

    FROM source
    WHERE is_current = 1
)

SELECT * FROM current_records
