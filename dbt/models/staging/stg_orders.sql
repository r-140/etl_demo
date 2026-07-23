{{
  config(
    materialized='view',
    tags=['staging', 'orders']
  )
}}

-- Staging model for orders fact table
-- Applies basic transformations and type casting

WITH source AS (
    SELECT * FROM {{ source('olap', 'fact_orders') }}
),

renamed AS (
    SELECT
        -- Surrogate keys
        order_key,

        -- Natural keys
        order_id,

        -- Date dimension reference
        date_key,

        -- Customer dimension reference
        customer_key,

        -- Product dimension reference
        product_key,

        -- Vendor dimension reference
        vendor_key,

        -- Measures
        quantity,
        unit_price,
        discount_percent,
        discount_amount,
        line_total,
        cost_amount,
        profit_amount,

        -- Degenerate dimensions
        order_status,
        payment_method,

        -- Derived metrics
        quantity * unit_price AS gross_amount,
        line_total / NULLIF(quantity, 0) AS effective_unit_price,
        profit_amount / NULLIF(line_total, 0) AS profit_margin_pct

    FROM source
)

SELECT * FROM renamed
