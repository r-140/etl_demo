{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['product_key'],
    on_schema_change='sync_all_columns',
    tags=['marts', 'core', 'dimension']
  )
}}

-- Enriched product dimension with sales metrics

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

metrics AS (
    SELECT * FROM {{ ref('int_product_metrics') }}
),

enriched AS (
    SELECT
        p.product_key,
        p.product_id,
        p.product_name,
        p.description,
        p.sku,
        p.category_name,
        p.category_path,
        p.margin_category,
        p.current_product_price,
        p.current_product_cost,
        p.absolute_margin,
        p.vendor_name,
        p.vendor_id,
        p.status,
        p.valid_from,
        p.valid_to,
        p.is_current,

        -- Sales metrics
        m.times_ordered,
        m.total_units_sold,
        m.total_revenue,
        m.total_profit,
        m.avg_margin_pct,
        m.unique_customers,
        m.avg_selling_price,
        m.avg_discount_pct,
        m.max_selling_price,
        m.min_selling_price,
        m.first_sale_date,
        m.last_sale_date,

        -- Product performance flags
        CASE 
            WHEN m.total_revenue >= 100000 THEN 'Top Seller'
            WHEN m.total_revenue >= 10000 THEN 'Strong Performer'
            WHEN m.total_revenue >= 1000 THEN 'Moderate'
            ELSE 'Low Volume'
        END AS performance_tier,

        -- Price competitiveness
        CASE 
            WHEN m.avg_selling_price >= p.current_product_price * 0.95 THEN 'Premium'
            WHEN m.avg_selling_price >= p.current_product_price * 0.8 THEN 'Standard'
            ELSE 'Discounted'
        END AS price_position,

        -- Inventory velocity proxy
        CASE 
            WHEN m.times_ordered IS NULL THEN 'No Sales'
            WHEN m.times_ordered >= 100 THEN 'Fast Moving'
            WHEN m.times_ordered >= 20 THEN 'Steady'
            ELSE 'Slow Moving'
        END AS velocity_category

    FROM products p
    LEFT JOIN metrics m ON p.product_key = m.product_key
)

SELECT * FROM enriched
