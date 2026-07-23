{{
  config(
    materialized='ephemeral',
    tags=['intermediate', 'products']
  )
}}

-- Intermediate: Product-level aggregated metrics

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),

product_metrics AS (
    SELECT
        product_key,
        product_name,
        sku,
        category_name,
        category_path,
        margin_category,
        current_product_price,
        current_product_cost,
        vendor_name,

        -- Sales metrics
        COUNT(DISTINCT order_id) AS times_ordered,
        SUM(quantity) AS total_units_sold,
        SUM(line_total) AS total_revenue,
        SUM(profit_amount) AS total_profit,
        AVG(profit_margin_pct) AS avg_margin_pct,

        -- Customer metrics
        COUNT(DISTINCT customer_key) AS unique_customers,

        -- Price metrics
        AVG(unit_price) AS avg_selling_price,
        AVG(discount_percent) AS avg_discount_pct,
        MAX(unit_price) AS max_selling_price,
        MIN(unit_price) AS min_selling_price,

        -- Time range
        MIN(date) AS first_sale_date,
        MAX(date) AS last_sale_date

    FROM orders
    GROUP BY 
        product_key,
        product_name,
        sku,
        category_name,
        category_path,
        margin_category,
        current_product_price,
        current_product_cost,
        vendor_name
)

SELECT * FROM product_metrics
