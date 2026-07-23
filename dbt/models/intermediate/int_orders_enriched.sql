{{
  config(
    materialized='ephemeral',
    tags=['intermediate', 'orders']
  )
}}

-- Intermediate: Enriched orders with dimension lookups
-- Joins fact_orders with all dimensions for downstream use

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

dates AS (
    SELECT * FROM {{ ref('stg_dates') }}
),

vendors AS (
    SELECT * FROM {{ source('olap', 'dim_vendor') }}
),

enriched AS (
    SELECT
        -- Order keys
        o.order_key,
        o.order_id,

        -- Date attributes
        o.date_key,
        d.date,
        d.year,
        d.month,
        d.quarter,
        d.year_month,
        d.year_quarter,
        d.is_weekend,
        d.is_holiday,

        -- Customer attributes
        o.customer_key,
        c.customer_id,
        c.full_name AS customer_name,
        c.email AS customer_email,
        c.segment AS customer_segment,
        c.tier AS customer_tier,
        c.tier_rank,
        c.registration_date,

        -- Product attributes
        o.product_key,
        p.product_name,
        p.sku,
        p.category_name,
        p.category_path,
        p.margin_category,
        p.price AS current_product_price,
        p.cost AS current_product_cost,

        -- Vendor attributes
        o.vendor_key,
        v.name AS vendor_name,
        v.status AS vendor_status,

        -- Measures
        o.quantity,
        o.unit_price,
        o.discount_percent,
        o.discount_amount,
        o.line_total,
        o.cost_amount,
        o.profit_amount,
        o.gross_amount,
        o.effective_unit_price,
        o.profit_margin_pct,

        -- Degenerate dimensions
        o.order_status,
        o.payment_method

    FROM orders o
    LEFT JOIN customers c ON o.customer_key = c.customer_key
    LEFT JOIN products p ON o.product_key = p.product_key
    LEFT JOIN dates d ON o.date_key = d.date_key
    LEFT JOIN vendors v ON o.vendor_key = v.vendor_key
)

SELECT * FROM enriched
