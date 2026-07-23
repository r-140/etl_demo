{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date_key'],
    on_schema_change='sync_all_columns',
    tags=['marts', 'core', 'daily']
  )
}}

-- Daily sales fact table
-- Incremental: processes only new dates

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
    WHERE date_key > (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
),

daily_agg AS (
    SELECT
        date_key,
        date,
        year,
        month,
        quarter,
        year_month,
        year_quarter,
        is_weekend,
        is_holiday,

        -- Order metrics
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_key) AS unique_customers,
        COUNT(DISTINCT product_key) AS unique_products,

        -- Revenue metrics
        SUM(line_total) AS total_revenue,
        SUM(gross_amount) AS total_gross_revenue,
        SUM(discount_amount) AS total_discounts,
        SUM(profit_amount) AS total_profit,

        -- Quantity metrics
        SUM(quantity) AS total_quantity,
        AVG(quantity) AS avg_items_per_order,

        -- Price metrics
        AVG(line_total) AS avg_order_value,
        AVG(profit_margin_pct) AS avg_profit_margin,

        -- Customer metrics
        COUNT(DISTINCT CASE WHEN customer_tier = 'VIP' THEN customer_key END) AS vip_customers,
        SUM(CASE WHEN customer_tier = 'VIP' THEN line_total ELSE 0 END) AS vip_revenue,

        -- Payment method breakdown
        COUNT(DISTINCT CASE WHEN payment_method = 'credit_card' THEN order_id END) AS credit_card_orders,
        COUNT(DISTINCT CASE WHEN payment_method = 'debit_card' THEN order_id END) AS debit_card_orders,
        COUNT(DISTINCT CASE WHEN payment_method = 'paypal' THEN order_id END) AS paypal_orders,

        -- Time-based flags
        CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        CASE 
            WHEN is_holiday = 1 THEN 'Holiday'
            WHEN is_weekend = 1 THEN 'Weekend'
            ELSE 'Regular'
        END AS day_category

    FROM orders
    GROUP BY 
        date_key,
        date,
        year,
        month,
        quarter,
        year_month,
        year_quarter,
        is_weekend,
        is_holiday
)

SELECT * FROM daily_agg
