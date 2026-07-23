{{
  config(
    materialized='ephemeral',
    tags=['intermediate', 'customers']
  )
}}

-- Intermediate: Customer-level aggregated metrics
-- Used by multiple downstream marts

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),

customer_metrics AS (
    SELECT
        customer_key,
        customer_id,
        customer_name,
        customer_email,
        customer_segment,
        customer_tier,
        registration_date,

        -- Order metrics
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT date) AS unique_order_days,
        MIN(date) AS first_order_date,
        MAX(date) AS last_order_date,

        -- Revenue metrics
        SUM(line_total) AS lifetime_value,
        SUM(profit_amount) AS lifetime_profit,
        AVG(line_total) AS avg_order_value,
        AVG(profit_margin_pct) AS avg_profit_margin,

        -- Product diversity
        COUNT(DISTINCT product_key) AS unique_products_purchased,
        COUNT(DISTINCT category_name) AS unique_categories,

        -- Time-based metrics
        dateDiff('day', first_order_date, last_order_date) AS customer_tenure_days,
        dateDiff('day', last_order_date, today()) AS days_since_last_order,

        -- Recency-Frequency-Monetary scoring
        CASE 
            WHEN days_since_last_order <= 7 THEN 5
            WHEN days_since_last_order <= 30 THEN 4
            WHEN days_since_last_order <= 90 THEN 3
            WHEN days_since_last_order <= 180 THEN 2
            ELSE 1
        END AS recency_score,

        CASE 
            WHEN total_orders >= 20 THEN 5
            WHEN total_orders >= 10 THEN 4
            WHEN total_orders >= 5 THEN 3
            WHEN total_orders >= 2 THEN 2
            ELSE 1
        END AS frequency_score,

        CASE 
            WHEN lifetime_value >= {{ var('vip_threshold') }} THEN 5
            WHEN lifetime_value >= {{ var('gold_threshold') }} THEN 4
            WHEN lifetime_value >= {{ var('silver_threshold') }} THEN 3
            WHEN lifetime_value >= 500 THEN 2
            ELSE 1
        END AS monetary_score,

        -- Composite RFM score
        recency_score + frequency_score + monetary_score AS rfm_score

    FROM orders
    GROUP BY 
        customer_key,
        customer_id,
        customer_name,
        customer_email,
        customer_segment,
        customer_tier,
        registration_date
)

SELECT * FROM customer_metrics
