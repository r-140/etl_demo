{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['customer_key'],
    on_schema_change='sync_all_columns',
    tags=['marts', 'core', 'dimension']
  )
}}

-- Enriched customer dimension with calculated metrics

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

metrics AS (
    SELECT * FROM {{ ref('int_customer_metrics') }}
),

enriched AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        c.display_name,
        c.email,
        c.phone,
        c.segment AS original_segment,
        c.tier AS original_tier,
        c.registration_date,
        c.valid_from,
        c.valid_to,
        c.is_current,

        -- Metrics from intermediate
        m.total_orders,
        m.unique_order_days,
        m.first_order_date,
        m.last_order_date,
        m.lifetime_value,
        m.lifetime_profit,
        m.avg_order_value,
        m.avg_profit_margin,
        m.unique_products_purchased,
        m.unique_categories,
        m.customer_tenure_days,
        m.days_since_last_order,
        m.recency_score,
        m.frequency_score,
        m.monetary_score,
        m.rfm_score,

        -- Calculated tier based on LTV
        CASE 
            WHEN m.lifetime_value >= {{ var('vip_threshold') }} THEN 'VIP'
            WHEN m.lifetime_value >= {{ var('gold_threshold') }} THEN 'Gold'
            WHEN m.lifetime_value >= {{ var('silver_threshold') }} THEN 'Silver'
            ELSE 'Bronze'
        END AS calculated_tier,

        -- Customer lifecycle stage
        CASE 
            WHEN m.total_orders = 1 THEN 'New'
            WHEN m.days_since_last_order <= 30 THEN 'Active'
            WHEN m.days_since_last_order <= 90 THEN 'At Risk'
            WHEN m.days_since_last_order <= 180 THEN 'Dormant'
            ELSE 'Churned'
        END AS lifecycle_stage,

        -- RFM segment
        CASE 
            WHEN m.rfm_score >= 13 THEN 'Champions'
            WHEN m.rfm_score >= 10 THEN 'Loyal Customers'
            WHEN m.rfm_score >= 7 THEN 'Potential Loyalists'
            WHEN m.rfm_score >= 5 THEN 'At Risk'
            WHEN m.rfm_score >= 3 THEN 'Cannot Lose Them'
            ELSE 'Lost'
        END AS rfm_segment,

        -- Data quality flags
        CASE WHEN c.email IS NULL THEN 1 ELSE 0 END AS missing_email_flag,
        CASE WHEN m.lifetime_value < 0 THEN 1 ELSE 0 END AS negative_ltv_flag

    FROM customers c
    LEFT JOIN metrics m ON c.customer_key = m.customer_key
)

SELECT * FROM enriched
