{{
  config(
    materialized='table',
    tags=['marts', 'marketing', 'segmentation']
  )
}}

-- RFM (Recency, Frequency, Monetary) segmentation
-- Ready for marketing campaigns targeting

WITH customer_metrics AS (
    SELECT * FROM {{ ref('int_customer_metrics') }}
),

rfm_segments AS (
    SELECT
        customer_key,
        customer_id,
        customer_name,
        customer_email,
        customer_segment,
        registration_date,

        -- Raw RFM scores
        recency_score,
        frequency_score,
        monetary_score,
        rfm_score,

        -- RFM segment
        CASE 
            WHEN rfm_score >= 13 THEN 'Champions'
            WHEN rfm_score >= 11 THEN 'Loyal Customers'
            WHEN rfm_score >= 9 THEN 'Potential Loyalists'
            WHEN rfm_score >= 7 THEN 'New Customers'
            WHEN rfm_score >= 5 THEN 'Promising'
            WHEN rfm_score >= 4 THEN 'Need Attention'
            WHEN rfm_score >= 3 THEN 'About to Sleep'
            ELSE 'At Risk'
        END AS rfm_segment,

        -- Marketing action recommendations
        CASE 
            WHEN rfm_score >= 13 THEN 'Reward them, early adopter for new products'
            WHEN rfm_score >= 11 THEN 'Upsell higher value products'
            WHEN rfm_score >= 9 THEN 'Offer membership/loyalty program'
            WHEN rfm_score >= 7 THEN 'Make them familiar, nurture them'
            WHEN rfm_score >= 5 THEN 'Offer free trials, limited time offers'
            WHEN rfm_score >= 4 THEN 'Make limited time offers'
            WHEN rfm_score >= 3 THEN 'Share valuable resources, recommend products'
            ELSE 'Send personalized reactivation campaigns'
        END AS recommended_action,

        -- Campaign priority
        CASE 
            WHEN rfm_score >= 11 THEN 'High'
            WHEN rfm_score >= 7 THEN 'Medium'
            ELSE 'Low'
        END AS campaign_priority,

        -- Customer value tier
        CASE 
            WHEN lifetime_value >= {{ var('vip_threshold') }} THEN 'VIP'
            WHEN lifetime_value >= {{ var('gold_threshold') }} THEN 'High Value'
            WHEN lifetime_value >= {{ var('silver_threshold') }} THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_tier,

        -- Churn risk
        CASE 
            WHEN days_since_last_order > 180 THEN 'High Risk'
            WHEN days_since_last_order > 90 THEN 'Medium Risk'
            WHEN days_since_last_order > 30 THEN 'Low Risk'
            ELSE 'Active'
        END AS churn_risk,

        -- Metrics
        total_orders,
        lifetime_value,
        lifetime_profit,
        avg_order_value,
        days_since_last_order,
        unique_products_purchased

    FROM customer_metrics
)

SELECT * FROM rfm_segments
